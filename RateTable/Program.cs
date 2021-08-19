using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;

namespace RateTable
{
    class Program
    {
        static string connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
        static long totalCount = 0;

        static void Main(string[] args)
        {
            if (Directory.Exists("csv"))
            {
                string[] fileEntries = Directory.GetFiles("csv");
                foreach (string fileName in fileEntries)
                {
                    Console.WriteLine(DateTime.Now.ToString());
                    UpdateDatabase(fileName);
                    Console.WriteLine(DateTime.Now.ToString());
                }
            }
            Console.WriteLine("Total Count: " + Program.totalCount.ToString());
            Console.ReadLine();
        }
        static void UpdateDatabase(string fileName)
        {
            Console.WriteLine(fileName);
            try
            {
                using (StreamReader streamReader = new StreamReader(fileName))
                {
                    int num = 0;
                    try
                    {
                        using (SqlConnection connection = new SqlConnection(Program.connectionString))
                        {
                            connection.Open();
                            while (!streamReader.EndOfStream)
                            {
                                string str1 = streamReader.ReadLine();
                                ++num;
                                string[] strArray = str1.Split(',');
                                string cmdText = "INSERT INTO dbo.z2t_Zip4_BOUNDARY (State,Jurisdiction_Type,Jurisdiction_FIPS_code,General_Tax_Rate_1,General_Tax_Rate_2,Food_Drug_Tax_Rate_1,Food_Drug_Tax_Rate_2,Effective_Begin_Date,Effective_End_Date) VALUES ("
                                    +"@State,@Jurisdiction_Type,@Jurisdiction_FIPS_code,@General_Tax_Rate_1,@General_Tax_Rate_2,@Food_Drug_Tax_Rate_1,@Food_Drug_Tax_Rate_2,@Effective_Begin_Date,@Effective_End_Date)";
                                try
                                {
                                    using (SqlCommand sqlCommand = new SqlCommand(cmdText, connection))
                                    {
                                        sqlCommand.Parameters.AddWithValue("@State", (object)strArray[0]);
                                        sqlCommand.Parameters.AddWithValue("@Jurisdiction_Type", (object)strArray[1]);
                                        sqlCommand.Parameters.AddWithValue("@Jurisdiction_FIPS_code", (object)strArray[2]);
                                        sqlCommand.Parameters.AddWithValue("@General_Tax_Rate_1", (object)strArray[3]);
                                        sqlCommand.Parameters.AddWithValue("@General_Tax_Rate_2", (object)strArray[4]);
                                        sqlCommand.Parameters.AddWithValue("@Food_Drug_Tax_Rate_1", (object)strArray[5]);
                                        sqlCommand.Parameters.AddWithValue("@Food_Drug_Tax_Rate_2", (object)strArray[6]);
                                        sqlCommand.Parameters.AddWithValue("@Effective_Begin_Date", (object)strArray[7]);
                                        sqlCommand.Parameters.AddWithValue("@Effective_End_Date", (object)strArray[8]);
                                        if (sqlCommand.ExecuteNonQuery() < 0)
                                            Console.WriteLine("Error inserting data into Database!");
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Insert Error: " + ex.ToString());
                                }
                                ++Program.totalCount;
                            }
                            connection.Close();
                        }
                        Console.WriteLine(fileName + " : " + num.ToString());
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Not found " + fileName);
            }
        }
    }
}
