#include <iostream>
#include <mysqlxx/mysqlxx.h>


int main(int argc, char ** argv)
{
	mysqlxx::Connection connection("", "127.0.0.1", "root", "qwerty", 3306);
	std::cerr << "Connected." << std::endl;

	mysqlxx::Query query = connection.query();
	query << "SELECT 1 x, '2010-01-01 01:01:01' d";
	mysqlxx::UseQueryResult result = query.use();
	std::cerr << "use() called." << std::endl;

	while (mysqlxx::Row row = result.fetch())
	{
		std::cerr << "Fetched row." << std::endl;
		std::cerr << row[0] << ", " << row["x"] << std::endl;
		std::cerr << row[1] << ", " << row["d"]
			<< ", " << row[1].getDate()
			<< ", " << row[1].getDateTime()
			<< std::endl;
	}
	
	return 0;
}
