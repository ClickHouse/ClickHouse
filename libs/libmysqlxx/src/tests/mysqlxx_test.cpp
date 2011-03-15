#include <iostream>
#include <mysqlxx/mysqlxx.h>
#include <Yandex/time2str.h>


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
			<< ", " << Yandex::Date2Str(row[1].getDate())
			<< ", " << Yandex::Time2Str(row[1].getDateTime())
			<< std::endl;

		time_t t1 = row[0];
		time_t t2 = row[1];
		std::cerr << t1 << ", " << mysqlxx::DateTime(t1) << std::endl;
		std::cerr << t2 << ", " << mysqlxx::DateTime(t2) << std::endl;
	}
	
	return 0;
}
