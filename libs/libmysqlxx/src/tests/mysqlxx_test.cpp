#include <iostream>
#include <mysqlxx/mysqlxx.h>
#include <Yandex/time2str.h>
#include <strconvert/escape_manip.h>


int main(int argc, char ** argv)
{
	try
	{
		mysqlxx::Connection connection("test", "127.0.0.1", "root", "qwerty", 3306);
		std::cerr << "Connected." << std::endl;

		{
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
					<< std::endl
					<< mysqlxx::escape << row[1].getDate() << ", " << mysqlxx::escape << row[1].getDateTime() << std::endl
					<< mysqlxx::quote << row[1].getDate() << ", " << mysqlxx::quote << row[1].getDateTime() << std::endl
					<< strconvert::escape_file << row[1].getDate() << ", " << strconvert::escape_file << row[1].getDateTime() << std::endl
					<< strconvert::quote_fast << row[1].getDate() << ", " << strconvert::quote_fast << row[1].getDateTime() << std::endl
					;

				time_t t1 = row[0];
				time_t t2 = row[1];
				std::cerr << t1 << ", " << mysqlxx::DateTime(t1) << std::endl;
				std::cerr << t2 << ", " << mysqlxx::DateTime(t2) << std::endl;
			}
		}

		{
			mysqlxx::Query query = connection.query();
			query << "SELECT 1234567890 abc, 12345.67890 def UNION ALL SELECT 9876543210, 98765.43210";
			mysqlxx::StoreQueryResult result = query.store();

			std::cerr << result.at(0)["abc"].getUInt() << ", " << result.at(0)["def"].getDouble() << std::endl
				<< result.at(1)["abc"].getUInt() << ", " << result.at(1)["def"].getDouble() << std::endl;
		}

		{
			mysqlxx::UseQueryResult result = connection.query("SELECT 'abc\\\\def' x").use();
			mysqlxx::Row row = result.fetch();
			std::cerr << row << std::endl;
			std::cerr << mysqlxx::escape << row << std::endl;
		}

		{
			mysqlxx::Query query = connection.query("SEL");
			query << "ECT 1";

			std::cerr << query.store().at(0).at(0) << std::endl;
		}

		{
			/// Копирование Query
			mysqlxx::Query query = connection.query("SELECT 'Ok' x");
			typedef std::vector<mysqlxx::Query> Queries;
			Queries queries;
			queries.push_back(query);

			for (Queries::iterator it = queries.begin(); it != queries.end(); ++it)
			{
				std::cerr << it->str() << std::endl;
				std::cerr << mysqlxx::escape << it->store().at(0) << std::endl;
			}
		}

		{
			/// Копирование Query
			mysqlxx::Query query1 = connection.query("SELECT");
			mysqlxx::Query query2 = query1;
			query2 << " 1";

			std::cerr << query1.str() << ", " << query2.str() << std::endl;
		}

		{
			/// Копирование Query
			typedef std::list<mysqlxx::Query> Queries;
			Queries queries;
			queries.push_back(connection.query("SELECT"));
			mysqlxx::Query & qref = queries.back();
			qref << " 1";

			for (Queries::iterator it = queries.begin(); it != queries.end(); ++it)
			{
				std::cerr << it->str() << std::endl;
				std::cerr << mysqlxx::escape << it->store().at(0) << std::endl;
			}
		}

		{
			/// Транзакции
			connection.query("DROP TABLE IF EXISTS tmp").execute();
			connection.query("CREATE TABLE tmp (x INT, PRIMARY KEY (x)) ENGINE = InnoDB").execute();

			mysqlxx::Transaction trans(connection);
			connection.query("INSERT INTO tmp VALUES (1)").execute();

			std::cerr << connection.query("SELECT * FROM tmp").store().size() << std::endl;

			trans.rollback();

			std::cerr << connection.query("SELECT * FROM tmp").store().size() << std::endl;
		}

		{
			/// Транзакции
			connection.query("DROP TABLE IF EXISTS tmp").execute();
			connection.query("CREATE TABLE tmp (x INT, PRIMARY KEY (x)) ENGINE = InnoDB").execute();

			{
				mysqlxx::Transaction trans(connection);
				connection.query("INSERT INTO tmp VALUES (1)").execute();
				std::cerr << connection.query("SELECT * FROM tmp").store().size() << std::endl;
			}

			std::cerr << connection.query("SELECT * FROM tmp").store().size() << std::endl;
		}
	}
	catch (const mysqlxx::Exception & e)
	{
		std::cerr << e.code() << ", " << e.message() << std::endl;
		throw;
	}
	
	return 0;
}
