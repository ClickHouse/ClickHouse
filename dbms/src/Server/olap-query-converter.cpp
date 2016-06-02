#include <DB/Common/Exception.h>
#include "OLAPQueryParser.h"
#include "OLAPQueryConverter.h"

/** Программа для преобразования запросов к OLAPServer к SQL запросам.
  */

using namespace DB;


int main(int argc, char ** argv)
try
{
	Context context;
	OLAP::QueryParser parser;
	OLAP::QueryConverter converter("visits_all", "visits_all");

	OLAP::QueryParseResult olap_query = parser.parse(std::cin);
	std::string clickhouse_query;

	converter.OLAPServerQueryToClickHouse(olap_query, context, clickhouse_query);
	std::cout << clickhouse_query << "\n";
}
catch (...)
{
	std::cerr << getCurrentExceptionMessage(false) << "\n";
	throw;
}
