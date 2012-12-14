#pragma once

#include "OLAPQueryParser.h"
#include <DB/Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace OLAP
{

/// Конвертирует распаршенный XML-запрос в формате OLAP-server в SQL-подобный запрос для clickhouse.
class QueryConverter
{
public:
	QueryConverter(Poco::Util::AbstractConfiguration & config);
	
	/// Получает из запроса в формате OLAP-server запрос и настройки для clickhouse.
	void OLAPServerQueryToClickhouse(const QueryParseResult & query, Context & inout_context, std::string & out_query);
private:
	
};

}
}