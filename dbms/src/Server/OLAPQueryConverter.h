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
	enum AttributeUsage
	{
		ATTRIBUTE_KEY,		  /// в SELECT и GROUP BY
		ATTRIBUTE_AGGREGATE, /// в SELECT в качестве аргумента какой-нибудь агрегатной функции
		ATTRIBUTE_CONDITION, /// в WHERE в качестве аргумента какой-нибудь функции
		ATTRIBUTE_SORT, 	 /// в ORDER BY
	};
	
	std::string convertAttribute(const std::string & attribute, unsigned parameter, AttributeUsage usage);
	
	/// <keys><attribute> => SELECT ... GROUP BY x
	std::string convertAggregationKey(const std::string & attribute, unsigned parameter);
	/// <aggregates><aggregate> => SELECT x
	std::string convertAggregateFunction(const std::string & attribute, unsigned parameter, const std::string & function);
	/// <where><condition><rhs> => SELECT ... where F(A, x)
	std::string convertConstant(const std::string & attribute, const std::string & value);
	/// <where><condition> => SELECT ... WHERE x
	std::string convertCondition(const std::string & attribute, unsigned parameter, const std::string & relation, const std::string & rhs);
	/// <sort><column> => SELECT ... ORDER BY x
	std::string convertSortByKey(const std::string & attribute, unsigned parameter);
	/// <sort><column> => SELECT ... ORDER BY x
	std::string convertSortByAggregate(const std::string & attribute, unsigned parameter, const std::string & function);
	/// ASC или DESC
	std::string convertSortDirection(const std::string & direction);
	/// <dates> => SELECT ... WHERE x
	std::string convertDateRange(time_t date_first, time_t date_last);
	/// <counter_id> => SELECT ... WHERE x
	std::string convertCounterID(Yandex::CounterID_t CounterID);
	
	std::string getTableName(Yandex::CounterID_t CounterID);
	std::string getHavingSection();

	void fillKeyAttributeMap();
	
	std::string table_for_single_counter;
	std::string table_for_all_counters;
	
	std::map<std::string, std::string> key_attribute_map;
};

}
}
