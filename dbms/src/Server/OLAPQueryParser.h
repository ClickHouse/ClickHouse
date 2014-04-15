#pragma once

#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/DOMWriter.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/Text.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/SAX/InputSource.h>

#include <Poco/RegularExpression.h>
#include <Poco/AutoPtr.h>

#include <Yandex/logger_useful.h>
#include <Yandex/Common.h>

namespace DB
{
namespace OLAP
{
	
/// формат выдачи результата
enum Format
{
	FORMAT_XML,
	FORMAT_TAB,
	FORMAT_BIN
};

/// что делать, если размер результата больше max_result_size
enum OverflowMode
{
	OVERFLOW_MODE_THROW,	/// прекратить выполнение запроса, вернуть ошибку (по-умолчанию)
	OVERFLOW_MODE_BREAK,	/// вернуть то, что успело посчитаться до переполнения
	OVERFLOW_MODE_ANY,		/** для тех ключей, которые на момент переполнения, попали в результат,
	* посчитать до конца, остальные ключи игнорировать
	* (то есть, выбрать "первые попавшиеся" max_result_size записи)
	*/
};


/// результат парсинга XML-запроса в формате OLAP-server
struct QueryParseResult
{
	struct KeyAttribute
	{
		std::string attribute;
		unsigned parameter;
	};
	
	struct Aggregate
	{
		std::string attribute;
		std::string function;
		unsigned parameter;
	};
	
	struct WhereCondition
	{
		std::string attribute;
		unsigned parameter;
		std::string relation;
		std::string rhs;
	};
	
	struct SortColumn
	{
		unsigned index;
		std::string direction;
	};
	
	/// 0, если не ограничено
	unsigned max_result_size;
	unsigned max_execution_time;
	unsigned max_threads_per_counter;
	unsigned concurrency;
	unsigned limit; /// максимальное количество записей для вывода (все остальные - игнорируются)
	
	bool cut_date_last;
	bool cut_dates_for_goals; /// если за какую-то дату цели не существовало - то всего лишь пропускать эту дату

	/// Использовать таблицу для одного слоя, даже если указан CounterID = 0.
	bool local;

	/// сэмплинг - по какой доле данных выполнять запрос. принимает значения в диапазоне (0, 1]
	/// если равно 1 - то отключен
	float sample;
	
	Format format;
	OverflowMode overflow_mode;
	
	Poco::AutoPtr<Poco::XML::Document> query;
	
	CounterID_t CounterID;
	time_t date_first;
	time_t date_last;
	unsigned days;
	
	std::vector<KeyAttribute> key_attributes;
	std::vector<Aggregate> aggregates;
	std::vector<WhereCondition> where_conditions;
	std::vector<SortColumn> sort_columns;

	/// Какую иерархию регионов использовать.
	std::string regions_point_of_view;
};


/// Парсер XML-запросов в формате OLAP-server.
class QueryParser
{
private:
	typedef std::pair<std::string, unsigned> AttributeWithParameter;
	AttributeWithParameter parseAttributeWithParameter(const std::string & s);
	time_t getLastDate();
	
	/// regexp для парсинга выражения типа "GoalReaches(111)"
	Poco::RegularExpression parse_attribute_with_parameter_regexp;
	
	Logger * log;
	
public:
	QueryParser()
		: parse_attribute_with_parameter_regexp("^\\s*(\\w+)\\s*\\((\\d+)\\)\\s*$"),
		log(&Logger::get("QueryParser"))
	{
	}
	
	QueryParseResult parse(std::istream & s);
};

}
}
