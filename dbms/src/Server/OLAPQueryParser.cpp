#include <common/DateLUT.h>
#include <Poco/DateTimeParser.h>
#include <Poco/AutoPtr.h>

#include "OLAPQueryParser.h"
#include <DB/Common/Exception.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

	namespace ErrorCodes
{
	extern const int NOT_FOUND_NODE;
	extern const int FOUND_MORE_THAN_ONE_NODE;
	extern const int SYNTAX_ERROR;
	extern const int UNKNOWN_FORMAT;
	extern const int FIRST_DATE_IS_BIGGER_THAN_LAST_DATE;
	extern const int UNKNOWN_OVERFLOW_MODE;
	extern const int NOT_FOUND_FUNCTION_ELEMENT_FOR_AGGREGATE;
	extern const int NOT_FOUND_RELATION_ELEMENT_FOR_CONDITION;
	extern const int NOT_FOUND_RHS_ELEMENT_FOR_CONDITION;
	extern const int NO_ATTRIBUTES_LISTED;
	extern const int UNKNOWN_DIRECTION_OF_SORTING;
	extern const int INDEX_OF_COLUMN_IN_SORT_CLAUSE_IS_OUT_OF_RANGE;
}

namespace OLAP
{

static std::string getValueOfOneTextElement(Poco::XML::Document * document, const std::string & tag_name)
{
	Poco::AutoPtr<Poco::XML::NodeList> node_list = document->getElementsByTagName(tag_name);
	if (0 == node_list->length())
		throw Exception(std::string("Not found node ") + tag_name, ErrorCodes::NOT_FOUND_NODE);
	else if (1 != node_list->length())
		throw Exception(std::string("Found more than one node ") + tag_name, ErrorCodes::FOUND_MORE_THAN_ONE_NODE);

	return node_list->item(0)->innerText();
}


QueryParser::AttributeWithParameter QueryParser::parseAttributeWithParameter(const std::string & s)
{
	AttributeWithParameter res;
	Poco::RegularExpression::MatchVec matches;

	if (parse_attribute_with_parameter_regexp.match(s, 0, matches))
	{
		if (matches.size() == 3)
		{
			res.first = s.substr(matches[1].offset, matches[1].length);
			res.second = DB::parse<unsigned>(s.substr(matches[2].offset, matches[2].length));
			return res;
		}
	}
	throw Exception(std::string("Invalid attribute syntax: ") + s, ErrorCodes::SYNTAX_ERROR);
}


QueryParseResult QueryParser::parse(std::istream & s)
{
	QueryParseResult result;

	Poco::XML::DOMParser parser;
	Poco::XML::InputSource source(s);

	result.max_result_size = 0;
	result.max_execution_time = 0;

	result.sample = 1.0;

	result.query = parser.parse(&source);

	std::string format_element_name("format");
	std::string CounterID_element_name("counter_id");
	std::string date_first_element_name("first");
	std::string date_last_element_name("last");

	result.format = FORMAT_XML;
	Poco::AutoPtr<Poco::XML::NodeList> node_list = result.query->getElementsByTagName(format_element_name);
	if (node_list->length() > 1)
		throw Exception(std::string("Found more than one node ") + format_element_name,
						 ErrorCodes::FOUND_MORE_THAN_ONE_NODE);

	if (node_list->length() == 1)
	{
		if (node_list->item(0)->innerText() != "xml"
			&& node_list->item(0)->innerText() != "tab"
			&& node_list->item(0)->innerText() != "bin")
			throw Exception(std::string("Unknown format: ") + node_list->item(0)->innerText(),
								ErrorCodes::UNKNOWN_FORMAT);

		result.format = (node_list->item(0)->innerText() == "xml") ? FORMAT_XML
		: ((node_list->item(0)->innerText() == "tab") ? FORMAT_TAB
		: FORMAT_BIN);
	}

	result.CounterID = 0;
	if (result.query->getElementsByTagName(CounterID_element_name)->length() > 0)
		result.CounterID = DB::parse<unsigned>(getValueOfOneTextElement(result.query, CounterID_element_name));

	int time_zone_diff = 0;
	result.date_first = DateLUT::instance().toDate(Poco::DateTimeParser::parse(
		getValueOfOneTextElement(result.query, date_first_element_name), time_zone_diff).timestamp().epochTime());
	result.date_last = DateLUT::instance().toDate(Poco::DateTimeParser::parse(
		getValueOfOneTextElement(result.query, date_last_element_name), time_zone_diff).timestamp().epochTime());

	if (result.date_first > result.date_last)
		throw Exception("First date is bigger than last date.", ErrorCodes::FIRST_DATE_IS_BIGGER_THAN_LAST_DATE);

	const auto & date_lut = DateLUT::instance();
	result.days = 1 + date_lut.toDayNum(result.date_last) - date_lut.toDayNum(result.date_first);

	result.cut_date_last = false;
	result.cut_dates_for_goals = false;
	result.concurrency = 0;
	result.max_threads_per_counter = 0;
	result.limit = 0;
	result.local = false;

	Poco::AutoPtr<Poco::XML::NodeList> settings_nodes = result.query->getElementsByTagName("settings");
	if (settings_nodes->length() > 1)
		throw Exception(std::string("Found more than one node settings"), ErrorCodes::FOUND_MORE_THAN_ONE_NODE);
	if (settings_nodes->length() == 1)
	{
		Poco::AutoPtr<Poco::XML::NodeList> settings_child_nodes = settings_nodes->item(0)->childNodes();
		for (unsigned i = 0; i < settings_child_nodes->length(); i++)
		{
			if (settings_child_nodes->item(i)->nodeName() == "max_result_size")
			{
				/// выставить дополнительное локальное ограничение на максимальный размер результата

				result.max_result_size = DB::parse<unsigned>(settings_child_nodes->item(i)->innerText());
			}
			else if (settings_child_nodes->item(i)->nodeName() == "max_execution_time")
			{
				/// выставить дополнительное локальное ограничение на максимальное время выполнения запроса

				result.max_execution_time = DB::parse<unsigned>(settings_child_nodes->item(i)->innerText());
			}
			else if (settings_child_nodes->item(i)->nodeName() == "cut_date_last")
			{
				/** обрезать запрошенный период до максимальной даты, за которую есть данные
					* вместо того, чтобы сообщить об ошибке, если дата конца периода больше максимальной даты
					*/

				result.cut_date_last = true;
			}
			else if (settings_child_nodes->item(i)->nodeName() == "cut_dates_for_goals")
			{
				/** если за какой-либо день не существовало цели, то пропускать этот день
					*/

				result.cut_dates_for_goals = true;
			}
			else if (settings_child_nodes->item(i)->nodeName() == "overflow_mode")
			{
				/** определяет, что делать, если количество строк превышает max_result_size
					*/

				std::string overflow_mode_str = settings_child_nodes->item(i)->innerText();

				if (overflow_mode_str != "throw" && overflow_mode_str != "break" && overflow_mode_str != "any")
					throw Exception(std::string("Unknown overflow mode: ") + overflow_mode_str,
					ErrorCodes::UNKNOWN_OVERFLOW_MODE);

				result.has_overflow_mode = true;
				result.overflow_mode = overflow_mode_str == "throw" ? OVERFLOW_MODE_THROW
					: (overflow_mode_str == "break" ? OVERFLOW_MODE_BREAK
					: OVERFLOW_MODE_ANY);
			}
			else if (settings_child_nodes->item(i)->nodeName() == "concurrency")
			{
				/// выставить количество потоков для обработки запроса

				result.concurrency = DB::parse<unsigned>(settings_child_nodes->item(i)->innerText());
			}
			else if (settings_child_nodes->item(i)->nodeName() == "max_threads_per_counter")
			{
				/** Выставить локальное ограничение на максимальное количество обрабатываемых запросов
					* Оно может быть больше, чем ограничение по умолчанию.
					*/
				result.max_threads_per_counter = DB::parse<unsigned>(settings_child_nodes->item(i)->innerText());
			}
			else if (settings_child_nodes->item(i)->nodeName() == "local")
			{
				result.local = true;
			}
			else if (settings_child_nodes->item(i)->nodeName() == "sample")
			{
				result.sample = DB::parse<Float32>(settings_child_nodes->item(i)->innerText());
				if (result.sample <= 0 || result.sample > 1.)
					throw Exception(std::string("Wrong sample = ") + DB::toString(result.sample) + ". Sampling must be in range (0, 1]");
			}
			else if (settings_child_nodes->item(i)->nodeName() == "regions_point_of_view")
			{
				result.regions_point_of_view = settings_child_nodes->item(i)->innerText();
			}
		}
	}

	Poco::AutoPtr<Poco::XML::NodeList> limit_nodes = result.query->getElementsByTagName("limit");
	if (limit_nodes->length() > 1)
		throw Exception(std::string("Found more than one node limit"), ErrorCodes::FOUND_MORE_THAN_ONE_NODE);
	if (limit_nodes->length() == 1)
		result.limit = DB::parse<unsigned>(limit_nodes->item(0)->innerText());

	LOG_DEBUG(log, "CounterID: " << result.CounterID
		<< ", dates: " << LocalDate(result.date_first) << " - " << LocalDate(result.date_last));

	/// получаем список имён атрибутов
	Poco::AutoPtr<Poco::XML::NodeList> attributes = result.query->getElementsByTagName("attribute");
	for (unsigned i = 0; i < attributes->length(); i++)
	{
		std::string attribute_string = attributes->item(i)->innerText();
		AttributeWithParameter attr_with_param;
		std::string & attribute_name = attr_with_param.first;
		unsigned & attribute_param = attr_with_param.second;
		attribute_param = 0;

		if (attribute_string.find('(') != std::string::npos)
			attr_with_param = parseAttributeWithParameter(attribute_string);
		else
			attribute_name = attribute_string;

		if (attributes->item(i)->parentNode()->nodeName() == "keys")
		{
			QueryParseResult::KeyAttribute key_attribute;
			key_attribute.attribute = attribute_name;
			key_attribute.parameter = attribute_param;
			result.key_attributes.push_back(key_attribute);
		}

		if (attributes->item(i)->parentNode()->nodeName() == "aggregate")
		{
			Poco::AutoPtr<Poco::XML::NodeList> aggregate_nodes = attributes->item(i)->parentNode()->childNodes();

			unsigned j;
			for (j = 0; j < aggregate_nodes->length(); j++)
			{
				if (aggregate_nodes->item(j)->nodeName() == "function")
				{
					QueryParseResult::Aggregate aggregate;
					aggregate.attribute = attribute_name;
					aggregate.parameter = attribute_param;
					aggregate.function = aggregate_nodes->item(j)->innerText();
					result.aggregates.push_back(aggregate);
					break;
				}
			}

			if (j == aggregate_nodes->length())
				throw Exception(std::string("Not found 'function' element for aggregate with attribute ") + attribute_name,
									ErrorCodes::NOT_FOUND_FUNCTION_ELEMENT_FOR_AGGREGATE);
		}

		if (attributes->item(i)->parentNode()->nodeName() == "condition")
		{
			Poco::AutoPtr<Poco::XML::NodeList> condition_nodes = attributes->item(i)->parentNode()->childNodes();
			QueryParseResult::WhereCondition condition;
			condition.attribute = attribute_name;
			condition.parameter = attribute_param;

			unsigned j;
			for (j = 0; j < condition_nodes->length(); j++)
			{
				if (condition_nodes->item(j)->nodeName() == "relation")
				{
					condition.relation = condition_nodes->item(j)->innerText();
					break;
				}
			}

			if (j == condition_nodes->length())
				throw Exception(std::string("Not found 'relation' element for condition with attribute ") + attribute_name,
									ErrorCodes::NOT_FOUND_RELATION_ELEMENT_FOR_CONDITION);

			for (j = 0; j < condition_nodes->length(); j++)
			{
				if (condition_nodes->item(j)->nodeName() == "rhs")
				{
					condition.rhs = condition_nodes->item(j)->innerText();
					break;
				}
			}

			if (j == condition_nodes->length())
				throw Exception(std::string("Not found 'rhs' element for condition with attribute ") + attribute_name,
									ErrorCodes::NOT_FOUND_RHS_ELEMENT_FOR_CONDITION);

			result.where_conditions.push_back(condition);
		}
	}

	if (result.key_attributes.size() == 0)
		throw Exception("No attributes listed.", ErrorCodes::NO_ATTRIBUTES_LISTED);

	/// получаем условие сортировки
	Poco::AutoPtr<Poco::XML::NodeList> sort_nodes = result.query->getElementsByTagName("sort");
	if (sort_nodes->length() >= 1)
	{
		Poco::AutoPtr<Poco::XML::NodeList> column_nodes = sort_nodes->item(0)->childNodes();
		for (unsigned i = 0; i < column_nodes->length(); i++)
		{
			if (column_nodes->item(i)->nodeName() != "column")
				continue;

			QueryParseResult::SortColumn column;
			column.direction = "ascending";

			Poco::AutoPtr<Poco::XML::NodeList> index_direction_nodes = column_nodes->item(i)->childNodes();
			for (unsigned j = 0; j < index_direction_nodes->length(); j++)
			{
				if (index_direction_nodes->item(j)->nodeName() == "index")
				{
					column.index = DB::parse<unsigned>(index_direction_nodes->item(j)->innerText());
					if (column.index < 1 || column.index > result.key_attributes.size() + result.aggregates.size())
						throw Exception("Index of column in sort clause is out of range.",
											ErrorCodes::INDEX_OF_COLUMN_IN_SORT_CLAUSE_IS_OUT_OF_RANGE);
				}

				if (index_direction_nodes->item(j)->nodeName() == "direction")
				{
					column.direction = index_direction_nodes->item(j)->innerText();
					if (column.direction != "ascending" && column.direction != "descending")
						throw Exception("Unknown direction of sorting.",
											ErrorCodes::UNKNOWN_DIRECTION_OF_SORTING);
				}
			}

			result.sort_columns.push_back(column);
		}
	}
	return result;
}

}
}
