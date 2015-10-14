#include "OLAPQueryConverter.h"
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/WriteBufferFromString.h>


namespace DB
{
namespace OLAP
{

QueryConverter::QueryConverter(Poco::Util::AbstractConfiguration & config)
{
	table_for_single_counter = config.getString("olap_compatibility.table_for_single_counter");
	table_for_all_counters = config.getString("olap_compatibility.table_for_all_counters");
	
	fillFormattedAttributeMap();
	fillNumericAttributeMap();
	fillFormattingAggregatedAttributeMap();
	attribute_metadatas = GetOLAPAttributeMetadata();
}

static std::string firstWord(std::string s)
{
	for (size_t i = 0; i < s.length(); ++i)
	{
		if ((s[i] < 'a' || s[i] > 'z') && (s[i] < 'A' || s[i] > 'Z'))
		{
			s.erase(s.begin() + i, s.end());
			return s;
		}
	}
	return s;
}

void QueryConverter::OLAPServerQueryToClickHouse(const QueryParseResult & query, Context & inout_context, std::string & out_query) const
{
	/// Пустая строка, или строка вида ", 'ua'".
	std::string regions_point_of_view_formatted;

	if (!query.regions_point_of_view.empty())
	{
		std::stringstream tmp;
		tmp << ", " << mysqlxx::quote << query.regions_point_of_view;
		regions_point_of_view_formatted = tmp.str();
	}

	/// Проверим, умеем ли мы выполнять такой запрос.
	if (query.format != FORMAT_TAB)
		throw Exception("Only tab-separated output format is supported", ErrorCodes::UNSUPPORTED_PARAMETER);
	
	/// Учтем некоторые настройки (далеко не все).
	
	Settings new_settings = inout_context.getSettings();
	
	if (query.concurrency != 0)
		new_settings.max_threads = query.concurrency;
	
	if (query.max_execution_time != 0)
		new_settings.limits.max_execution_time = Poco::Timespan(query.max_execution_time, 0);
	
	if (query.max_result_size != 0)
		new_settings.limits.max_rows_to_group_by = query.max_result_size;
	
	if (query.has_overflow_mode)
	{
		switch (query.overflow_mode)
		{
			case OLAP::OVERFLOW_MODE_THROW:
				new_settings.limits.group_by_overflow_mode = DB::OverflowMode::THROW;
				break;
			case OLAP::OVERFLOW_MODE_BREAK:
				new_settings.limits.group_by_overflow_mode = DB::OverflowMode::BREAK;
				break;
			case OLAP::OVERFLOW_MODE_ANY:
				new_settings.limits.group_by_overflow_mode = DB::OverflowMode::ANY;
				break;
		}
	}
	
	inout_context.setSettings(new_settings);
	
	/// Составим запрос.
	out_query = "SELECT ";
	
	std::vector<std::string> selected_expressions;
	
	/// Что выбирать: ключи агрегации и агрегированные значения.
	for (size_t i = 0; i < query.key_attributes.size(); ++i)
	{
		const QueryParseResult::KeyAttribute & key = query.key_attributes[i];
		std::string s = convertAttributeFormatted(key.attribute, key.parameter, regions_point_of_view_formatted);
		
		if (i > 0)
			out_query += ", ";
		out_query += s + " AS _" + firstWord(key.attribute) + (key.parameter ? "_" + toString(key.parameter) : "");
		selected_expressions.push_back(s);
	}
	
	for (size_t i = 0; i < query.aggregates.size(); ++i)
	{
		const QueryParseResult::Aggregate & aggregate = query.aggregates[i];
		std::string s = convertAggregateFunction(aggregate.attribute, aggregate.parameter, aggregate.function, query, regions_point_of_view_formatted);
		
		if (query.key_attributes.size() + i > 0)
			out_query += ", ";
		out_query += s + " AS _" + firstWord(aggregate.function) + "_" + firstWord(aggregate.attribute) + (aggregate.parameter ? "_" + toString(aggregate.parameter) : "");
		selected_expressions.push_back(s);
	}
	
	/// Из какой таблицы.
	out_query += " FROM " + getTableName(query.CounterID, query.local);

	/// Добавляем сэмплирование.
	if (query.sample != 1)
		out_query += " SAMPLE " + toString(query.sample);
	
	/// Условия.
	out_query += " WHERE ";
	
	/// Диапазон дат.
	out_query += convertDateRange(query.date_first, query.date_last);
	
	/// Счетчик.
	if (query.CounterID != 0)
		out_query += " AND " + convertCounterID(query.CounterID);
	
	/// Произвольные условия.
	for (size_t i = 0; i < query.where_conditions.size(); ++i)
	{
		const QueryParseResult::WhereCondition & condition = query.where_conditions[i];
		out_query += " AND " + convertCondition(
			condition.attribute, condition.parameter, condition.relation, condition.rhs, regions_point_of_view_formatted);
	}
	
	/// Группировка.
	if (!query.key_attributes.empty())
	{
		out_query += " GROUP BY ";
		for (size_t i = 0; i < query.key_attributes.size(); ++i)
		{
			if (i > 0)
				out_query += ", ";
			out_query += selected_expressions[i];
		}
	}
	
	/// Условие для групп.
	out_query += " " + getHavingSection();
	
	/// Сортировка.
	if (!query.sort_columns.empty())
	{
		out_query += " ORDER BY ";
		for (size_t i = 0; i < query.sort_columns.size(); ++i)
		{
			const QueryParseResult::SortColumn & column = query.sort_columns[i];
			
			if (i > 0)
				out_query += ", ";
			out_query += selected_expressions[column.index - 1];
			out_query += " " + convertSortDirection(column.direction);
		}
	}
	
	/// Ограничение на количество выводимых строк.
	if (query.limit != 0)
		out_query += " LIMIT " + toString(query.limit);
}

std::string QueryConverter::convertAttributeFormatted(const std::string & attribute, unsigned parameter,
													  const std::string & regions_point_of_view_formatted) const
{
	if (formatted_attribute_map.count(attribute))
		return Poco::format(formatted_attribute_map.at(attribute), parameter);
	
	/** Для атрибутов по регионам, выражение содержит подстановку %s,
	  *  куда должна быть подставлена regions_point_of_view_formatted.
	  */
	if (regions_attributes_set.count(attribute))
		return Poco::format(numeric_attribute_map.at(attribute), regions_point_of_view_formatted);

	if (numeric_attribute_map.count(attribute))
	{
		std::string numeric = Poco::format(numeric_attribute_map.at(attribute), parameter);
		
		if (formatting_aggregated_attribute_map.count(attribute))
			return Poco::format(formatting_aggregated_attribute_map.at(attribute), std::string("(") + numeric + ")");
		else
			return numeric;
	}
	
	throw Exception("Unknown attribute: " + attribute, ErrorCodes::UNKNOWN_IDENTIFIER);
}

std::string QueryConverter::convertAttributeNumeric(const std::string & attribute, unsigned parameter,
													const std::string & regions_point_of_view_formatted) const
{
	/** Для атрибутов по регионам, выражение содержит подстановку %s,
	  *  куда должна быть подставлена regions_point_of_view_formatted.
	  */
	if (regions_attributes_set.count(attribute))
		return Poco::format(numeric_attribute_map.at(attribute), regions_point_of_view_formatted);

	if (numeric_attribute_map.count(attribute))
		return Poco::format(numeric_attribute_map.at(attribute), parameter);
	
	throw Exception("Unknown attribute: " + attribute, ErrorCodes::UNKNOWN_IDENTIFIER);
}

static bool StartsWith(const std::string & str, const std::string & prefix)
{
	return str.length() >= prefix.length() && str.substr(0, prefix.length()) == prefix;
}

std::string QueryConverter::convertAggregateFunction(const std::string & attribute, unsigned parameter, const std::string & name,
													const QueryParseResult & query, const std::string & regions_point_of_view_formatted) const
{
	bool float_value = false;

	/// если включено сэмплирование, то надо умножить агрегатные функции на 1./sample
	if (name == "count")
	{
		if (query.sample != 1)
		{
			float_value = true;
			return "sum(Sign)*" + toString(1./query.sample);
		}
		else
			return "sum(Sign)";
	}
	
	std::string numeric = convertAttributeNumeric(attribute, parameter, regions_point_of_view_formatted);
	
	if (name == "uniq" ||
		name == "uniq_sort" ||
		name == "uniq_hash" ||
		name == "uniq_approx" ||
		name == "sequental_uniq" ||
		StartsWith(name, "uniq_approx"))
		return "uniq(" + numeric + ")";

	if (name == "uniq_state")
		return "uniqState(" + numeric + ")";
	
	if (name == "uniq_hll12")
		return "uniqHLL12(" + numeric + ")";
	
	if (name == "uniq_hll12_state")
		return "uniqHLL12State(" + numeric + ")";
	
	if (name == "count_non_zero")
	{
		if (query.sample != 1)
		{
			float_value = true;
			return "sum((" + numeric + ") == 0 ? toInt64(0) : toInt64(Sign)) * " + toString(1/query.sample);
		}
		else
			return "sum((" + numeric + ") == 0 ? toInt64(0) : toInt64(Sign))";
	}

	if (name == "count_non_minus_one")
	{
		if (query.sample != 1)
		{
			float_value = true;
			return "sum((" + numeric + ") == -1 ? toInt64(0) : toInt64(Sign)) * " + toString(1/query.sample);
		}
		else
			return "sum((" + numeric + ") == -1 ? toInt64(0) : toInt64(Sign))";
	}

	bool trivial_format;
	
	std::string format;
	if (formatting_aggregated_attribute_map.count(attribute))
	{
		format = formatting_aggregated_attribute_map.at(attribute);
		trivial_format = false;
	}
	else
	{
		format = "%s";
		trivial_format = true;
	}

	std::string s;
	
	if (name == "sum")
	{
		if (query.sample != 1)
		{
			s = "sum((" + numeric + ") * Sign) * " + toString(1/query.sample);
			float_value = true;
		}
		else
			s = "sum((" + numeric + ") * Sign)";
	}

	if (name == "sum_non_minus_one")
	{
		if (query.sample != 1)
		{
			s = "sum((" + numeric + ") == -1 ? toInt64(0) : toInt64(" + numeric + ") * Sign) * " + toString(1/query.sample);
			float_value = true;
		}
		else
			s = "sum((" + numeric + ") == -1 ? toInt64(0) : toInt64(" + numeric + ") * Sign)";
	}
	if (name == "avg")
	{
		s = "sum((" + numeric + ") * Sign) / sum(Sign)";
		float_value = true;
	}
	if (name == "avg_non_zero")
	{
		s = "sum((" + numeric + ") * Sign) / sum((" + numeric + ") == 0 ? toInt64(0) : toInt64(Sign))";
		float_value = true;
	}
	if (name == "avg_non_minus_one")
	{
		s = "sum((" + numeric + ") == -1 ? toInt64(0) : toInt64(" + numeric + ") * Sign) / sum((" + numeric + ") == -1 ? toInt64(0) : toInt64(Sign))";
		float_value = true;
	}
	if (name == "min")
		s = "min(" + numeric + ")";
	if (name == "max")
		s = "max(" + numeric + ")";
	
	/// Если агрегатная функция возвращает дробное число, и атрибут имеет нетривиальное форматирование, после агрегации приведем дробное число к целому.
	bool need_cast = !trivial_format && float_value;
	
	return Poco::format(format, std::string() + (need_cast ? "toInt64" : "") + "(" + s + ")");
}

std::string QueryConverter::convertConstant(const std::string & attribute, const std::string & value) const
{
	if (!attribute_metadatas.count(attribute))
		throw Exception("Unknown attribute " + attribute, ErrorCodes::UNKNOWN_IDENTIFIER);
	return toString(attribute_metadatas.at(attribute)->parse(value));
}

std::string QueryConverter::convertCondition(
	const std::string & attribute,
	unsigned parameter,
	const std::string & name,
	const std::string & rhs,
	const std::string & regions_point_of_view_formatted) const
{
	std::string value = convertAttributeNumeric(attribute, parameter, regions_point_of_view_formatted);
	std::string constant = convertConstant(attribute, rhs);
	
	if (name == "equals")
		return "(" + value + ")" + " == " + constant;
	if (name == "not_equals")
		return "(" + value + ")" + " != " + constant;
	if (name == "less")
		return "(" + value + ")" + " < " + constant;
	if (name == "greater")
		return "(" + value + ")" + " > " + constant;
	if (name == "less_or_equals")
		return "(" + value + ")" + " <= " + constant;
	if (name == "greater_or_equals")
		return "(" + value + ")" + " >= " + constant;
	if (name == "region_in")
		return "regionIn(" + value + ", toUInt32(" + constant + ")" + regions_point_of_view_formatted + ")";
	if (name == "region_not_in")
		return "NOT regionIn(" + value + ", toUInt32(" + constant + ")" + regions_point_of_view_formatted + ")";
	if (name == "os_in")
		return "OSIn(" + value + ", " + constant + ")";
	if (name == "os_not_in")
		return "NOT OSIn(" + value + ", " + constant + ")";
	if (name == "se_in")
		return "SEIn(toUInt8(" + value + "), toUInt8(" + constant + "))";
	if (name == "se_not_in")
		return "NOT SEIn(toUInt8(" + value + "), toUInt8(" + constant + "))";
	if (name == "interest_has_all_from")
		return "bitwiseAnd(" + value + ", " + constant + ") == " + constant;
	if (name == "interest_not_has_all_from")
		return "bitwiseAnd(" + value + ", " + constant + ") != " + constant;
	if (name == "interest_has_any_from")
		return "bitwiseAnd(" + value + ", " + constant + ") != 0";
	if (name == "interest_not_has_any_from")
		return "bitwiseAnd(" + value + ", " + constant + ") == 0";
	throw Exception("Unknown relation " + name, ErrorCodes::UNKNOWN_RELATION);
}

std::string QueryConverter::convertSortDirection(const std::string & direction) const
{
	if (direction == "descending")
		return "DESC";
	else
		return "ASC";
}

std::string QueryConverter::convertDateRange(time_t date_first, time_t date_last) const
{
	std::string first_str;
	std::string last_str;
	{
		WriteBufferFromString first_buf(first_str);
		WriteBufferFromString last_buf(last_str);
		writeDateText(DateLUT::instance().toDayNum(date_first), first_buf);
		writeDateText(DateLUT::instance().toDayNum(date_last), last_buf);
	}
	return "StartDate >= toDate('" + first_str + "') AND StartDate <= toDate('" + last_str + "')";
}

std::string QueryConverter::convertCounterID(CounterID_t CounterID) const
{
	return "CounterID == " + toString(CounterID);
}

std::string QueryConverter::getTableName(CounterID_t CounterID, bool local) const
{
	if (CounterID == 0 && !local)
		return table_for_all_counters;
	else
		return table_for_single_counter;
}

std::string QueryConverter::getHavingSection() const
{
	return "HAVING sum(Sign) > 0";
}

void QueryConverter::fillNumericAttributeMap()
{
#define M(a, b) numeric_attribute_map[a] = b;
	M("DummyAttribute",       "0")
	M("VisitStartDateTime",   				"toInt32(StartTime)")
	M("VisitStartDateTimeRoundedToMinute", 	"toInt32(toStartOfMinute(StartTime))")
	M("VisitStartDateTimeRoundedToHour",   	"toInt32(toStartOfHour(StartTime))")
	M("VisitStartDate",       				"toInt32(toDateTime(StartDate))")
	M("VisitStartDateRoundedToMonth",      	"toInt32(toDateTime(toStartOfMonth(StartDate)))")
	M("VisitStartWeek",       				"toInt32(toDateTime(toMonday(StartDate)))")
	M("VisitStartTime",       				"toInt32(toTime(StartTime))")
	M("VisitStartTimeRoundedToMinute",		"toInt32(toStartOfMinute(toTime(StartTime)))")
	
	M("VisitStartYear",       "toYear(StartDate)")
	M("VisitStartMonth",      "toMonth(StartDate)")
	M("VisitStartDayOfWeek",  "toDayOfWeek(StartDate)")
	M("VisitStartDayOfMonth", "toDayOfMonth(StartDate)")
	M("VisitStartHour",       "toHour(StartTime)")
	M("VisitStartMinute",     "toMinute(StartTime)")
	M("VisitStartSecond",     "toSecond(StartTime)")

	M("FirstVisitDateTime",   "toInt32(FirstVisit)")
	M("FirstVisitDate",       "toInt32(toDateTime(toDate(FirstVisit)))")
	M("FirstVisitWeek",       "toInt32(toDateTime(toMonday(FirstVisit)))")
	M("FirstVisitTime",       "toInt32(toTime(FirstVisit))")
	
	M("FirstVisitYear",       "toYear(FirstVisit)")
	M("FirstVisitMonth",      "toMonth(FirstVisit)")
	M("FirstVisitDayOfWeek",  "toDayOfWeek(FirstVisit)")
	M("FirstVisitDayOfMonth", "toDayOfMonth(FirstVisit)")
	M("FirstVisitHour",       "toHour(FirstVisit)")
	M("FirstVisitMinute",     "toMinute(FirstVisit)")
	M("FirstVisitSecond",     "toSecond(FirstVisit)")

	M("PredLastVisitDate",    "toInt32(toDateTime(PredLastVisit))")
	M("PredLastVisitWeek",    "toInt32(toDateTime(toMonday(PredLastVisit)))")
	M("PredLastVisitYear",    "toYear(PredLastVisit)")
	M("PredLastVisitMonth",   "toMonth(PredLastVisit)")
	M("PredLastVisitDayOfWeek","toDayOfWeek(PredLastVisit)")
	M("PredLastVisitDayOfMonth","toDayOfMonth(PredLastVisit)")

	M("ClientDateTime",       "toInt32(ClientEventTime)")
	M("ClientTime",           "toInt32(toTime(ClientEventTime))")
	M("ClientTimeHour",       "toHour(ClientEventTime)")
	M("ClientTimeMinute",     "toMinute(ClientEventTime)")
	M("ClientTimeSecond",     "toSecond(ClientEventTime)")
	
	M("SearchPhraseHash",     "SearchPhraseHash")
	M("RefererDomainHash",    "RefererDomainHash")
	M("StartURLHash",         "NormalizedStartURLHash")
	M("StartURLDomainHash",   "StartURLDomainHash")
	M("RegionID",             "RegionID")
	M("RegionCity",           "regionToCity(RegionID%s)")
	M("RegionArea",           "regionToArea(RegionID%s)")
	M("RegionCountry",        "regionToCountry(RegionID%s)")
	M("TraficSourceID",       "TraficSourceID")
	M("IsNewUser",            "intDiv(toUInt32(FirstVisit), 1800) == intDiv(toUInt32(StartTime), 1800)")
	M("UserNewness",          "intDiv(toUInt64(StartTime)-toUInt64(FirstVisit), 86400)")
	M("UserNewnessInterval",  "roundToExp2(intDiv(toUInt64(StartTime)-toUInt64(FirstVisit), 86400))")
	M("UserReturnTime",       "toInt32(toDate(StartTime))-toInt32(PredLastVisit)")
	M("UserReturnTimeInterval","roundToExp2(toInt32(toDate(StartTime))-toInt32(PredLastVisit))")
	M("UserVisitsPeriod",     "(TotalVisits <= 1 ? toUInt16(0) : toUInt16((toInt64(StartTime)-toInt64(FirstVisit)) / (86400 * (TotalVisits - 1))))")
	M("UserVisitsPeriodInterval","(TotalVisits <= 1 ? toUInt16(0) : roundToExp2(toUInt16((toInt64(StartTime)-toInt64(FirstVisit)) / (86400 * (TotalVisits - 1)))))")
	M("VisitTime",            "Duration")
	M("VisitTimeInterval",    "roundDuration(Duration)")
	M("PageViews",            "PageViews")
	M("PageViewsInterval",    "roundToExp2(PageViews)")
	M("Bounce",               "PageViews <= 1")
	M("BouncePrecise",        "IsBounce")
	M("IsYandex",             "IsYandex")
	M("UserID",               "UserID")
	
	M("UserIDCreateDateTime", "(UserID > 10000000000000000000 OR UserID %% 10000000000 > 2000000000 OR UserID %% 10000000000 < 1000000000 ? toUInt64(0) : UserID %% 10000000000)")
	M("UserIDCreateDate",     "(UserID > 10000000000000000000 OR UserID %% 10000000000 > 2000000000 OR UserID %% 10000000000 < 1000000000 ? toUInt64(0) : UserID %% 10000000000)")
	
	M("UserIDAge",            "(UserID > 10000000000000000000 OR UserID %% 10000000000 < 1000000000 OR UserID %% 10000000000 > toUInt64(StartTime) ? toInt64(-1) : intDiv(toInt64(StartTime) - UserID %% 10000000000, 86400))")
	M("UserIDAgeInterval",    "(UserID > 10000000000000000000 OR UserID %% 10000000000 < 1000000000 OR UserID %% 10000000000 > toUInt64(StartTime) ? toInt64(-1) : toInt64(roundToExp2(intDiv(toUInt64(StartTime) - UserID %% 10000000000, 86400))))")
	M("TotalVisits",          "TotalVisits")
	M("TotalVisitsInterval",  "roundToExp2(TotalVisits)")
	M("Age",                  "Age")
	M("AgeInterval",          "roundAge(Age)")
	M("Sex",                  "Sex")
	M("Income",               "Income")
	M("AdvEngineID",          "AdvEngineID")
	
	M("DotNet",               "NetMajor * 256 + NetMinor")
	
	M("DotNetMajor",          "NetMajor")
	
	M("Flash",                "FlashMajor * 256 + FlashMinor")
	
	M("FlashExists",          "FlashMajor > 0")
	M("FlashMajor",           "FlashMajor")
	
	M("Silverlight",          "SilverlightVersion1 * 72057594037927936 + SilverlightVersion2 * 281474976710656 + SilverlightVersion3 * 65536 + SilverlightVersion4")
	
	M("SilverlightMajor",     "SilverlightVersion1")
	M("Hits",                 "Hits")
	M("HitsInterval",         "roundToExp2(Hits)")
	M("JavaEnable",           "JavaEnable")
	M("CookieEnable",         "CookieEnable")
	M("JavascriptEnable",     "JavascriptEnable")
	M("IsMobile",             "IsMobile")
	M("MobilePhoneID",        "MobilePhone")
	M("MobilePhoneModelHash", "halfMD5(MobilePhoneModel)")
	
	M("MobilePhoneModel",     "reinterpretAsUInt64(MobilePhoneModel)")
	M("BrowserLanguage",      "BrowserLanguage")
	M("BrowserCountry",       "BrowserCountry")
	M("TopLevelDomain",       "TopLevelDomain")
	M("URLScheme",            "URLScheme")
	
	M("IPNetworkID",          "IPNetworkID")
	M("ClientTimeZone",       "ClientTimeZone")
	M("OSID",                 "OS")
	M("OSMostAncestor",       "OSToRoot(OS)")
	
	M("ClientIP",             "ClientIP")
	M("Resolution",           "ResolutionWidth * 16777216 + ResolutionHeight * 256 + ResolutionDepth")
	M("ResolutionWidthHeight","ResolutionWidth * 65536 + ResolutionHeight")
	
	M("ResolutionWidth",      "ResolutionWidth")
	M("ResolutionHeight",     "ResolutionHeight")
	M("ResolutionWidthInterval","intDiv(ResolutionWidth, 100) * 100")
	M("ResolutionHeightInterval","intDiv(ResolutionHeight, 100) * 100")
	M("ResolutionColor",      "ResolutionDepth")
	
	M("WindowClientArea",     "WindowClientWidth * 65536 + WindowClientHeight")
	
	M("WindowClientAreaInterval","intDiv(WindowClientWidth, 100) * 6553600 + intDiv(WindowClientHeight, 100) * 100")
	M("WindowClientWidth",    "WindowClientWidth")
	M("WindowClientWidthInterval","intDiv(WindowClientWidth, 100) * 100")
	M("WindowClientHeight",   "WindowClientHeight")
	M("WindowClientHeightInterval","intDiv(WindowClientHeight, 100) * 100")
	M("SearchEngineID",       "SearchEngineID")
	M("SearchEngineMostAncestor", "SEToRoot(toUInt8(SearchEngineID))")
	M("CodeVersion",          "CodeVersion")
	
	M("UserAgent",            "UserAgent * 16777216 + UserAgentMajor * 65536 + UserAgentMinor")
	M("UserAgentVersion",     "UserAgentMajor * 65536 + UserAgentMinor")
	M("UserAgentMajor",       "UserAgent * 256 + UserAgentMajor")
	
	M("UserAgentID",          "UserAgent")
	M("ClickGoodEvent",       "ClickGoodEvent")
	M("ClickPriorityID",      "ClickPriorityID")
	M("ClickBannerID",        "ClickBannerID")
	M("ClickPageID",          "ClickPageID")
	M("ClickPlaceID",         "ClickPlaceID")
	M("ClickTypeID",          "ClickTypeID")
	M("ClickResourceID",      "ClickResourceID")
	M("ClickDomainID",        "ClickDomainID")
	M("ClickCost",            "ClickCost")
	M("ClickURLHash",         "ClickURLHash")
	M("ClickOrderID",         "ClickOrderID")
	M("GoalReachesAny",       "GoalReachesAny")
	M("GoalReachesDepth",     "GoalReachesDepth")
	M("GoalReachesURL",       "GoalReachesURL")
	M("ConvertedAny",         "(GoalReachesAny > 1 ? toInt32(1) : GoalReachesAny)")
	M("ConvertedDepth",       "(GoalReachesDepth > 1 ? toInt32(1) : GoalReachesDepth)")
	M("ConvertedURL",         "(GoalReachesURL > 1 ? toInt32(1) : GoalReachesURL)")
	M("GoalReaches",          "countEqual(Goals.ID, toUInt32(%u))")
	M("Converted",            "has(Goals.ID, toUInt32(%u))")
	M("CounterID",            "CounterID")
	M("VisitID",              "VisitID")
	
	M("Interests",            "Interests")
	
	M("HasInterestPhoto",     "modulo(intDiv(Interests, 128), 2)")
	M("HasInterestMoviePremieres","modulo(intDiv(Interests, 64), 2)")
	M("HasInterestTourism",   "modulo(intDiv(Interests, 32), 2)")
	M("HasInterestFamilyAndChildren","modulo(intDiv(Interests, 16), 2)")
	M("HasInterestFinance",   "modulo(intDiv(Interests, 8), 2)")
	M("HasInterestB2B",       "modulo(intDiv(Interests, 4), 2)")
	M("HasInterestCars",      "modulo(intDiv(Interests, 2), 2)")
	M("HasInterestMobileAndInternetCommunications","modulo(Interests, 2)")
	M("HasInterestBuilding",  "modulo(intDiv(Interests, 256), 2)")
	M("HasInterestCulinary",  "modulo(intDiv(Interests, 512), 2)")
	M("OpenstatServiceNameHash","OpenstatServiceNameHash")
	M("OpenstatCampaignIDHash","OpenstatCampaignIDHash")
	M("OpenstatAdIDHash",     "OpenstatAdIDHash")
	M("OpenstatSourceIDHash", "OpenstatSourceIDHash")
	M("UTMSourceHash",        "UTMSourceHash")
	M("UTMMediumHash",        "UTMMediumHash")
	M("UTMCampaignHash",      "UTMCampaignHash")
	M("UTMContentHash",       "UTMContentHash")
	M("UTMTermHash",          "UTMTermHash")
	M("FromHash",             "FromHash")
	M("CLID",                 "CLID")
	
	M("SocialSourceNetworkID","SocialSourceNetworkID")
	/// где 26 это Яндекс (db_dumps/SearchEngines).
	M("CorrectedTraficSourceID", "(IsYandex AND SEIn(toUInt8(SearchEngineID), 26)) ? -1 : TraficSourceID")
	M("CorrectedSearchEngineID", "(IsYandex AND SEIn(toUInt8(SearchEngineID), 26)) ? 0 : toUInt8(SearchEngineID)")
	
#undef M
}

void QueryConverter::fillFormattedAttributeMap()
{
#define M(a, b) formatted_attribute_map[a] = b;
	M("VisitStartDateTime",   "StartTime")
	M("VisitStartDate",       "StartDate")
	M("VisitStartWeek",       "toMonday(StartDate)")
	M("VisitStartTime",       "substring(toString(toTime(StartTime)), 12, 8)")

	M("VisitStartDateTimeRoundedToMinute", 	"toStartOfMinute(StartTime)")
	M("VisitStartDateTimeRoundedToHour",   	"toStartOfHour(StartTime)")
	M("VisitStartDateRoundedToMonth",      	"toDateTime(toStartOfMonth(StartDate))")
	M("VisitStartTimeRoundedToMinute",		"substring(toString(toStartOfMinute(toTime(StartTime))), 12, 8)")
	
	M("FirstVisitDateTime",   "FirstVisit")
	M("FirstVisitDate",       "toDate(FirstVisit)")
	M("FirstVisitWeek",       "toMonday(FirstVisit)")
	M("FirstVisitTime",       "substring(toString(FirstVisit), 12, 8)")
	
	M("PredLastVisitDate",    "PredLastVisit")
	M("PredLastVisitWeek",    "toMonday(PredLastVisit)")
	
	M("ClientDateTime",       "ClientEventTime")
	M("ClientTime",           "substring(toString(ClientEventTime), 12, 8)")
	
	M("DotNet",               "concat(concat(toString(NetMajor), '.'), toString(NetMinor))")
	M("Flash",                "concat(concat(toString(FlashMajor),'.'),toString(FlashMinor))")
	M("Silverlight",          "concat(concat(concat(concat(concat(concat(toString(SilverlightVersion1), '.'), toString(SilverlightVersion2)), '.'), toString(SilverlightVersion3)), '.'), toString(SilverlightVersion4))")
	M("MobilePhoneModel",     "MobilePhoneModel")
	
	M("ClientIP",             "IPv4NumToString(ClientIP)")
	M("Resolution",           "concat(concat(concat(concat(toString(ResolutionWidth),'x'),toString(ResolutionHeight)),'x'),toString(ResolutionDepth))")
	M("ResolutionWidthHeight","concat(concat(toString(ResolutionWidth),'x'),toString(ResolutionHeight))")
	
	M("WindowClientArea",     "concat(concat(toString(WindowClientWidth),'x'),toString(WindowClientHeight))")
	
	M("UserAgent",            "concat(concat(concat(toString(UserAgent), ' '), toString(UserAgentMajor)), UserAgentMinor == 0 ? '' : concat('.', reinterpretAsString(UserAgentMinor)))")
	M("UserAgentVersion",     "concat(toString(UserAgentMajor), UserAgentMinor == 0 ? '' : concat('.', reinterpretAsString(UserAgentMinor)))")
	M("UserAgentMajor",       "concat(concat(toString(UserAgent), ' '), toString(UserAgentMajor))")
#undef M	
}

void QueryConverter::fillFormattingAggregatedAttributeMap()
{
#define M(a, b) formatting_aggregated_attribute_map[a] = b;
	std::string todate = "toDate(toDateTime(%s))";
	std::string todatetime = "toDateTime(%s)";
	std::string cuttime = "substring(toString(toDateTime(%s)), 12, 8)";
	std::string tostring = "reinterpretAsString(%s)";

	M("VisitStartDateTime",   todatetime)
	M("VisitStartDate",       todate)
	M("VisitStartWeek",       todate)
	M("VisitStartTime",       cuttime)

	M("VisitStartDateTimeRoundedToMinute", 	todatetime)
	M("VisitStartDateTimeRoundedToHour",   	todatetime)
	M("VisitStartDateRoundedToMonth",      	todate)
	M("VisitStartTimeRoundedToMinute",		cuttime)

	M("FirstVisitDateTime",   todatetime)
	M("FirstVisitDate",       todate)
	M("FirstVisitWeek",       todate)
	M("FirstVisitTime",       cuttime)

	M("PredLastVisitDate",    todate)
	M("PredLastVisitWeek",    todate)

	M("ClientDateTime",       todatetime)
	M("ClientTime",           cuttime)

	M("UserIDCreateDateTime", todatetime)
	M("UserIDCreateDate",     todate)

	M("DotNet",               "concat(concat(toString(intDiv(toUInt32(%[0]s), 256)), '.'), toString(modulo(toUInt32(%[0]s), 256)))")

	M("Flash",                "concat(concat(toString(intDiv(toUInt32(%[0]s), 256)), '.'), toString(modulo(toUInt32(%[0]s), 256)))")

	M("Silverlight",          "concat(concat(concat(concat(concat(concat(toString(intDiv(toUInt64(%[0]s), 72057594037927936)), '.'), toString(modulo(intDiv(toUInt64(%[0]s), 281474976710656), 256))), '.'), toString(modulo(intDiv(toUInt64(%[0]s), 65536), 4294967296))), '.'), toString(modulo(toUInt64(%[0]s), 65536)))")

	M("MobilePhoneModel",     tostring)
	M("BrowserLanguage",      tostring)
	M("BrowserCountry",       tostring)
	M("TopLevelDomain",       tostring)
	M("URLScheme",            tostring)

	M("ClientIP",             "IPv4NumToString(%[0]s)")
	M("Resolution",           "concat(concat(concat(concat(toString(intDiv(toUInt64(%[0]s), 16777216)),'x'),toString(intDiv(toUInt64(%[0]s), 256) %% 65536)),'x'),toString(toUInt64(%[0]s) %% 256))")
	M("ResolutionWidthHeight","concat(concat(toString(intDiv(toUInt64(%[0]s), 65536)),'x'),toString(toUInt64(%[0]s) %% 65536))")

	M("WindowClientArea",     "concat(concat(toString(intDiv(toUInt64(%[0]s), 65536)),'x'),toString(toUInt64(%[0]s) %% 65536))")

	M("UserAgent",            "concat(concat(concat(toString(intDiv(toUInt32(%[0]s), 16777216)), ' '), toString(intDiv(toUInt32(%[0]s), 65536) %% 256)), (toUInt32(%[0]s) %% 65536) == 0 ? '' : concat('.', reinterpretAsString(toUInt32(%[0]s) %% 65536)))")
	M("UserAgentVersion",     "concat(toString(intDiv(toUInt32(%[0]s), 65536)), (toUInt32(%[0]s) %% 65536) == 0 ? '' : concat('.', reinterpretAsString(toUInt32(%[0]s) %% 65536)))")
	M("UserAgentMajor",       "concat(concat(toString(intDiv(toUInt32(%[0]s), 256)), ' '), toString(toUInt32(%[0]s) %% 256))")

	M("Interests",            "bitmaskToList(%s)")
#undef M
}

}
}
