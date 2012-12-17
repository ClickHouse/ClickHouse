#include "OLAPQueryConverter.h"
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/WriteBufferFromString.h>
#include <Poco/NumberFormatter.h>


namespace DB
{
namespace OLAP
{

QueryConverter::QueryConverter(Poco::Util::AbstractConfiguration & config)
{
	table_for_single_counter = config.getString("olap_table_for_single_counter");
	table_for_all_counters = config.getString("olap_table_for_all_counters");
}

void QueryConverter::OLAPServerQueryToClickhouse(const QueryParseResult & query, Context & inout_context, std::string & out_query)
{
	/// Проверим, умеем ли мы выполнять такой запрос.
	if (query.is_list_of_visits_query)
		throw Exception("List of visits queries not supported", ErrorCodes::UNSUPPORTED_PARAMETER);
	if (query.format != FORMAT_TAB)
		throw Exception("Only tab-separated output format is supported", ErrorCodes::UNSUPPORTED_PARAMETER);
	
	/// Учтем некоторые настройки (пока далеко не все).
	
	Settings new_settings = inout_context.getSettings();
	
	if (query.concurrency != 0)
		new_settings.max_threads = query.concurrency;
	
	inout_context.setSettings(new_settings);
	
	/// Составим запрос.
	out_query = "SELECT ";
	
	/// Что выбирать: ключи агрегации и агрегированные значения.
	for (size_t i = 0; i < query.key_attributes.size(); ++i)
	{
		const QueryParseResult::KeyAttribute & key = query.key_attributes[i];
		
		if (i > 0)
			out_query += ", ";
		out_query += convertAggregationKey(key.attribute, key.parameter);
	}
	
	for (size_t i = 0; i < query.aggregates.size(); ++i)
	{
		const QueryParseResult::Aggregate & aggregate = query.aggregates[i];
		
		if (i > 0)
			out_query += ", ";
		out_query += convertAggregateFunction(aggregate.attribute, aggregate.parameter, aggregate.function);
	}
	
	/// Из какой таблицы.
	out_query += " FROM " + getTableName(query.CounterID);
	
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
		out_query += " AND " + convertCondition(condition.attribute, condition.parameter, condition.relation, condition.rhs);
	}
	
	/// Группировка.
	if (!query.key_attributes.empty())
	{
		out_query += " GROUP BY ";
		for (size_t i = 0; i < query.key_attributes.size(); ++i)
		{
			const QueryParseResult::KeyAttribute & key = query.key_attributes[i];
			
			if (i > 0)
				out_query += ", ";
			out_query += convertAggregationKey(key.attribute, key.parameter);
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
			if (column.index < query.key_attributes.size())
			{
				const QueryParseResult::KeyAttribute & key = query.key_attributes[column.index];
				out_query += convertSortByKey(key.attribute, key.parameter);
			}
			else
			{
				const QueryParseResult::Aggregate & aggregate = query.aggregates[column.index - query.key_attributes.size()];
				out_query += convertSortByAggregate(aggregate.attribute, aggregate.parameter, aggregate.function);
			}
			out_query += " " + convertSortDirection(column.direction);
		}
	}
	
	/// Ограничение на количество выводимых строк.
	if (query.limit != 0)
		out_query += " LIMIT " + Poco::NumberFormatter::format(query.limit);
}

std::string QueryConverter::convertAttribute(const std::string & attribute, unsigned parameter, AttributeUsage usage)
{
	if (usage == ATTRIBUTE_KEY)
		return Poco::format(key_attribute_map[attribute], parameter);
	throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

/// <keys><attribute> => SELECT ... GROUP BY x
std::string QueryConverter::convertAggregationKey(const std::string & attribute, unsigned parameter)
{
	return convertAttribute(attribute, parameter, ATTRIBUTE_KEY);
}

/// <aggregates><aggregate> => SELECT x
std::string QueryConverter::convertAggregateFunction(const std::string & attribute, unsigned parameter, const std::string & function)
{
	return "";
}

/// <where><condition><rhs> => SELECT ... where F(A, x)
std::string QueryConverter::convertConstant(const std::string & attribute, const std::string & value)
{
	return "";
}

/// <where><condition> => SELECT ... WHERE x
std::string QueryConverter::convertCondition(const std::string & attribute, unsigned parameter, const std::string & relation, const std::string & rhs)
{
	return "";
}

/// <sort><column> => SELECT ... ORDER BY x
std::string QueryConverter::convertSortByKey(const std::string & attribute, unsigned parameter)
{
	return "";
}

/// <sort><column> => SELECT ... ORDER BY x
std::string QueryConverter::convertSortByAggregate(const std::string & attribute, unsigned parameter, const std::string & function)
{
	return "";
}

/// ASC или DESC
std::string QueryConverter::convertSortDirection(const std::string & direction)
{
	if (direction == "descending")
		return "ASC";
	else
		return "DESC";
}

/// <dates> => SELECT ... WHERE x
std::string QueryConverter::convertDateRange(time_t date_first, time_t date_last)
{
	std::string first_str;
	std::string last_str;
	{
		WriteBufferFromString first_buf(first_str);
		WriteBufferFromString last_buf(last_str);
		writeDateText(Yandex::DateLUTSingleton::instance().toDayNum(date_first), first_buf);
		writeDateText(Yandex::DateLUTSingleton::instance().toDayNum(date_last), last_buf);
	}
	return "StartDate >= '" + first_str + "' AND StartDate <= '" + last_str + "'";
}

/// <counter_id> => SELECT ... WHERE x
std::string QueryConverter::convertCounterID(Yandex::CounterID_t CounterID)
{
	return "CounterID = " + Poco::NumberFormatter::format(CounterID);
}

std::string QueryConverter::getTableName(Yandex::CounterID_t CounterID)
{
	if (CounterID == 0)
		return table_for_all_counters;
	else
		return table_for_single_counter;
}

std::string QueryConverter::getHavingSection()
{
	return "HAVING sum(Sign) > 0";
}

void QueryConverter::fillKeyAttributeMap()
{
#define M(a, b) key_attribute_map[a] = b;
	M("Dummy",                "0")
	M("VisitStartDateTime",   "StartTime")
	M("VisitStartDate",       "StartDate")
	M("VisitStartWeek",       "toMonday(StartDate)")
	M("VisitStartTime",       "substring(toString(StartTime), 12, 8)")
	M("VisitStartYear",       "toYear(StartDate)")
	M("VisitStartMonth",      "toMonth(StartDate)")
	M("VisitStartDayOfWeek",  "toDayOfWeek(StartDate)")
	M("VisitStartDayOfMonth", "toDayOfMonth(StartDate)")
	M("VisitStartHour",       "toHour(StartTime)")
	M("VisitStartMinute",     "toMinute(StartTime)")
	M("VisitStartSecond",     "toSecond(StartTime)")
	
	M("FirstVisitDateTime",   "toDateTime(FirstVisit)")
	M("FirstVisitDate",       "toDate(toDateTime(FirstVisit))")
	M("FirstVisitWeek",       "toMonday(toDateTime(FirstVisit))")
	M("FirstVisitTime",       "substring(toString(toDateTime(FirstVisit)), 12, 8)")
	M("FirstVisitYear",       "toYear(toDateTime(FirstVisit))")
	M("FirstVisitMonth",      "toMonth(toDateTime(FirstVisit))")
	M("FirstVisitDayOfWeek",  "toDayOfWeek(toDateTime(FirstVisit))")
	M("FirstVisitDayOfMonth", "toDayOfMonth(toDateTime(FirstVisit))")
	M("FirstVisitHour",       "toHour(toDateTime(FirstVisit))")
	M("FirstVisitMinute",     "toMinute(toDateTime(FirstVisit))")
	M("FirstVisitSecond",     "toSecond(toDateTime(FirstVisit))")
	
	M("PredLastVisitDate",    "toDate(toDateTime(PredLastVisit))")
	M("PredLastVisitWeek",    "toMonday(toDateTime(PredLastVisit))")
	M("PredLastVisitYear",    "toYear(toDateTime(PredLastVisit))")
	M("PredLastVisitMonth",   "toMonth(toDateTime(PredLastVisit))")
	M("PredLastVisitDayOfWeek","toDayOfWeek(toDateTime(PredLastVisit))")
	M("PredLastVisitDayOfMonth", "toDayOfMonth(toDateTime(PredLastVisit))")
	
	M("ClientDateTime",       "ClientEventTime")
	M("ClientTime",           "substring(toString(ClientEventTime), 12, 8)")
	M("ClientTimeHour",       "toHour(ClientEventTime)")
	M("ClientTimeMinute",     "toMinute(ClientEventTime)")
	M("ClientTimeSecond",     "toSecond(ClientEventTime)")
	
	M("EndURLHash",           "halfMD5(EndURL)")
	M("RefererHash",          "halfMD5(Referer)")
	M("SearchPhraseHash",     "halfMD5SearchPhrase()")
	M("RefererDomainHash",    "halfMD5(domainWithoutWWW(Referer))")
	M("StartURLHash",         "halfMD5(StartURL)")
	M("StartURLDomainHash",   "halfMD5(domainWithoutWWW(StartURL))")
	M("RegionID",             "RegionID")
	M("RegionCity",           "regionToCity(RegionID)")
	M("RegionArea",           "regionToArea(RegionID)")
	M("RegionCountry",        "regionToCountry(RegionID)")
	M("TraficSourceID",       "TraficSourceID")
	M("IsNewUser",            "IsNew")
	M("UserNewness",          "intDiv(toUInt64(StartTime)-FirstVisit, 86400)")
	M("UserNewnessInterval",  "roundToExp2(intDiv(toUInt64(StartTime)-FirstVisit, 86400))")
	M("UserReturnTime",       "intDiv(toUInt64(StartTime)-PredLastVisit, 86400)")
	M("UserReturnTimeInterval","roundToExp2(intDiv(toUInt64(StartTime)-PredLastVisit, 86400))")
	M("UserVisitsPeriod",     "TotalVisits <= 1 ? toUInt64(0) : intDiv(toUInt64(StartTime)-FirstVisit, 86400 * (TotalVisits - 1))")
	M("UserVisitsPeriodInterval","TotalVisits <= 1 ? toUInt64(0) : roundToExp2(intDiv(toUInt64(StartTime)-FirstVisit, 86400 * (TotalVisits - 1)))")
	M("VisitTime",            "Duration")
	M("VisitTimeInterval",    "roundDuration(Duration)")
	M("PageViews",            "PageViews")
	M("PageViewsInterval",    "roundToExp2(PageViews)")
	M("Bounce",               "(PageViews <= 1)")
	M("BouncePrecise",        "IsBounce")
	M("IsYandex",             "IsYandex")
	M("UserID",               "UserID")
	M("UserIDCreateDateTime", "toDateTime(UserID > 10000000000000000000 OR UserID % 10000000000 > 2000000000 OR UserID % 10000000000 < 1000000000 ? toUInt64(0) : UserID % 10000000000)")
	M("UserIDCreateDate",     "toDate(toDateTime(UserID > 10000000000000000000 OR UserID % 10000000000 > 2000000000 OR UserID % 10000000000 < 1000000000 ? toUInt64(0) : UserID % 10000000000))")
	M("UserIDAge",            "UserID > 10000000000000000000 OR UserID % 10000000000 < 1000000000 OR UserID % 10000000000 > toUInt64(StartTime) ? toInt64(-1) : intDiv(toInt64(StartTime) - UserID % 10000000000, 86400)")
	M("UserIDAgeInterval",    "UserID > 10000000000000000000 OR UserID % 10000000000 < 1000000000 OR UserID % 10000000000 > toUInt64(StartTime) ? toInt64(-1) : toInt64(roundToExp2(intDiv(toUInt64(StartTime) - UserID % 10000000000, 86400)))")
	M("TotalVisits",          "TotalVisits")
	M("TotalVisitsInterval",  "roundToExp2(TotalVisits)")
	M("Age",                  "Age")
	M("AgeInterval",          "roundAge(Age)")
	M("Sex",                  "Sex")
	M("Income",               "Income")
	M("AdvEngineID",          "AdvEngineID")
	M("DotNet",               "DotNet")
	M("DotNetMajor",          "DotNetMajor")
	M("Flash",                "concat(concat(toString(FlashMajor),'.'),toString(FlashMinor))")
	M("FlashExists",          "FlashMajor > 0")
	M("FlashMajor",           "FlashMajor")
	M("Silverlight",          "Silverlight")
	M("SilverlightMajor",     "SilverlightMajor")
	M("Hits",                 "Hits")
	M("HitsInterval",         "roundToExp2(Hits)")
	M("JavaEnable",           "JavaEnable")
	M("CookieEnable",         "CookieEnable")
	M("JavascriptEnable",     "JavascriptEnable")
	M("IsMobile",             "IsMobile")
	M("MobilePhoneID",        "MobilePhone")
	M("MobilePhoneModelHash", "halfMD5(MobilePhoneModel)")
	M("MobilePhoneModel",     "MobilePhoneModel")
	M("BrowserLanguage",      "unpackString(BrowserLanguage)")
	M("BrowserCountry",       "unpackString(BrowserCountry)")
	M("TopLevelDomain",       "TopLevelDomain")
	M("URLScheme",            "URLScheme")
	M("IPNetworkID",          "IPNetworkID")
	M("ClientTimeZone",       "ClientTimeZone")
	M("OSID",                 "OS")
	M("OSMostAncestor",       "rootOS(OS)")
	M("ClientIP",             "concat(concat(concat(concat(concat(concat(toString(intDiv(ClientIP, 16777216)),'.'),toString(intDiv(ClientIP, 65536) % 256)),'.'),toString(intDiv(ClientIP, 256) % 256)),'.'),toString(ClientIP % 256))")
	M("Resolution",           "concat(concat(concat(concat(toString(ResolutionWidth),'x'),toString(ResolutionHeight)),'x'),toString(ResolutionDepth))")
	M("ResolutionWidthHeight","concat(concat(toString(ResolutionWidth),'x'),toString(ResolutionHeight))")
	M("ResolutionWidth",      "ResolutionWidth")
	M("ResolutionHeight",     "ResolutionHeight")
	M("ResolutionWidthInterval","intDiv(ResolutionWidth, 100) * 100")
	M("ResolutionHeightInterval","intDiv(ResolutionHeight, 100) * 100")
	M("ResolutionColor",      "ResolutionDepth")
	M("WindowClientArea",     "concat(concat(toString(WindowClientWidth),'x'),toString(WindowClientHeight))")
	M("WindowClientAreaInterval","intDiv(WindowClientWidth, 100) * 6553600 + intDiv(WindowClientHeight, 100) * 100")
	M("WindowClientWidth",    "WindowClientWidth")
	M("WindowClientWidthInterval","intDiv(WindowClientWidth, 100) * 100")
	M("WindowClientHeight",   "WindowClientHeight")
	M("WindowClientHeightInterval","intDiv(WindowClientHeight, 100) * 100")
	M("SearchEngineID",       "SearchEngineID")
	M("SEMostAncestor", "")
	M("CodeVersion", "")
	M("UserAgent", "")
	M("UserAgentVersion", "")
	M("UserAgentMajor", "")
	M("UserAgentID", "")
	M("ClickGoodEvent", "")
	M("ClickPriorityID", "")
	M("ClickBannerID", "")
	M("ClickPhraseID", "")
	M("ClickPageID", "")
	M("ClickPlaceID", "")
	M("ClickTypeID", "")
	M("ClickResourceID", "")
	M("ClickDomainID", "")
	M("ClickCost", "")
	M("ClickURLHash", "")
	M("ClickOrderID", "")
	M("GoalReachesAny", "")
	M("GoalReachesDepth", "")
	M("GoalReachesURL", "")
	M("ConvertedAny", "")
	M("ConvertedDepth", "")
	M("ConvertedURL", "")
	M("GoalReaches", "")
	M("Converted", "")
	M("CounterID", "")
	M("VisitID", "")
	M("Interests", "")
	M("HasInterestPhoto", "")
	M("HasInterestMoviePremieres", "")
	M("HasInterestTourism", "")
	M("HasInterestFamilyAndChildren", "")
	M("HasInterestFinance", "")
	M("HasInterestB2B", "")
	M("HasInterestCars", "")
	M("HasInterestMobileAndInternetCommunications", "")
	M("HasInterestBuilding", "")
	M("HasInterestCulinary", "")
	M("OpenstatServiceNameHash", "")
	M("OpenstatCampaignIDHash", "")
	M("OpenstatAdIDHash", "")
	M("OpenstatSourceIDHash", "")
	M("UTMSourceHash", "")
	M("UTMMediumHash", "")
	M("UTMCampaignHash", "")
	M("UTMContentHash", "")
	M("UTMTermHash", "")
	M("FromHash", "")
	M("CLID", "")
#undef M
}

}
}
