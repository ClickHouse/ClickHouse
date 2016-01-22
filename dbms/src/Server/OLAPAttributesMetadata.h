#pragma once

#include <math.h>	// log2()
#include <openssl/md5.h>

#include <boost/algorithm/string.hpp>

#include <Poco/StringTokenizer.h>
#include <Poco/ByteOrder.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <common/DateLUT.h>

#include <DB/Dictionaries/Embedded/RegionsHierarchy.h>
#include <DB/Dictionaries/Embedded/TechDataHierarchy.h>


/// Код в основном взят из из OLAP-server. Здесь нужен только для парсинга значений атрибутов.

namespace DB
{
namespace OLAP
{

typedef Int64 BinaryData;

/** Информация о типе атрибута */
struct IAttributeMetadata
{
	/// получение значения из строки в запросе
	virtual BinaryData parse(const std::string & s) const = 0;
	virtual ~IAttributeMetadata() {}
};


/// атрибут - заглушка, всегда равен нулю, подходит для подстановки в агрегатную функцию count
struct DummyAttribute : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const { return 0; }
};


/// базовый класс для атрибутов, которые являются просто UInt8, UInt16, UInt32 или UInt64 (таких тоже много)
struct AttributeUIntBase : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return static_cast<BinaryData>(DB::parse<UInt64>(s));
	}
};


/// базовый класс для атрибутов, которые являются Int8, Int16, Int32 или Int64
struct AttributeIntBase : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<Int64>(s);
	}
};


/** Базовые классы для атрибутов, получаемых из времени (unix timestamp, 4 байта) */
struct AttributeDateTimeBase : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		struct tm tm;

		memset(&tm, 0, sizeof(tm));
		sscanf(s.c_str(), "%04d-%02d-%02d %02d:%02d:%02d",
			   &tm.tm_year, &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min, &tm.tm_sec);
		tm.tm_mon--;
		tm.tm_year -= 1900;
		tm.tm_isdst = -1;

		time_t res = mktime(&tm);
		return res >= 0 ? res : 0;
	}
};


struct AttributeDateBase : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		struct tm tm;

		memset(&tm, 0, sizeof(tm));
		sscanf(s.c_str(), "%04d-%02d-%02d",
			   &tm.tm_year, &tm.tm_mon, &tm.tm_mday);
		tm.tm_mon--;
		tm.tm_year -= 1900;
		tm.tm_isdst = -1;

		time_t res = mktime(&tm);
		return res >= 0 ? res : 0;
	}
};


struct AttributeTimeBase : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		struct tm tm;

		memset(&tm, 0, sizeof(tm));
		sscanf(s.c_str(), "%02d:%02d:%02d",
			   &tm.tm_hour, &tm.tm_min, &tm.tm_sec);

		time_t res = mktime(&tm);
		return res >= 0 ? res : 0;
	}
};


typedef AttributeUIntBase AttributeYearBase;
typedef AttributeUIntBase AttributeMonthBase;
typedef AttributeUIntBase AttributeDayOfWeekBase;
typedef AttributeUIntBase AttributeDayOfMonthBase;
typedef AttributeDateBase AttributeWeekBase;
typedef AttributeUIntBase AttributeHourBase;
typedef AttributeUIntBase AttributeMinuteBase;
typedef AttributeUIntBase AttributeSecondBase;

struct AttributeShortStringBase : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		std::string tmp = s;
		tmp.resize(sizeof(BinaryData));
		return *reinterpret_cast<const BinaryData *>(tmp.data());
	}
};


/** Атрибуты, относящиеся к времени начала визита */
typedef AttributeDateTimeBase VisitStartDateTime;
typedef AttributeDateTimeBase VisitStartDateTimeRoundedToMinute;
typedef AttributeDateTimeBase VisitStartDateTimeRoundedToHour;
typedef AttributeDateTimeBase VisitStartDateTime;
typedef AttributeDateBase VisitStartDate;
typedef AttributeDateBase VisitStartDateRoundedToMonth;
typedef AttributeWeekBase VisitStartWeek;
typedef AttributeTimeBase VisitStartTime;
typedef AttributeTimeBase VisitStartTimeRoundedToMinute;
typedef AttributeYearBase VisitStartYear;
typedef AttributeMonthBase VisitStartMonth;
typedef AttributeDayOfWeekBase VisitStartDayOfWeek;
typedef AttributeDayOfMonthBase VisitStartDayOfMonth;
typedef AttributeHourBase VisitStartHour;
typedef AttributeMinuteBase VisitStartMinute;
typedef AttributeSecondBase VisitStartSecond;

/** Атрибуты, относящиеся к времени начала первого визита */
typedef AttributeDateTimeBase FirstVisitDateTime;
typedef AttributeDateBase FirstVisitDate;
typedef AttributeWeekBase FirstVisitWeek;
typedef AttributeTimeBase FirstVisitTime;
typedef AttributeYearBase FirstVisitYear;
typedef AttributeMonthBase FirstVisitMonth;
typedef AttributeDayOfWeekBase FirstVisitDayOfWeek;
typedef AttributeDayOfMonthBase FirstVisitDayOfMonth;
typedef AttributeHourBase FirstVisitHour;
typedef AttributeMinuteBase FirstVisitMinute;
typedef AttributeSecondBase FirstVisitSecond;

/** Атрибуты, относящиеся к времени начала предпоследнего визита */
typedef AttributeDateBase PredLastVisitDate;
typedef AttributeWeekBase PredLastVisitWeek;
typedef AttributeYearBase PredLastVisitYear;
typedef AttributeMonthBase PredLastVisitMonth;
typedef AttributeDayOfWeekBase PredLastVisitDayOfWeek;
typedef AttributeDayOfMonthBase PredLastVisitDayOfMonth;

/** Атрибуты, относящиеся к времени на компьютере посетителя */
typedef AttributeDateTimeBase ClientDateTime;
typedef AttributeTimeBase ClientTime;
typedef AttributeHourBase ClientTimeHour;
typedef AttributeMinuteBase ClientTimeMinute;
typedef AttributeSecondBase ClientTimeSecond;

/** Базовый класс для атрибутов, для которых хранится хэш. */
struct AttributeHashBase : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		union
		{
			unsigned char char_data[16];
			Poco::UInt64 uint64_data;
		} buf;

		MD5_CTX ctx;
		MD5_Init(&ctx);
		MD5_Update(&ctx, reinterpret_cast<const unsigned char *>(s.data()), s.size());
		MD5_Final(buf.char_data, &ctx);

		return Poco::ByteOrder::flipBytes(buf.uint64_data);
	}
};


typedef AttributeHashBase EndURLHash;
typedef AttributeHashBase RefererHash;
typedef AttributeHashBase SearchPhraseHash;
typedef AttributeHashBase RefererDomainHash;
typedef AttributeHashBase StartURLHash;
typedef AttributeHashBase StartURLDomainHash;
typedef AttributeUIntBase RegionID;
typedef AttributeUIntBase RegionCity;
typedef AttributeUIntBase RegionArea;
typedef AttributeUIntBase RegionCountry;
typedef AttributeIntBase TraficSourceID;
typedef AttributeIntBase CorrectedTraficSourceID;
typedef AttributeUIntBase CorrectedSearchEngineID;

typedef AttributeUIntBase IsNewUser;
typedef AttributeUIntBase UserNewness;

struct UserNewnessInterval : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


typedef AttributeUIntBase UserReturnTime;

struct UserReturnTimeInterval : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


typedef AttributeUIntBase UserVisitsPeriod;

struct UserVisitsPeriodInterval : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


typedef AttributeUIntBase VisitTime;
typedef AttributeUIntBase VisitTimeInterval;
typedef AttributeUIntBase PageViews;
typedef AttributeUIntBase PageViewsInterval;
typedef AttributeUIntBase Bounce;
typedef AttributeUIntBase BouncePrecise;
typedef AttributeUIntBase IsYandex;
typedef AttributeUIntBase UserID;
typedef AttributeDateTimeBase UserIDCreateDateTime;
typedef AttributeDateBase UserIDCreateDate;
typedef AttributeIntBase UserIDAge;
typedef AttributeIntBase UserIDAgeInterval;
typedef AttributeUIntBase TotalVisits;
typedef AttributeUIntBase TotalVisitsInterval;
typedef AttributeUIntBase Age;
typedef AttributeUIntBase AgeInterval;
typedef AttributeUIntBase Sex;
typedef AttributeUIntBase Income;
typedef AttributeUIntBase AdvEngineID;

struct DotNet : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (DB::parse<UInt64>(tokenizer[0]) << 8)
		: ((DB::parse<UInt64>(tokenizer[0]) << 8) + DB::parse<UInt64>(tokenizer[1])));
	}
};


struct DotNetMajor : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


struct Flash : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (DB::parse<UInt64>(tokenizer[0]) << 8)
		: ((DB::parse<UInt64>(tokenizer[0]) << 8) + DB::parse<UInt64>(tokenizer[1])));
	}
};


struct FlashExists : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


struct FlashMajor : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


struct Silverlight : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1
		? (DB::parse<UInt64>(tokenizer[0]) << 56)
		: (tokenizer.count() == 2
		? ((DB::parse<UInt64>(tokenizer[0]) << 56)
		| (DB::parse<UInt64>(tokenizer[1]) << 48))
		: (tokenizer.count() == 3
		? ((DB::parse<UInt64>(tokenizer[0]) << 56)
		| (DB::parse<UInt64>(tokenizer[1]) << 48)
		| (DB::parse<UInt64>(tokenizer[2]) << 16))
		: ((DB::parse<UInt64>(tokenizer[0]) << 56)
		| (DB::parse<UInt64>(tokenizer[1]) << 48)
		| (DB::parse<UInt64>(tokenizer[2]) << 16)
		| DB::parse<UInt64>(tokenizer[3])))));
	}
};


typedef AttributeUIntBase SilverlightMajor;
typedef AttributeUIntBase Hits;
typedef AttributeUIntBase HitsInterval;
typedef AttributeUIntBase JavaEnable;
typedef AttributeUIntBase CookieEnable;
typedef AttributeUIntBase JavascriptEnable;
typedef AttributeUIntBase IsMobile;
typedef AttributeUIntBase MobilePhoneID;
typedef AttributeHashBase MobilePhoneModelHash;
typedef AttributeShortStringBase MobilePhoneModel;

struct BrowserLanguage : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		std::string tmp = s;
		tmp.resize(sizeof(UInt16));
		return *reinterpret_cast<const UInt16 *>(tmp.data());
	}
};


typedef BrowserLanguage BrowserCountry;
typedef AttributeShortStringBase TopLevelDomain;
typedef AttributeShortStringBase URLScheme;
typedef AttributeUIntBase IPNetworkID;
typedef AttributeIntBase ClientTimeZone;
typedef AttributeUIntBase OSID;
typedef AttributeUIntBase OSMostAncestor;

struct ClientIP : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (DB::parse<UInt64>(tokenizer[0]) << 24)
		: (tokenizer.count() == 2 ? (DB::parse<UInt64>(tokenizer[0]) << 24)
		+ (DB::parse<UInt64>(tokenizer[1]) << 16)
		: (tokenizer.count() == 3 ? (DB::parse<UInt64>(tokenizer[0]) << 24)
		+ (DB::parse<UInt64>(tokenizer[1]) << 16)
		+ (DB::parse<UInt64>(tokenizer[2]) << 8)
		: ((DB::parse<UInt64>(tokenizer[0]) << 24)
		+ (DB::parse<UInt64>(tokenizer[1]) << 16)
		+ (DB::parse<UInt64>(tokenizer[2]) << 8)
		+ DB::parse<UInt64>(tokenizer[3])))));
	}
};


struct Resolution : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, "x");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (DB::parse<UInt64>(tokenizer[0]) << 24)
		: (tokenizer.count() == 2 ? (DB::parse<UInt64>(tokenizer[0]) << 24)
		+ (DB::parse<UInt64>(tokenizer[1]) << 8)
		: ((DB::parse<UInt64>(tokenizer[0]) << 24)
		+ (DB::parse<UInt64>(tokenizer[1]) << 8)
		+ DB::parse<UInt64>(tokenizer[2]))));
	}
};


struct ResolutionWidthHeight : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, "x");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (DB::parse<UInt64>(tokenizer[0]) << 16)
		: ((DB::parse<UInt64>(tokenizer[0]) << 16)
		+ DB::parse<UInt64>(tokenizer[1])));
	}
};


struct ResolutionWidth : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


struct ResolutionHeight : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


typedef ResolutionWidth ResolutionWidthInterval;
typedef ResolutionHeight ResolutionHeightInterval;

struct ResolutionColor : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


struct WindowClientArea : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, "x");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (DB::parse<UInt64>(tokenizer[0]) << 16)
		: ((DB::parse<UInt64>(tokenizer[0]) << 16)
		+ DB::parse<UInt64>(tokenizer[1])));
	}
};


typedef WindowClientArea WindowClientAreaInterval;
typedef AttributeUIntBase WindowClientWidth;
typedef WindowClientWidth WindowClientWidthInterval;
typedef AttributeUIntBase WindowClientHeight;
typedef WindowClientHeight WindowClientHeightInterval;
typedef AttributeUIntBase SearchEngineID;
typedef AttributeUIntBase SearchEngineMostAncestor;
typedef AttributeUIntBase CodeVersion;

/// формат строки вида "10 7.5b", где первое число - UserAgentID, дальше - версия.
struct UserAgent : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, " .");
		return tokenizer.count() == 0 ? 0
			: (tokenizer.count() == 1 ? (DB::parse<UInt64>(tokenizer[0]) << 24)
			: (tokenizer.count() == 2 ? (DB::parse<UInt64>(tokenizer[0]) << 24)
				+ (DB::parse<UInt64>(tokenizer[1]) << 16)
			: ((DB::parse<UInt64>(tokenizer[0]) << 24)
				+ (DB::parse<UInt64>(tokenizer[1]) << 16)
				+ (static_cast<UInt32>(tokenizer[2][1]) << 8)
				+ (tokenizer[2][0]))));
	}
};


struct UserAgentVersion : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
			: (tokenizer.count() == 1 ? (DB::parse<UInt64>(tokenizer[0]) << 16)
			: ((DB::parse<UInt64>(tokenizer[0]) << 16)
				+ (static_cast<UInt32>(tokenizer[1][1]) << 8)
				+ tokenizer[1][0]));
	}
};


struct UserAgentMajor : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, " ");
		return tokenizer.count() == 0 ? 0
			: (tokenizer.count() == 1 ? (DB::parse<UInt64>(tokenizer[0]) << 8)
			: ((DB::parse<UInt64>(tokenizer[0]) << 8)
				+ DB::parse<UInt64>(tokenizer[1])));
	}
};


struct UserAgentID : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


typedef AttributeIntBase ClickGoodEvent;
typedef AttributeIntBase ClickPriorityID;
typedef AttributeIntBase ClickBannerID;
typedef AttributeIntBase ClickPageID;
typedef AttributeIntBase ClickPlaceID;
typedef AttributeIntBase ClickTypeID;
typedef AttributeIntBase ClickResourceID;
typedef AttributeUIntBase ClickDomainID;
typedef AttributeUIntBase ClickCost;
typedef AttributeHashBase ClickURLHash;
typedef AttributeUIntBase ClickOrderID;
typedef AttributeIntBase GoalReachesAny;
typedef AttributeIntBase GoalReachesDepth;
typedef AttributeIntBase GoalReachesURL;
typedef AttributeIntBase ConvertedAny;
typedef AttributeIntBase ConvertedDepth;
typedef AttributeIntBase ConvertedURL;
typedef AttributeIntBase GoalReaches;
typedef AttributeIntBase Converted;
typedef AttributeUIntBase CounterID;
typedef AttributeUIntBase VisitID;

struct Interests : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		if(s.empty())
			return 0;
		using namespace boost::algorithm;
		BinaryData value = 0;

		///коряво
		for(split_iterator<std::string::const_iterator> i
			= make_split_iterator(s, token_finder(is_any_of(","),
												  token_compress_on)); i != split_iterator<std::string::const_iterator>(); ++i)
		{
			UInt16 interest = DB::parse<UInt64>(boost::copy_range<std::string>(*i));
			value |= (interest == 0x2000 ? 0x2000 :
			(interest == 0x1000 ? 0x1000 :
			(interest == 0x800 ? 0x800 :
			(interest == 0x400 ? 0x400 :
			(interest == 0x200 ? 0x200 :
			(interest == 0x100 ? 0x100 :
			(interest == 0x80 ? 0x80 :
			(interest == 0x40 ? 0x40 :
			(interest == 0x20 ? 0x20 :
			(interest == 0x10 ? 0x10 :
			(interest == 8 ? 8 :
			(interest == 4 ? 4 :
			(interest == 2 ? 2 :
			(interest == 1 ? 1 : 0))))))))))))));
		}

		return value;
	}
};

typedef AttributeUIntBase HasInterestPhoto;
typedef AttributeUIntBase HasInterestMoviePremieres;
typedef AttributeUIntBase HasInterestTourism;
typedef AttributeUIntBase HasInterestFamilyAndChildren;
typedef AttributeUIntBase HasInterestFinance;
typedef AttributeUIntBase HasInterestB2B;
typedef AttributeUIntBase HasInterestCars;
typedef AttributeUIntBase HasInterestMobileAndInternetCommunications;
typedef AttributeUIntBase HasInterestBuilding;
typedef AttributeUIntBase HasInterestCulinary;
typedef AttributeUIntBase HasInterestSoftware;
typedef AttributeUIntBase HasInterestEstate;
typedef AttributeUIntBase HasInterestHealthyLifestyle;
typedef AttributeUIntBase HasInterestLiterature;
typedef AttributeHashBase OpenstatServiceNameHash;
typedef AttributeHashBase OpenstatCampaignIDHash;
typedef AttributeHashBase OpenstatAdIDHash;
typedef AttributeHashBase OpenstatSourceIDHash;
typedef AttributeHashBase UTMSourceHash;
typedef AttributeHashBase UTMMediumHash;
typedef AttributeHashBase UTMCampaignHash;
typedef AttributeHashBase UTMContentHash;
typedef AttributeHashBase UTMTermHash;
typedef AttributeHashBase FromHash;
typedef AttributeUIntBase CLID;
typedef AttributeUIntBase SocialSourceNetworkID;


/** Информация о типах атрибутов */
typedef std::map<std::string, Poco::SharedPtr<IAttributeMetadata> > AttributeMetadatas;

inline AttributeMetadatas GetOLAPAttributeMetadata()
{
	return
	{
		{"DummyAttribute", 						new DummyAttribute},
		{"VisitStartDateTime",					new VisitStartDateTime},
		{"VisitStartDateTimeRoundedToMinute",	new VisitStartDateTimeRoundedToMinute},
		{"VisitStartDateTimeRoundedToHour",		new VisitStartDateTimeRoundedToHour},
		{"VisitStartDate",						new VisitStartDate},
		{"VisitStartDateRoundedToMonth",		new VisitStartDateRoundedToMonth},
		{"VisitStartTime",						new VisitStartTime},
		{"VisitStartTimeRoundedToMinute",		new VisitStartTimeRoundedToMinute},
		{"VisitStartYear",			new VisitStartYear},
		{"VisitStartMonth",			new VisitStartMonth},
		{"VisitStartDayOfWeek",		new VisitStartDayOfWeek},
		{"VisitStartDayOfMonth",	new VisitStartDayOfMonth},
		{"VisitStartHour",			new VisitStartHour},
		{"VisitStartMinute",		new VisitStartMinute},
		{"VisitStartSecond",		new VisitStartSecond},
		{"VisitStartWeek",			new VisitStartWeek},
		{"FirstVisitDateTime",		new FirstVisitDateTime},
		{"FirstVisitDate",			new FirstVisitDate},
		{"FirstVisitTime",			new FirstVisitTime},
		{"FirstVisitYear",			new FirstVisitYear},
		{"FirstVisitMonth",			new FirstVisitMonth},
		{"FirstVisitDayOfWeek",		new FirstVisitDayOfWeek},
		{"FirstVisitDayOfMonth",	new FirstVisitDayOfMonth},
		{"FirstVisitHour",			new FirstVisitHour},
		{"FirstVisitMinute",		new FirstVisitMinute},
		{"FirstVisitSecond",		new FirstVisitSecond},
		{"FirstVisitWeek",			new FirstVisitWeek},
		{"PredLastVisitDate",		new PredLastVisitDate},
		{"PredLastVisitYear",		new PredLastVisitYear},
		{"PredLastVisitMonth",		new PredLastVisitMonth},
		{"PredLastVisitDayOfWeek",	new PredLastVisitDayOfWeek},
		{"PredLastVisitDayOfMonth",	new PredLastVisitDayOfMonth},
		{"PredLastVisitWeek",		new PredLastVisitWeek},
		{"RegionID", 				new RegionID},
		{"RegionCity", 				new RegionCity},
		{"RegionArea", 				new RegionArea},
		{"RegionCountry",			new RegionCountry},
		{"TraficSourceID", 			new TraficSourceID},
		{"UserNewness", 			new UserNewness},
		{"UserNewnessInterval", 	new UserNewnessInterval},
		{"UserReturnTime", 			new UserReturnTime},
		{"UserReturnTimeInterval", 	new UserReturnTimeInterval},
		{"UserVisitsPeriod",		new UserVisitsPeriod},
		{"UserVisitsPeriodInterval",new UserVisitsPeriodInterval},
		{"VisitTime", 				new VisitTime},
		{"VisitTimeInterval",		new VisitTimeInterval},
		{"PageViews", 				new PageViews},
		{"PageViewsInterval",		new PageViewsInterval},
		{"UserID", 					new UserID},
		{"TotalVisits", 			new TotalVisits},
		{"TotalVisitsInterval",		new TotalVisitsInterval},
		{"Age", 					new Age},
		{"AgeInterval",				new AgeInterval},
		{"Sex", 					new Sex},
		{"Income", 					new Income},
		{"AdvEngineID", 			new AdvEngineID},
		{"DotNet", 					new DotNet},
		{"DotNetMajor",				new DotNetMajor},
		{"EndURLHash",				new EndURLHash},
		{"Flash", 					new Flash},
		{"FlashMajor",				new FlashMajor},
		{"FlashExists",				new FlashExists},
		{"Hits", 					new Hits},
		{"HitsInterval",			new HitsInterval},
		{"JavaEnable",				new JavaEnable},
		{"OSID", 					new OSID},
		{"ClientIP",				new ClientIP},
		{"RefererHash",				new RefererHash},
		{"RefererDomainHash",		new RefererDomainHash},
		{"Resolution",				new Resolution},
		{"ResolutionWidthHeight",	new ResolutionWidthHeight},
		{"ResolutionWidth",			new ResolutionWidth},
		{"ResolutionHeight",		new ResolutionHeight},
		{"ResolutionWidthInterval",	new ResolutionWidthInterval},
		{"ResolutionHeightInterval",new ResolutionHeightInterval},
		{"ResolutionColor",			new ResolutionColor},
		{"CookieEnable",			new CookieEnable},
		{"JavascriptEnable",		new JavascriptEnable},
		{"IsMobile",				new IsMobile},
		{"MobilePhoneID",			new MobilePhoneID},
		{"MobilePhoneModel",		new MobilePhoneModel},
		{"MobilePhoneModelHash",	new MobilePhoneModelHash},
		{"IPNetworkID",				new IPNetworkID},
		{"WindowClientArea",		new WindowClientArea},
		{"WindowClientWidth",		new WindowClientWidth},
		{"WindowClientHeight",		new WindowClientHeight},
		{"WindowClientAreaInterval",new WindowClientAreaInterval},
		{"WindowClientWidthInterval",new WindowClientWidthInterval},
		{"WindowClientHeightInterval",new WindowClientHeightInterval},
		{"ClientTimeZone",			new ClientTimeZone},
		{"ClientDateTime",			new ClientDateTime},
		{"ClientTime",				new ClientTime},
		{"ClientTimeHour",			new ClientTimeHour},
		{"ClientTimeMinute",		new ClientTimeMinute},
		{"ClientTimeSecond",		new ClientTimeSecond},
		{"Silverlight",				new Silverlight},
		{"SilverlightMajor",		new SilverlightMajor},
		{"SearchEngineID",			new SearchEngineID},
		{"SearchPhraseHash",		new SearchPhraseHash},
		{"StartURLHash",			new StartURLHash},
		{"StartURLDomainHash",		new StartURLDomainHash},
		{"UserAgent",				new UserAgent},
		{"UserAgentVersion",		new UserAgentVersion},
		{"UserAgentMajor",			new UserAgentMajor},
		{"UserAgentID",				new UserAgentID},
		{"ClickGoodEvent",			new ClickGoodEvent},
		{"ClickPriorityID",			new ClickPriorityID},
		{"ClickBannerID",			new ClickBannerID},
		{"ClickPageID",				new ClickPageID},
		{"ClickPlaceID",			new ClickPlaceID},
		{"ClickTypeID",				new ClickTypeID},
		{"ClickResourceID",			new ClickResourceID},
		{"ClickDomainID",			new ClickDomainID},
		{"ClickCost",				new ClickCost},
		{"ClickURLHash",			new ClickURLHash},
		{"ClickOrderID",			new ClickOrderID},
		{"GoalReaches",				new GoalReaches},
		{"GoalReachesAny",			new GoalReachesAny},
		{"GoalReachesDepth",		new GoalReachesDepth},
		{"GoalReachesURL",			new GoalReachesURL},
		{"Converted",				new Converted},
		{"ConvertedAny",			new ConvertedAny},
		{"ConvertedDepth",			new ConvertedDepth},
		{"ConvertedURL",			new ConvertedURL},
		{"Bounce",					new Bounce},
		{"BouncePrecise",			new BouncePrecise},
		{"IsNewUser",				new IsNewUser},
		{"CodeVersion",				new CodeVersion},
		{"CounterID",				new CounterID},
		{"VisitID",					new VisitID},
		{"IsYandex",				new IsYandex},
		{"TopLevelDomain",			new TopLevelDomain},
		{"URLScheme",				new URLScheme},
		{"UserIDCreateDateTime",	new UserIDCreateDateTime},
		{"UserIDCreateDate",		new UserIDCreateDate},
		{"UserIDAge",				new UserIDAge},
		{"UserIDAgeInterval",		new UserIDAgeInterval},
		{"OSMostAncestor",			new OSMostAncestor},
		{"SearchEngineMostAncestor",new SearchEngineMostAncestor},
		{"BrowserLanguage",			new BrowserLanguage},
		{"BrowserCountry",			new BrowserCountry},
		{"Interests",				new Interests},
		{"HasInterestPhoto",		new HasInterestPhoto},
		{"HasInterestMoviePremieres",	new HasInterestMoviePremieres},
		{"HasInterestMobileAndInternetCommunications",	new HasInterestMobileAndInternetCommunications},
		{"HasInterestFinance",		new HasInterestFinance},
		{"HasInterestFamilyAndChildren",	new HasInterestFamilyAndChildren},
		{"HasInterestCars",			new HasInterestCars},
		{"HasInterestB2B",			new HasInterestB2B},
		{"HasInterestTourism",		new HasInterestTourism},
		{"HasInterestBuilding",		new HasInterestBuilding},
		{"HasInterestCulinary",		new HasInterestCulinary},
		{"HasInterestSoftware",		new HasInterestSoftware},
		{"HasInterestEstate",		new HasInterestEstate},
		{"HasInterestHealthyLifestyle",	new HasInterestHealthyLifestyle},
		{"HasInterestLiterature",	new HasInterestLiterature},

		{"OpenstatServiceNameHash",new OpenstatServiceNameHash},
		{"OpenstatCampaignIDHash",	new OpenstatCampaignIDHash},
		{"OpenstatAdIDHash",		new OpenstatAdIDHash},
		{"OpenstatSourceIDHash",	new OpenstatSourceIDHash},

		{"UTMSourceHash",			new UTMSourceHash},
		{"UTMMediumHash",			new UTMMediumHash},
		{"UTMCampaignHash",			new UTMCampaignHash},
		{"UTMContentHash",			new UTMContentHash},
		{"UTMTermHash",				new UTMTermHash},

		{"FromHash",				new FromHash},
		{"CLID",					new CLID},

		{"SocialSourceNetworkID",	new SocialSourceNetworkID},

		{"CorrectedTraficSourceID",	new CorrectedTraficSourceID},
		{"CorrectedSearchEngineID", new CorrectedSearchEngineID},
	};
}

}
}
