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

using BinaryData = Int64;

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


using AttributeYearBase = AttributeUIntBase;
using AttributeMonthBase = AttributeUIntBase;
using AttributeDayOfWeekBase = AttributeUIntBase;
using AttributeDayOfMonthBase = AttributeUIntBase;
using AttributeWeekBase = AttributeDateBase;
using AttributeHourBase = AttributeUIntBase;
using AttributeMinuteBase = AttributeUIntBase;
using AttributeSecondBase = AttributeUIntBase;

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
using VisitStartDateTime = AttributeDateTimeBase;
using VisitStartDateTimeRoundedToMinute = AttributeDateTimeBase;
using VisitStartDateTimeRoundedToHour = AttributeDateTimeBase;
using VisitStartDateTime = AttributeDateTimeBase;
using VisitStartDate = AttributeDateBase;
using VisitStartDateRoundedToMonth = AttributeDateBase;
using VisitStartWeek = AttributeWeekBase;
using VisitStartTime = AttributeTimeBase;
using VisitStartTimeRoundedToMinute = AttributeTimeBase;
using VisitStartYear = AttributeYearBase;
using VisitStartMonth = AttributeMonthBase;
using VisitStartDayOfWeek = AttributeDayOfWeekBase;
using VisitStartDayOfMonth = AttributeDayOfMonthBase;
using VisitStartHour = AttributeHourBase;
using VisitStartMinute = AttributeMinuteBase;
using VisitStartSecond = AttributeSecondBase;

/** Атрибуты, относящиеся к времени начала первого визита */
using FirstVisitDateTime = AttributeDateTimeBase;
using FirstVisitDate = AttributeDateBase;
using FirstVisitWeek = AttributeWeekBase;
using FirstVisitTime = AttributeTimeBase;
using FirstVisitYear = AttributeYearBase;
using FirstVisitMonth = AttributeMonthBase;
using FirstVisitDayOfWeek = AttributeDayOfWeekBase;
using FirstVisitDayOfMonth = AttributeDayOfMonthBase;
using FirstVisitHour = AttributeHourBase;
using FirstVisitMinute = AttributeMinuteBase;
using FirstVisitSecond = AttributeSecondBase;

/** Атрибуты, относящиеся к времени начала предпоследнего визита */
using PredLastVisitDate = AttributeDateBase;
using PredLastVisitWeek = AttributeWeekBase;
using PredLastVisitYear = AttributeYearBase;
using PredLastVisitMonth = AttributeMonthBase;
using PredLastVisitDayOfWeek = AttributeDayOfWeekBase;
using PredLastVisitDayOfMonth = AttributeDayOfMonthBase;

/** Атрибуты, относящиеся к времени на компьютере посетителя */
using ClientDateTime = AttributeDateTimeBase;
using ClientTime = AttributeTimeBase;
using ClientTimeHour = AttributeHourBase;
using ClientTimeMinute = AttributeMinuteBase;
using ClientTimeSecond = AttributeSecondBase;

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


using EndURLHash = AttributeHashBase;
using RefererHash = AttributeHashBase;
using SearchPhraseHash = AttributeHashBase;
using RefererDomainHash = AttributeHashBase;
using StartURLHash = AttributeHashBase;
using StartURLDomainHash = AttributeHashBase;
using RegionID = AttributeUIntBase;
using RegionCity = AttributeUIntBase;
using RegionArea = AttributeUIntBase;
using RegionCountry = AttributeUIntBase;
using TraficSourceID = AttributeIntBase;
using CorrectedTraficSourceID = AttributeIntBase;
using CorrectedSearchEngineID = AttributeUIntBase;

using IsNewUser = AttributeUIntBase;
using UserNewness = AttributeUIntBase;

struct UserNewnessInterval : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


using UserReturnTime = AttributeUIntBase;

struct UserReturnTimeInterval : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


using UserVisitsPeriod = AttributeUIntBase;

struct UserVisitsPeriodInterval : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return DB::parse<UInt64>(s);
	}
};


using VisitTime = AttributeUIntBase;
using VisitTimeInterval = AttributeUIntBase;
using PageViews = AttributeUIntBase;
using PageViewsInterval = AttributeUIntBase;
using Bounce = AttributeUIntBase;
using BouncePrecise = AttributeUIntBase;
using IsYandex = AttributeUIntBase;
using UserID = AttributeUIntBase;
using UserIDCreateDateTime = AttributeDateTimeBase;
using UserIDCreateDate = AttributeDateBase;
using UserIDAge = AttributeIntBase;
using UserIDAgeInterval = AttributeIntBase;
using TotalVisits = AttributeUIntBase;
using TotalVisitsInterval = AttributeUIntBase;
using Age = AttributeUIntBase;
using AgeInterval = AttributeUIntBase;
using Sex = AttributeUIntBase;
using Income = AttributeUIntBase;
using AdvEngineID = AttributeUIntBase;

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


using SilverlightMajor = AttributeUIntBase;
using Hits = AttributeUIntBase;
using HitsInterval = AttributeUIntBase;
using JavaEnable = AttributeUIntBase;
using CookieEnable = AttributeUIntBase;
using JavascriptEnable = AttributeUIntBase;
using IsMobile = AttributeUIntBase;
using MobilePhoneID = AttributeUIntBase;
using MobilePhoneModelHash = AttributeHashBase;
using MobilePhoneModel = AttributeShortStringBase;

struct BrowserLanguage : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		std::string tmp = s;
		tmp.resize(sizeof(UInt16));
		return *reinterpret_cast<const UInt16 *>(tmp.data());
	}
};


using BrowserCountry = BrowserLanguage;
using TopLevelDomain = AttributeShortStringBase;
using URLScheme = AttributeShortStringBase;
using IPNetworkID = AttributeUIntBase;
using ClientTimeZone = AttributeIntBase;
using OSID = AttributeUIntBase;
using OSMostAncestor = AttributeUIntBase;

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


using ResolutionWidthInterval = ResolutionWidth;
using ResolutionHeightInterval = ResolutionHeight;

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


using WindowClientAreaInterval = WindowClientArea;
using WindowClientWidth = AttributeUIntBase;
using WindowClientWidthInterval = WindowClientWidth;
using WindowClientHeight = AttributeUIntBase;
using WindowClientHeightInterval = WindowClientHeight;
using SearchEngineID = AttributeUIntBase;
using SearchEngineMostAncestor = AttributeUIntBase;
using CodeVersion = AttributeUIntBase;

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


using ClickGoodEvent = AttributeIntBase;
using ClickPriorityID = AttributeIntBase;
using ClickBannerID = AttributeIntBase;
using ClickPageID = AttributeIntBase;
using ClickPlaceID = AttributeIntBase;
using ClickTypeID = AttributeIntBase;
using ClickResourceID = AttributeIntBase;
using ClickDomainID = AttributeUIntBase;
using ClickCost = AttributeUIntBase;
using ClickURLHash = AttributeHashBase;
using ClickOrderID = AttributeUIntBase;
using GoalReachesAny = AttributeIntBase;
using GoalReachesDepth = AttributeIntBase;
using GoalReachesURL = AttributeIntBase;
using ConvertedAny = AttributeIntBase;
using ConvertedDepth = AttributeIntBase;
using ConvertedURL = AttributeIntBase;
using GoalReaches = AttributeIntBase;
using Converted = AttributeIntBase;
using CounterID = AttributeUIntBase;
using VisitID = AttributeUIntBase;

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

using HasInterestPhoto = AttributeUIntBase;
using HasInterestMoviePremieres = AttributeUIntBase;
using HasInterestTourism = AttributeUIntBase;
using HasInterestFamilyAndChildren = AttributeUIntBase;
using HasInterestFinance = AttributeUIntBase;
using HasInterestB2B = AttributeUIntBase;
using HasInterestCars = AttributeUIntBase;
using HasInterestMobileAndInternetCommunications = AttributeUIntBase;
using HasInterestBuilding = AttributeUIntBase;
using HasInterestCulinary = AttributeUIntBase;
using HasInterestSoftware = AttributeUIntBase;
using HasInterestEstate = AttributeUIntBase;
using HasInterestHealthyLifestyle = AttributeUIntBase;
using HasInterestLiterature = AttributeUIntBase;
using OpenstatServiceNameHash = AttributeHashBase;
using OpenstatCampaignIDHash = AttributeHashBase;
using OpenstatAdIDHash = AttributeHashBase;
using OpenstatSourceIDHash = AttributeHashBase;
using UTMSourceHash = AttributeHashBase;
using UTMMediumHash = AttributeHashBase;
using UTMCampaignHash = AttributeHashBase;
using UTMContentHash = AttributeHashBase;
using UTMTermHash = AttributeHashBase;
using FromHash = AttributeHashBase;
using CLID = AttributeUIntBase;
using SocialSourceNetworkID = AttributeUIntBase;


/** Информация о типах атрибутов */
using AttributeMetadatas = std::map<std::string, Poco::SharedPtr<IAttributeMetadata> >;

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
