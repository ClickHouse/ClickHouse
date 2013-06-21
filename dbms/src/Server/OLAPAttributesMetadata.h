#pragma once

#include <math.h>	// log2()

#include <boost/algorithm/string.hpp>

#include <Poco/NumberParser.h>
#include <Poco/StringTokenizer.h>

#include <DB/IO/WriteHelpers.h>

#include <Yandex/DateLUT.h>
#include <strconvert/hash64.h>
#include <statdaemons/RegionsHierarchy.h>
#include <statdaemons/TechDataHierarchy.h>
#include <statdaemons/Interests.h>


/// Код в основном взят из из OLAP-server. Здесь нужен только для парсинга значений атрибутов.

namespace DB
{
namespace OLAP
{

typedef Poco::Int64 BinaryData;

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


/// базовый класс для атрибутов, для получения значения которых надо прочитать только один файл (таких большинство)
struct AttributeInOneFileBase : public IAttributeMetadata
{
};


/// базовый класс для атрибутов, которые являются просто UInt8, UInt16, UInt32 или UInt64 (таких тоже много)
template <typename T>
struct AttributeUIntBase : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		return static_cast<BinaryData>(Poco::NumberParser::parseUnsigned64(s));
	}
};


/// базовый класс для атрибутов, которые являются Int8, Int16, Int32 или Int64
template <typename T>
struct AttributeIntBase : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parse64(s);
	}
};


/** базовый класс для атрибутов, которые являются просто UInt8, UInt16, UInt32,
 * при этом значение округляется до степени двух,
 * при этом для хранения значения хватает точности double
 */
template <typename T>
struct AttributeUIntLogIntervalBase : public AttributeUIntBase<T>
{
};


/** базовый класс для атрибутов, которые являются целыми числами, но
 * усреднение которых должно производиться с точностью до тысячных долей
 */
template <typename T>
struct AttributeFixedPointBase : public AttributeUIntBase<T>
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned64(s) * 1000;
	}
};


/** Базовые классы для атрибутов, получаемых из времени (unix timestamp, 4 байта) */
struct AttributeDateTimeBase : public AttributeIntBase<Poco::Int32>
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
		
		return mktime(&tm);
	}
};


struct AttributeDateBase : public AttributeIntBase<Poco::Int32>
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
		
		return mktime(&tm);
	}
};


struct AttributeTimeBase : public AttributeIntBase<Poco::Int32>
{
	BinaryData parse(const std::string & s) const
	{
		struct tm tm;
		
		memset(&tm, 0, sizeof(tm));
		sscanf(s.c_str(), "%02d:%02d:%02d",
			   &tm.tm_hour, &tm.tm_min, &tm.tm_sec);
		
		return mktime(&tm);
	}
};


struct AttributeYearBase : public AttributeUIntBase<UInt32>
{
};


struct AttributeMonthBase : public AttributeUIntBase<UInt32>
{
};


struct AttributeDayOfWeekBase : public AttributeUIntBase<UInt32>
{
};


struct AttributeDayOfMonthBase : public AttributeUIntBase<UInt32>
{
};


struct AttributeWeekBase : public AttributeDateBase
{
};


struct AttributeHourBase : public AttributeUIntBase<UInt32>
{
};


struct AttributeMinuteBase : public AttributeUIntBase<UInt32>
{
};


struct AttributeSecondBase : public AttributeUIntBase<UInt32>
{
};


struct AttributeShortStringBase : public AttributeUIntBase<UInt64>
{
	BinaryData parse(const std::string & s) const
	{
		std::string tmp = s;
		tmp.resize(sizeof(BinaryData));
		return *reinterpret_cast<const BinaryData *>(tmp.data());
	}
};


/** Атрибуты, относящиеся к времени начала визита */
struct VisitStartDateTime : public AttributeDateTimeBase
{
};

struct VisitStartDate : public AttributeDateBase
{
};

struct VisitStartWeek : public AttributeWeekBase
{
};

struct VisitStartTime : public AttributeTimeBase
{
};

struct VisitStartYear : public AttributeYearBase
{
};

struct VisitStartMonth : public AttributeMonthBase
{
};

struct VisitStartDayOfWeek : public AttributeDayOfWeekBase
{
};

struct VisitStartDayOfMonth : public AttributeDayOfMonthBase
{
};

struct VisitStartHour : public AttributeHourBase
{
};

struct VisitStartMinute : public AttributeMinuteBase
{
};

struct VisitStartSecond : public AttributeSecondBase
{
};


/** Атрибуты, относящиеся к времени начала первого визита */
struct FirstVisitDateTime : public AttributeDateTimeBase
{
};

struct FirstVisitDate : public AttributeDateBase
{
};

struct FirstVisitWeek : public AttributeWeekBase
{
};

struct FirstVisitTime : public AttributeTimeBase
{
};

struct FirstVisitYear : public AttributeYearBase
{
};

struct FirstVisitMonth : public AttributeMonthBase
{
};

struct FirstVisitDayOfWeek : public AttributeDayOfWeekBase
{
};

struct FirstVisitDayOfMonth : public AttributeDayOfMonthBase
{
};

struct FirstVisitHour : public AttributeHourBase
{
};

struct FirstVisitMinute : public AttributeMinuteBase
{
};

struct FirstVisitSecond : public AttributeSecondBase
{
};


/** Атрибуты, относящиеся к времени начала предпоследнего визита */
struct PredLastVisitDate : public AttributeDateBase
{
};

struct PredLastVisitWeek : public AttributeWeekBase
{
};

struct PredLastVisitYear : public AttributeYearBase
{
};

struct PredLastVisitMonth : public AttributeMonthBase
{
};

struct PredLastVisitDayOfWeek : public AttributeDayOfWeekBase
{
};

struct PredLastVisitDayOfMonth : public AttributeDayOfMonthBase
{
};


/** Атрибуты, относящиеся к времени на компьютере посетителя */
struct ClientDateTime : public AttributeDateTimeBase
{
};

struct ClientTime : public AttributeTimeBase
{
};

struct ClientTimeHour : public AttributeHourBase
{
};

struct ClientTimeMinute : public AttributeMinuteBase
{
};

struct ClientTimeSecond : public AttributeSecondBase
{
};


/** Базовый класс для атрибутов, для которых хранится хэш. */
struct AttributeHashBase : public AttributeUIntBase<UInt64>
{
	BinaryData parse(const std::string & s) const
	{
		return strconvert::hash64(s);
	}
};


struct EndURLHash : public AttributeHashBase
{
};

struct RefererHash : public AttributeHashBase
{
};

struct SearchPhraseHash : public AttributeHashBase
{
};

struct RefererDomainHash : public AttributeHashBase
{
};

struct StartURLHash : public AttributeHashBase
{
};

struct StartURLDomainHash : public AttributeHashBase
{
};


struct RegionID : public AttributeUIntBase<UInt32>
{
};


struct RegionCity : public AttributeUIntBase<UInt32>
{
};


struct RegionArea : public AttributeUIntBase<UInt32>
{
};


struct RegionCountry : public AttributeUIntBase<UInt32>
{
};


struct TraficSourceID : public AttributeIntBase<Poco::Int8>
{
};


struct IsNewUser : public AttributeFixedPointBase<UInt8>
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s) * 1000;
	}
};


struct UserNewness : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s) * 1000;
	}
};


struct UserNewnessInterval : public UserNewness
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct UserReturnTime : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s) * 1000;
	}
};


struct UserReturnTimeInterval : public UserReturnTime
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct UserVisitsPeriod : public IAttributeMetadata
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s) * 1000;
	}
};


struct UserVisitsPeriodInterval : public UserVisitsPeriod
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct VisitTime : public AttributeUIntBase<UInt32>
{
};


struct VisitTimeInterval : public AttributeUIntBase<UInt32>
{
};


struct PageViews : public AttributeFixedPointBase<UInt32>
{
};


struct PageViewsInterval : public AttributeUIntLogIntervalBase<UInt32>
{
};


struct Bounce : public AttributeFixedPointBase<UInt32>
{
};


struct BouncePrecise : public AttributeFixedPointBase<UInt8>
{
};


struct IsYandex : public AttributeFixedPointBase<UInt8>
{
};


struct UserID : public AttributeUIntBase<UInt64>
{
};


struct UserIDCreateDateTime : public AttributeDateTimeBase
{
};


struct UserIDCreateDate : public AttributeDateBase
{
};


struct UserIDAge : public AttributeDateTimeBase
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s) * 1000;
	}
};


struct UserIDAgeInterval : public UserIDAge
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct TotalVisits : public AttributeFixedPointBase<UInt32>
{
};


struct TotalVisitsInterval : public AttributeUIntLogIntervalBase<UInt32>
{
};


struct Age : public AttributeFixedPointBase<UInt8>
{
};


struct AgeInterval : public AttributeUIntBase<UInt8>
{
};


struct Sex : public AttributeFixedPointBase<UInt8>
{
};


struct Income : public AttributeFixedPointBase<UInt8>
{
};


struct AdvEngineID : public AttributeUIntBase<UInt8>
{
};


struct DotNet : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8)
		: ((Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8) + Poco::NumberParser::parseUnsigned(tokenizer[1])));
	}
};


struct DotNetMajor : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct Flash : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8)
		: ((Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8) + Poco::NumberParser::parseUnsigned(tokenizer[1])));
	}
};


struct FlashExists : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct FlashMajor : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct Silverlight : public AttributeUIntBase<UInt64>
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1
		? (Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 56)
		: (tokenizer.count() == 2
		? ((Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 56)
		| (Poco::NumberParser::parseUnsigned64(tokenizer[1]) << 48))
		: (tokenizer.count() == 3
		? ((Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 56)
		| (Poco::NumberParser::parseUnsigned64(tokenizer[1]) << 48)
		| (Poco::NumberParser::parseUnsigned64(tokenizer[2]) << 16))
		: ((Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 56)
		| (Poco::NumberParser::parseUnsigned64(tokenizer[1]) << 48)
		| (Poco::NumberParser::parseUnsigned64(tokenizer[2]) << 16)
		| Poco::NumberParser::parseUnsigned64(tokenizer[3])))));
	}
};


struct SilverlightMajor : public AttributeUIntBase<UInt64>
{
};


struct Hits : public AttributeFixedPointBase<UInt32>
{
};


struct HitsInterval : public AttributeUIntLogIntervalBase<UInt32>
{
};


struct JavaEnable : public AttributeFixedPointBase<UInt8>
{
};


struct CookieEnable : public AttributeFixedPointBase<UInt8>
{
};


struct JavascriptEnable : public AttributeFixedPointBase<UInt8>
{
};


struct IsMobile : public AttributeFixedPointBase<UInt8>
{
};


struct MobilePhoneID : public AttributeUIntBase<UInt8>
{
};


struct MobilePhoneModelHash : public AttributeHashBase
{
};


struct MobilePhoneModel : public AttributeShortStringBase
{
};


struct BrowserLanguage : public AttributeUIntBase<UInt16>
{
	BinaryData parse(const std::string & s) const
	{
		std::string tmp = s;
		tmp.resize(sizeof(UInt16));
		return *reinterpret_cast<const UInt16 *>(tmp.data());
	}
};


struct BrowserCountry : public BrowserLanguage
{
};


struct TopLevelDomain : public AttributeShortStringBase
{
};


struct URLScheme : public AttributeShortStringBase
{
};


struct IPNetworkID : public AttributeUIntBase<UInt32>
{
};


struct ClientTimeZone : public AttributeIntBase<Poco::Int16>
{
};


struct OSID : public AttributeUIntBase<UInt8>
{
};


struct OSMostAncestor : public AttributeUIntBase<UInt8>
{
};


struct ClientIP : public AttributeUIntBase<UInt32>
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 24)
		: (tokenizer.count() == 2 ? (Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 24)
		+ (Poco::NumberParser::parseUnsigned(tokenizer[1]) << 16)
		: (tokenizer.count() == 3 ? (Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 24)
		+ (Poco::NumberParser::parseUnsigned(tokenizer[1]) << 16)
		+ (Poco::NumberParser::parseUnsigned(tokenizer[2]) << 8)
		: ((Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 24)
		+ (Poco::NumberParser::parseUnsigned(tokenizer[1]) << 16)
		+ (Poco::NumberParser::parseUnsigned(tokenizer[2]) << 8)
		+ Poco::NumberParser::parseUnsigned(tokenizer[3])))));
	}
};


struct Resolution : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, "x");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 24)
		: (tokenizer.count() == 2 ? (Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 24)
		+ (Poco::NumberParser::parseUnsigned(tokenizer[1]) << 8)
		: ((Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 24)
		+ (Poco::NumberParser::parseUnsigned(tokenizer[1]) << 8)
		+ Poco::NumberParser::parseUnsigned(tokenizer[2]))));
	}
};


struct ResolutionWidthHeight : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, "x");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 16)
		: ((Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 16)
		+ Poco::NumberParser::parseUnsigned(tokenizer[1])));
	}
};


struct ResolutionWidth : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct ResolutionHeight : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct ResolutionWidthInterval : public ResolutionWidth
{
};


struct ResolutionHeightInterval : public ResolutionHeight
{
};


struct ResolutionColor : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct WindowClientArea : public AttributeUIntBase<UInt32>
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, "x");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 16)
		: ((Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 16)
		+ Poco::NumberParser::parseUnsigned(tokenizer[1])));
	}
};


struct WindowClientAreaInterval : public WindowClientArea
{
};


struct WindowClientWidth : public AttributeUIntBase<UInt16>
{
};


struct WindowClientWidthInterval : public WindowClientWidth
{
};


struct WindowClientHeight : public AttributeUIntBase<UInt16>
{
};


struct WindowClientHeightInterval : public WindowClientHeight
{
};


struct SearchEngineID : public AttributeUIntBase<UInt8>
{
};


struct SearchEngineMostAncestor : public AttributeUIntBase<UInt8>
{
};


struct CodeVersion : public AttributeUIntBase<UInt32>
{
};


/// формат строки вида "10 7.5b", где первое число - UserAgentID, дальше - версия.
struct UserAgent : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, " .");
		return tokenizer.count() == 0 ? 0
			: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 24)
			: (tokenizer.count() == 2 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 24)
				+ (Poco::NumberParser::parseUnsigned(tokenizer[1]) << 16)
			: ((Poco::NumberParser::parseUnsigned(tokenizer[0]) << 24)
				+ (Poco::NumberParser::parseUnsigned(tokenizer[1]) << 16)
				+ (static_cast<UInt32>(tokenizer[2][0]) << 8)
				+ (tokenizer[2][1]))));
	}
};


struct UserAgentVersion : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
			: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 16)
			: ((Poco::NumberParser::parseUnsigned(tokenizer[0]) << 16)
				+ (static_cast<UInt32>(tokenizer[1][0]) << 8)
				+ tokenizer[1][1]));
	}
};


struct UserAgentMajor : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, " ");
		return tokenizer.count() == 0 ? 0
			: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8)
			: ((Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8)
				+ Poco::NumberParser::parseUnsigned(tokenizer[1])));
	}
};


struct UserAgentID : public AttributeInOneFileBase
{
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
};


struct ClickGoodEvent : public AttributeIntBase<Poco::Int8>
{
};

struct ClickPriorityID : public AttributeIntBase<Poco::Int32>
{
};

struct ClickBannerID : public AttributeIntBase<Poco::Int32>
{
};

struct ClickPhraseID : public AttributeIntBase<Poco::Int32>
{
};

struct ClickPageID : public AttributeIntBase<Poco::Int32>
{
};

struct ClickPlaceID : public AttributeIntBase<Poco::Int32>
{
};

struct ClickTypeID : public AttributeIntBase<Poco::Int32>
{
};

struct ClickResourceID : public AttributeIntBase<Poco::Int32>
{
};

struct ClickDomainID : public AttributeUIntBase<UInt32>
{
};

struct ClickCost : public AttributeUIntBase<UInt32>
{
};

struct ClickURLHash : public AttributeHashBase
{
};

struct ClickOrderID : public AttributeUIntBase<UInt32>
{
};

struct ClickTargetPhraseID : public AttributeUIntBase<UInt64>
{
};

struct GoalReachesAny : public AttributeUIntBase<Poco::Int16>
{
};

struct GoalReachesDepth : public AttributeUIntBase<Poco::Int16>
{
};

struct GoalReachesURL : public AttributeUIntBase<Poco::Int16>
{
};

struct ConvertedAny : public AttributeFixedPointBase<Poco::Int16>
{
};

struct ConvertedDepth : public AttributeFixedPointBase<Poco::Int16>
{
};

struct ConvertedURL : public AttributeFixedPointBase<Poco::Int16>
{
};


struct GoalReaches : public AttributeUIntBase<UInt16>
{
};

struct Converted : public AttributeFixedPointBase<UInt16>
{
};


struct CounterID : public AttributeUIntBase<UInt32>
{
};

struct VisitID : public AttributeUIntBase<UInt64>
{
};

struct Interests : public AttributeUIntBase<UInt16>
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
			UInt16 interest = Poco::NumberParser::parseUnsigned(boost::copy_range<std::string>(*i));
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

struct HasInterestPhoto : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestMoviePremieres : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestTourism : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestFamilyAndChildren : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestFinance : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestB2B : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestCars : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestMobileAndInternetCommunications : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestBuilding : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestCulinary : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestSoftware : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestEstate : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestHealthyLifestyle : public  AttributeUIntBase<UInt16>
{
};

struct HasInterestLiterature : public  AttributeUIntBase<UInt16>
{
};

struct OpenstatServiceNameHash : public AttributeHashBase
{
};

struct OpenstatCampaignIDHash : public AttributeHashBase
{
};

struct OpenstatAdIDHash : public AttributeHashBase
{
};

struct OpenstatSourceIDHash: public AttributeHashBase
{
};

struct UTMSourceHash : public AttributeHashBase
{
};

struct UTMMediumHash : public AttributeHashBase
{
};

struct UTMCampaignHash : public AttributeHashBase
{
};

struct UTMContentHash : public AttributeHashBase
{
};

struct UTMTermHash : public AttributeHashBase
{
};

struct FromHash : public AttributeHashBase
{
};

struct CLID : public AttributeUIntBase<UInt32>
{
};


/** Информация о типах атрибутов */
typedef std::map<std::string, Poco::SharedPtr<IAttributeMetadata> > AttributeMetadatas;

inline AttributeMetadatas GetOLAPAttributeMetadata()
{
	AttributeMetadatas metadata;
	
	metadata["DummyAttribute"] 			= new DummyAttribute;
	metadata["VisitStartDateTime"]		= new VisitStartDateTime;
	metadata["VisitStartDate"]			= new VisitStartDate;
	metadata["VisitStartTime"]			= new VisitStartTime;
	metadata["VisitStartYear"]			= new VisitStartYear;
	metadata["VisitStartMonth"]			= new VisitStartMonth;
	metadata["VisitStartDayOfWeek"]	= new VisitStartDayOfWeek;
	metadata["VisitStartDayOfMonth"]	= new VisitStartDayOfMonth;
	metadata["VisitStartHour"]			= new VisitStartHour;
	metadata["VisitStartMinute"]		= new VisitStartMinute;
	metadata["VisitStartSecond"]		= new VisitStartSecond;
	metadata["VisitStartWeek"]			= new VisitStartWeek;
	metadata["FirstVisitDateTime"]		= new FirstVisitDateTime;
	metadata["FirstVisitDate"]			= new FirstVisitDate;
	metadata["FirstVisitTime"]			= new FirstVisitTime;
	metadata["FirstVisitYear"]			= new FirstVisitYear;
	metadata["FirstVisitMonth"]			= new FirstVisitMonth;
	metadata["FirstVisitDayOfWeek"]	= new FirstVisitDayOfWeek;
	metadata["FirstVisitDayOfMonth"]	= new FirstVisitDayOfMonth;
	metadata["FirstVisitHour"]			= new FirstVisitHour;
	metadata["FirstVisitMinute"]		= new FirstVisitMinute;
	metadata["FirstVisitSecond"]		= new FirstVisitSecond;
	metadata["FirstVisitWeek"]			= new FirstVisitWeek;
	metadata["PredLastVisitDate"]		= new PredLastVisitDate;
	metadata["PredLastVisitYear"]		= new PredLastVisitYear;
	metadata["PredLastVisitMonth"]		= new PredLastVisitMonth;
	metadata["PredLastVisitDayOfWeek"]	= new PredLastVisitDayOfWeek;
	metadata["PredLastVisitDayOfMonth"]= new PredLastVisitDayOfMonth;
	metadata["PredLastVisitWeek"]		= new PredLastVisitWeek;
	metadata["RegionID"] 				= new RegionID;
	metadata["RegionCity"] 				= new RegionCity;
	metadata["RegionArea"] 				= new RegionArea;
	metadata["RegionCountry"]			= new RegionCountry;
	metadata["TraficSourceID"] 			= new TraficSourceID;
	metadata["UserNewness"] 			= new UserNewness;
	metadata["UserNewnessInterval"] 	= new UserNewnessInterval;
	metadata["UserReturnTime"] 			= new UserReturnTime;
	metadata["UserReturnTimeInterval"] = new UserReturnTimeInterval;
	metadata["UserVisitsPeriod"]		= new UserVisitsPeriod;
	metadata["UserVisitsPeriodInterval"]= new UserVisitsPeriodInterval;
	metadata["VisitTime"] 				= new VisitTime;
	metadata["VisitTimeInterval"]		= new VisitTimeInterval;
	metadata["PageViews"] 				= new PageViews;
	metadata["PageViewsInterval"]		= new PageViewsInterval;
	metadata["UserID"] 					= new UserID;
	metadata["TotalVisits"] 			= new TotalVisits;
	metadata["TotalVisitsInterval"]	= new TotalVisitsInterval;
	metadata["Age"] 					= new Age;
	metadata["AgeInterval"]				= new AgeInterval;
	metadata["Sex"] 					= new Sex;
	metadata["Income"] 					= new Income;
	metadata["AdvEngineID"] 			= new AdvEngineID;
	metadata["DotNet"] 					= new DotNet;
	metadata["DotNetMajor"]				= new DotNetMajor;
	metadata["EndURLHash"]				= new EndURLHash;
	metadata["Flash"] 					= new Flash;
	metadata["FlashMajor"]				= new FlashMajor;
	metadata["FlashExists"]				= new FlashExists;
	metadata["Hits"] 					= new Hits;
	metadata["HitsInterval"]			= new HitsInterval;
	metadata["JavaEnable"]				= new JavaEnable;
	metadata["OSID"] 					= new OSID;
	metadata["ClientIP"]				= new ClientIP;
	metadata["RefererHash"]				= new RefererHash;
	metadata["RefererDomainHash"]		= new RefererDomainHash;
	metadata["Resolution"]				= new Resolution;
	metadata["ResolutionWidthHeight"]	= new ResolutionWidthHeight;
	metadata["ResolutionWidth"]			= new ResolutionWidth;
	metadata["ResolutionHeight"]		= new ResolutionHeight;
	metadata["ResolutionWidthInterval"]= new ResolutionWidthInterval;
	metadata["ResolutionHeightInterval"]= new ResolutionHeightInterval;
	metadata["ResolutionColor"]			= new ResolutionColor;
	metadata["CookieEnable"]			= new CookieEnable;
	metadata["JavascriptEnable"]		= new JavascriptEnable;
	metadata["IsMobile"]				= new IsMobile;
	metadata["MobilePhoneID"]			= new MobilePhoneID;
	metadata["MobilePhoneModel"]		= new MobilePhoneModel;
	metadata["MobilePhoneModelHash"]	= new MobilePhoneModelHash;
	metadata["IPNetworkID"]				= new IPNetworkID;
	metadata["WindowClientArea"]		= new WindowClientArea;
	metadata["WindowClientWidth"]		= new WindowClientWidth;
	metadata["WindowClientHeight"]		= new WindowClientHeight;
	metadata["WindowClientAreaInterval"]= new WindowClientAreaInterval;
	metadata["WindowClientWidthInterval"]= new WindowClientWidthInterval;
	metadata["WindowClientHeightInterval"]= new WindowClientHeightInterval;
	metadata["ClientTimeZone"]			= new ClientTimeZone;
	metadata["ClientDateTime"]			= new ClientDateTime;
	metadata["ClientTime"]				= new ClientTime;
	metadata["ClientTimeHour"]			= new ClientTimeHour;
	metadata["ClientTimeMinute"]		= new ClientTimeMinute;
	metadata["ClientTimeSecond"]		= new ClientTimeSecond;
	metadata["Silverlight"]				= new Silverlight;
	metadata["SilverlightMajor"]		= new SilverlightMajor;
	metadata["SearchEngineID"]			= new SearchEngineID;
	metadata["SearchPhraseHash"]		= new SearchPhraseHash;
	metadata["StartURLHash"]			= new StartURLHash;
	metadata["StartURLDomainHash"]		= new StartURLDomainHash;
	metadata["UserAgent"]				= new UserAgent;
	metadata["UserAgentVersion"]		= new UserAgentVersion;
	metadata["UserAgentMajor"]			= new UserAgentMajor;
	metadata["UserAgentID"]				= new UserAgentID;
	metadata["ClickGoodEvent"]			= new ClickGoodEvent;
	metadata["ClickPriorityID"]			= new ClickPriorityID;
	metadata["ClickBannerID"]			= new ClickBannerID;
	metadata["ClickPhraseID"]			= new ClickPhraseID;
	metadata["ClickPageID"]				= new ClickPageID;
	metadata["ClickPlaceID"]			= new ClickPlaceID;
	metadata["ClickTypeID"]				= new ClickTypeID;
	metadata["ClickResourceID"]			= new ClickResourceID;
	metadata["ClickDomainID"]			= new ClickDomainID;
	metadata["ClickCost"]				= new ClickCost;
	metadata["ClickURLHash"]			= new ClickURLHash;
	metadata["ClickOrderID"]			= new ClickOrderID;
	metadata["ClickTargetPhraseID"]		= new ClickTargetPhraseID;
	metadata["GoalReaches"]				= new GoalReaches;
	metadata["GoalReachesAny"]			= new GoalReachesAny;
	metadata["GoalReachesDepth"]		= new GoalReachesDepth;
	metadata["GoalReachesURL"]			= new GoalReachesURL;
	metadata["Converted"]				= new Converted;
	metadata["ConvertedAny"]			= new ConvertedAny;
	metadata["ConvertedDepth"]			= new ConvertedDepth;
	metadata["ConvertedURL"]			= new ConvertedURL;
	metadata["Bounce"]					= new Bounce;
	metadata["BouncePrecise"]			= new BouncePrecise;
	metadata["IsNewUser"]				= new IsNewUser;
	metadata["CodeVersion"]				= new CodeVersion;
	metadata["CounterID"]				= new CounterID;
	metadata["VisitID"]					= new VisitID;
	metadata["IsYandex"]				= new IsYandex;
	metadata["TopLevelDomain"]			= new TopLevelDomain;
	metadata["URLScheme"]				= new URLScheme;
	metadata["UserIDCreateDateTime"]	= new UserIDCreateDateTime;
	metadata["UserIDCreateDate"]		= new UserIDCreateDate;
	metadata["UserIDAge"]				= new UserIDAge;
	metadata["UserIDAgeInterval"]		= new UserIDAgeInterval;
	metadata["OSMostAncestor"]			= new OSMostAncestor;
	metadata["SearchEngineMostAncestor"]= new SearchEngineMostAncestor;
	metadata["BrowserLanguage"]			= new BrowserLanguage;
	metadata["BrowserCountry"]			= new BrowserCountry;
	metadata["Interests"]				= new Interests;
	metadata["HasInterestPhoto"]		= new HasInterestPhoto;
	metadata["HasInterestMoviePremieres"]	= new HasInterestMoviePremieres;
	metadata["HasInterestMobileAndInternetCommunications"]	= new HasInterestMobileAndInternetCommunications;
	metadata["HasInterestFinance"]		= new HasInterestFinance;
	metadata["HasInterestFamilyAndChildren"]	= new HasInterestFamilyAndChildren;
	metadata["HasInterestCars"]			= new HasInterestCars;
	metadata["HasInterestB2B"]			= new HasInterestB2B;
	metadata["HasInterestTourism"]		= new HasInterestTourism;
	metadata["HasInterestBuilding"]		= new HasInterestBuilding;
	metadata["HasInterestCulinary"]		= new HasInterestCulinary;
	metadata["HasInterestSoftware"]		= new HasInterestSoftware;
	metadata["HasInterestEstate"]		= new HasInterestEstate;
	metadata["HasInterestHealthyLifestyle"]	= new HasInterestHealthyLifestyle;
	metadata["HasInterestLiterature"]	= new HasInterestLiterature;
	
	metadata["OpenstatServiceNameHash"]= new OpenstatServiceNameHash;
	metadata["OpenstatCampaignIDHash"]	= new OpenstatCampaignIDHash;
	metadata["OpenstatAdIDHash"]		= new OpenstatAdIDHash;
	metadata["OpenstatSourceIDHash"]	= new OpenstatSourceIDHash;
	
	metadata["UTMSourceHash"]			= new UTMSourceHash;
	metadata["UTMMediumHash"]			= new UTMMediumHash;
	metadata["UTMCampaignHash"]			= new UTMCampaignHash;
	metadata["UTMContentHash"]			= new UTMContentHash;
	metadata["UTMTermHash"]				= new UTMTermHash;
	
	metadata["FromHash"]				= new FromHash;
	metadata["CLID"]					= new CLID;
	
	return metadata;
}

}
}
