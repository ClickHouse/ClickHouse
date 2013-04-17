#pragma once

#include <math.h>	// log2()

#include <boost/algorithm/string.hpp>

#include <Poco/NumberParser.h>
#include <Poco/NumberFormatter.h>
#include <Poco/StringTokenizer.h>

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
	/// имя в запросе (например, UserNewness)
	virtual std::string getName() const = 0;
	
	/// имена файлов, которые необходимы, чтобы считать значение атрибута (например, FirstVisit, VisitStart)
	virtual std::vector<std::string> getFileNames() const = 0;
	
	/// длина одной группы значений в файлах (например, 4, 4)
	virtual std::vector<size_t> getLengthInFiles() const = 0;
	
	/// длина значения (например, 2)
	virtual size_t getLength() const = 0;
	
	/// получение значения из наборов байт (групп значений) из файлов
	virtual BinaryData extract(const std::vector<void*> & buf) const = 0;
	
	/// получение значения из строки в запросе
	virtual BinaryData parse(const std::string & s) const = 0;
	
	/// получение строки из значения
	virtual std::string toString(BinaryData data) const = 0;
	
	/** true, если необходимо, чтобы после имени атрибута был в скобках указан параметр (например, "GoalReaches(111)")
	 * Тогда сервер будет считывать данные из файлов, к которым на конце приписан параметр после "_" ("FileName_111")
	 */
	virtual bool hasParameter() const { return false; }
	
	virtual ~IAttributeMetadata() {}
};


/// округление до степени двух или 0, если аргумент - 0.
inline UInt64 roundToExp2(UInt64 x)
{
	return x == 0 ? 0 : (1ULL << static_cast<UInt64>(log2(static_cast<double>(x))));
}


inline UInt64 roundTo100(UInt64 x)
{
	return x / 100 * 100;
}


inline UInt64 roundAge(UInt64 x)
{
	return x < 18 ? 0
	: (x < 25 ? 18
	: (x < 35 ? 25
	: (x < 45 ? 35
	: 45)));
}


inline UInt64 roundDuration(UInt64 x)
{
	return x == 0 ? 0
	: (x < 10 ? 1
	: (x < 30 ? 10
	: (x < 60 ? 30
	: (x < 120 ? 60
	: (x < 180 ? 120
	: (x < 240 ? 180
	: (x < 300 ? 240
	: (x < 600 ? 300
	: (x < 1200 ? 600
	: (x < 1800 ? 1200
	: (x < 3600 ? 1800
	: (x < 7200 ? 3600
	: (x < 18000 ? 7200
	: (x < 36000 ? 18000
	: 36000))))))))))))));
}


/// атрибут - заглушка, всегда равен нулю, подходит для подстановки в агрегатную функцию count
struct DummyAttribute : public IAttributeMetadata
{
	std::string getName() const { return "Dummy"; }
	std::vector<std::string> getFileNames() const { return std::vector<std::string>(); }
	std::vector<size_t> getLengthInFiles() const { return std::vector<size_t>(); }
	size_t getLength() const { return 0; }
	BinaryData extract(const std::vector<void*> & buf) const { return 0; }
	BinaryData parse(const std::string & s) const { return 0; }
	std::string toString(BinaryData data) const { return Poco::NumberFormatter::format(data); }
};


/// базовый класс для атрибутов, для получения значения которых надо прочитать только один файл (таких большинство)
struct AttributeInOneFileBase : public IAttributeMetadata
{
	std::vector<std::string> getFileNames() const
	{
		std::vector<std::string> res;
		res.push_back(getFileName());
		return res;
	}
	
	std::vector<size_t> getLengthInFiles() const
	{
		std::vector<size_t> res;
		res.push_back(getLengthInFile());
		return res;
	}
	
	BinaryData extract(const std::vector<void*> & buf) const
	{
		return extractFromOne(buf[0]);
	}
	
protected:
	virtual std::string getFileName() const = 0;
	virtual size_t getLengthInFile() const = 0;
	virtual BinaryData extractFromOne(void* buf) const = 0;
};


/// базовый класс для атрибутов, которые являются просто UInt8, UInt16, UInt32 или UInt64 (таких тоже много)
template <typename T>
struct AttributeUIntBase : public AttributeInOneFileBase
{
	std::string getFileName() const
	{
		return getName();
	}
	
	size_t getLengthInFile() const
	{
		return sizeof(T);
	}
	
	size_t getLength() const
	{
		return sizeof(T);
	}
	
	BinaryData extractFromOne(void* buf) const
	{
		return *static_cast<T*>(buf);
	}
	
	BinaryData parse(const std::string & s) const
	{
		return static_cast<BinaryData>(Poco::NumberParser::parseUnsigned64(s));
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(static_cast<UInt64>(data));
	}
};


/// базовый класс для атрибутов, которые являются Int8, Int16, Int32 или Int64
template <typename T>
struct AttributeIntBase : public AttributeInOneFileBase
{
	std::string getFileName() const
	{
		return getName();
	}
	
	size_t getLengthInFile() const
	{
		return sizeof(T);
	}
	
	size_t getLength() const
	{
		return sizeof(T);
	}
	
	BinaryData extractFromOne(void* buf) const
	{
		return *static_cast<T*>(buf);
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parse64(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


/** базовый класс для атрибутов, которые являются просто UInt8, UInt16, UInt32,
 * при этом значение округляется до степени двух,
 * при этом для хранения значения хватает точности double
 */
template <typename T>
struct AttributeUIntLogIntervalBase : public AttributeUIntBase<T>
{
	BinaryData extractFromOne(void* buf) const
	{
		return roundToExp2(*static_cast<T*>(buf));
	}
};


/** базовый класс для атрибутов, которые являются целыми числами, но
 * усреднение которых должно производиться с точностью до тысячных долей
 */
template <typename T>
struct AttributeFixedPointBase : public AttributeUIntBase<T>
{
	BinaryData extractFromOne(void* buf) const
	{
		return *static_cast<T*>(buf) * 1000;
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned64(s) * 1000;
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(static_cast<double>(data) / 1000, 3);
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
	
	std::string toString(BinaryData data) const
	{
		struct tm tm;
		char buf[24];
		time_t time_value = static_cast<Poco::Int32>(data);
		
		localtime_r(&time_value, &tm);
		snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
				 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
		
		return buf;
	}
};


struct AttributeDateBase : public AttributeIntBase<Poco::Int32>
{
	BinaryData extractFromOne(void* buf) const
	{
		return Yandex::DateLUTSingleton::instance().toDate(*static_cast<Poco::Int32*>(buf));
	}
	
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
	
	std::string toString(BinaryData data) const
	{
		struct tm tm;
		char buf[12];
		time_t time_value = static_cast<Poco::Int32>(data);
		
		localtime_r(&time_value, &tm);
		snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
				 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
		
		return buf;
	}
};


struct AttributeTimeBase : public AttributeIntBase<Poco::Int32>
{
	BinaryData extractFromOne(void* buf) const
	{
		return Yandex::DateLUTSingleton::instance().toTimeInaccurate(*static_cast<Poco::Int32*>(buf));
	}
	
	BinaryData parse(const std::string & s) const
	{
		struct tm tm;
		
		memset(&tm, 0, sizeof(tm));
		sscanf(s.c_str(), "%02d:%02d:%02d",
			   &tm.tm_hour, &tm.tm_min, &tm.tm_sec);
		
		return mktime(&tm);
	}
	
	std::string toString(BinaryData data) const
	{
		struct tm tm;
		char buf[12];
		time_t time_value = static_cast<Poco::Int32>(data);
		
		localtime_r(&time_value, &tm);
		snprintf(buf, sizeof(buf), "%02d:%02d:%02d",
				 tm.tm_hour, tm.tm_min, tm.tm_sec);
		
		return buf;
	}
};


struct AttributeYearBase : public AttributeUIntBase<UInt32>
{
	size_t getLength() const { return 2; }
	
	BinaryData extractFromOne(void* buf) const
	{
		/// на x86_64 время 64 битное, но мы его храним в файле как 32 битное
		return Yandex::DateLUTSingleton::instance().toYear(*static_cast<UInt32*>(buf));
	}
};


struct AttributeMonthBase : public AttributeUIntBase<UInt32>
{
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return Yandex::DateLUTSingleton::instance().toMonth(*static_cast<UInt32*>(buf));
	}
};


struct AttributeDayOfWeekBase : public AttributeUIntBase<UInt32>
{
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return Yandex::DateLUTSingleton::instance().toDayOfWeek(*static_cast<UInt32*>(buf));
	}
};


struct AttributeDayOfMonthBase : public AttributeUIntBase<UInt32>
{
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return Yandex::DateLUTSingleton::instance().toDayOfMonth(*static_cast<UInt32*>(buf));
	}
};


struct AttributeWeekBase : public AttributeDateBase
{
	BinaryData extractFromOne(void* buf) const
	{
		return Yandex::DateLUTSingleton::instance().toFirstDayOfWeek(*static_cast<Poco::Int32*>(buf));
	}
};


struct AttributeHourBase : public AttributeUIntBase<UInt32>
{
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return Yandex::DateLUTSingleton::instance().toHourInaccurate(*static_cast<Poco::Int32*>(buf));
	}
};


struct AttributeMinuteBase : public AttributeUIntBase<UInt32>
{
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return Yandex::DateLUTSingleton::instance().toMinuteInaccurate(*static_cast<UInt32*>(buf));
	}
};


struct AttributeSecondBase : public AttributeUIntBase<UInt32>
{
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return Yandex::DateLUTSingleton::instance().toSecondInaccurate(*static_cast<UInt32*>(buf));
	}
};


struct AttributeShortStringBase : public AttributeUIntBase<UInt64>
{
	BinaryData parse(const std::string & s) const
	{
		std::string tmp = s;
		tmp.resize(sizeof(BinaryData));
		return *reinterpret_cast<const BinaryData *>(tmp.data());
	}
	
	std::string toString(BinaryData data) const
	{
		std::string res(reinterpret_cast<const char *>(&data), 8);
		res.resize(strlen(res.c_str()));
		//Poco::trimRightInPlace(res);
		return res;
	}
};


/** Атрибуты, относящиеся к времени начала визита */
struct VisitStartDateTime : public AttributeDateTimeBase
{
	std::string getName() const { return "VisitStartDateTime"; }
	std::string getFileName() const { return "VisitStart"; }
};

struct VisitStartDate : public AttributeDateBase
{
	std::string getName() const { return "VisitStartDate"; }
	std::string getFileName() const { return "VisitStart"; }
};

struct VisitStartWeek : public AttributeWeekBase
{
	std::string getName() const { return "VisitStartWeek"; }
	std::string getFileName() const { return "VisitStart"; }
};

struct VisitStartTime : public AttributeTimeBase
{
	std::string getName() const { return "VisitStartTime"; }
	std::string getFileName() const { return "VisitStart"; }
};

struct VisitStartYear : public AttributeYearBase
{
	std::string getName() const { return "VisitStartYear"; }
	std::string getFileName() const { return "VisitStart"; }
};

struct VisitStartMonth : public AttributeMonthBase
{
	std::string getName() const { return "VisitStartMonth"; }
	std::string getFileName() const { return "VisitStart"; }
};

struct VisitStartDayOfWeek : public AttributeDayOfWeekBase
{
	std::string getName() const { return "VisitStartDayOfWeek"; }
	std::string getFileName() const { return "VisitStart"; }
};

struct VisitStartDayOfMonth : public AttributeDayOfMonthBase
{
	std::string getName() const { return "VisitStartDayOfMonth"; }
	std::string getFileName() const { return "VisitStart"; }
};

struct VisitStartHour : public AttributeHourBase
{
	std::string getName() const { return "VisitStartHour"; }
	std::string getFileName() const { return "VisitStart"; }
};

struct VisitStartMinute : public AttributeMinuteBase
{
	std::string getName() const { return "VisitStartMinute"; }
	std::string getFileName() const { return "VisitStart"; }
};

struct VisitStartSecond : public AttributeSecondBase
{
	std::string getName() const { return "VisitStartSecond"; }
	std::string getFileName() const { return "VisitStart"; }
};


/** Атрибуты, относящиеся к времени начала первого визита */
struct FirstVisitDateTime : public AttributeDateTimeBase
{
	std::string getName() const { return "FirstVisitDateTime"; }
	std::string getFileName() const { return "FirstVisit"; }
};

struct FirstVisitDate : public AttributeDateBase
{
	std::string getName() const { return "FirstVisitDate"; }
	std::string getFileName() const { return "FirstVisit"; }
};

struct FirstVisitWeek : public AttributeWeekBase
{
	std::string getName() const { return "FirstVisitWeek"; }
	std::string getFileName() const { return "FirstVisit"; }
};

struct FirstVisitTime : public AttributeTimeBase
{
	std::string getName() const { return "FirstVisitTime"; }
	std::string getFileName() const { return "FirstVisit"; }
};

struct FirstVisitYear : public AttributeYearBase
{
	std::string getName() const { return "FirstVisitYear"; }
	std::string getFileName() const { return "FirstVisit"; }
};

struct FirstVisitMonth : public AttributeMonthBase
{
	std::string getName() const { return "FirstVisitMonth"; }
	std::string getFileName() const { return "FirstVisit"; }
};

struct FirstVisitDayOfWeek : public AttributeDayOfWeekBase
{
	std::string getName() const { return "FirstVisitDayOfWeek"; }
	std::string getFileName() const { return "FirstVisit"; }
};

struct FirstVisitDayOfMonth : public AttributeDayOfMonthBase
{
	std::string getName() const { return "FirstVisitDayOfMonth"; }
	std::string getFileName() const { return "FirstVisit"; }
};

struct FirstVisitHour : public AttributeHourBase
{
	std::string getName() const { return "FirstVisitHour"; }
	std::string getFileName() const { return "FirstVisit"; }
};

struct FirstVisitMinute : public AttributeMinuteBase
{
	std::string getName() const { return "FirstVisitMinute"; }
	std::string getFileName() const { return "FirstVisit"; }
};

struct FirstVisitSecond : public AttributeSecondBase
{
	std::string getName() const { return "FirstVisitSecond"; }
	std::string getFileName() const { return "FirstVisit"; }
};


/** Атрибуты, относящиеся к времени начала предпоследнего визита */
struct PredLastVisitDate : public AttributeDateBase
{
	std::string getName() const { return "PredLastVisitDate"; }
	std::string getFileName() const { return "PredLastVisit"; }
};

struct PredLastVisitWeek : public AttributeWeekBase
{
	std::string getName() const { return "PredLastVisitWeek"; }
	std::string getFileName() const { return "PredLastVisit"; }
};

struct PredLastVisitYear : public AttributeYearBase
{
	std::string getName() const { return "PredLastVisitYear"; }
	std::string getFileName() const { return "PredLastVisit"; }
};

struct PredLastVisitMonth : public AttributeMonthBase
{
	std::string getName() const { return "PredLastVisitMonth"; }
	std::string getFileName() const { return "PredLastVisit"; }
};

struct PredLastVisitDayOfWeek : public AttributeDayOfWeekBase
{
	std::string getName() const { return "PredLastVisitDayOfWeek"; }
	std::string getFileName() const { return "PredLastVisit"; }
};

struct PredLastVisitDayOfMonth : public AttributeDayOfMonthBase
{
	std::string getName() const { return "PredLastVisitDayOfMonth"; }
	std::string getFileName() const { return "PredLastVisit"; }
};


/** Атрибуты, относящиеся к времени на компьютере посетителя */
struct ClientDateTime : public AttributeDateTimeBase
{
	std::string getName() const { return "ClientDateTime"; }
	std::string getFileName() const { return "ClientEventTime"; }
};

struct ClientTime : public AttributeTimeBase
{
	std::string getName() const { return "ClientTime"; }
	std::string getFileName() const { return "ClientEventTime"; }
};

struct ClientTimeHour : public AttributeHourBase
{
	std::string getName() const { return "ClientTimeHour"; }
	std::string getFileName() const { return "ClientEventTime"; }
};

struct ClientTimeMinute : public AttributeMinuteBase
{
	std::string getName() const { return "ClientTimeMinute"; }
	std::string getFileName() const { return "ClientEventTime"; }
};

struct ClientTimeSecond : public AttributeSecondBase
{
	std::string getName() const { return "ClientTimeSecond"; }
	std::string getFileName() const { return "ClientEventTime"; }
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
	std::string getName() const { return "EndURLHash"; }
};

struct RefererHash : public AttributeHashBase
{
	std::string getName() const { return "RefererHash"; }
};

struct SearchPhraseHash : public AttributeHashBase
{
	std::string getName() const { return "SearchPhraseHash"; }
};

struct RefererDomainHash : public AttributeHashBase
{
	std::string getName() const { return "RefererDomainHash"; }
};

struct StartURLHash : public AttributeHashBase
{
	std::string getName() const { return "StartURLHash"; }
};

struct StartURLDomainHash : public AttributeHashBase
{
	std::string getName() const { return "StartURLDomainHash"; }
};


struct RegionID : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "RegionID"; }
};


struct RegionCity : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "RegionCity"; }
	std::string getFileName() const { return "RegionID"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return 0;
		//return OLAPServer::current_regions_hierarchy->toCity(*static_cast<UInt32*>(buf));
	}
};


struct RegionArea : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "RegionArea"; }
	std::string getFileName() const { return "RegionID"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return 0;
		//return OLAPServer::current_regions_hierarchy->toArea(*static_cast<UInt32*>(buf));
	}
};


struct RegionCountry : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "RegionCountry"; }
	std::string getFileName() const { return "RegionID"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return 0;
		//return OLAPServer::current_regions_hierarchy->toCountry(*static_cast<UInt32*>(buf));
	}
};


struct TraficSourceID : public AttributeIntBase<Poco::Int8>
{
	std::string getName() const { return "TraficSourceID"; }
};


struct IsNewUser : public AttributeFixedPointBase<UInt8>
{
	std::string getName() const { return "IsNewUser"; };
	
	std::vector<std::string> getFileNames() const
	{
		std::vector<std::string> res;
		res.push_back("FirstVisit");
		res.push_back("VisitStart");
		return res;
	}
	
	std::vector<size_t> getLengthInFiles() const
	{
		std::vector<size_t> res;
		res.push_back(4);
		res.push_back(4);
		return res;
	}
	
	size_t getLength() const { return 1; };
	
	BinaryData extract(const std::vector<void*> & buf) const
	{
		return (*static_cast<UInt32*>(buf[1]) == *static_cast<UInt32*>(buf[0])) * 1000;
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s) * 1000;
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(static_cast<double>(data) / 1000, 3);
	}
};


struct UserNewness : public IAttributeMetadata
{
	std::string getName() const { return "UserNewness"; };
	
	std::vector<std::string> getFileNames() const
	{
		std::vector<std::string> res;
		res.push_back("FirstVisit");
		res.push_back("VisitStart");
		return res;
	}
	
	std::vector<size_t> getLengthInFiles() const
	{
		std::vector<size_t> res;
		res.push_back(4);
		res.push_back(4);
		return res;
	}
	
	size_t getLength() const { return 2; };
	
	BinaryData extract(const std::vector<void*> & buf) const
	{
		return (*static_cast<UInt32*>(buf[1]) <= *static_cast<UInt32*>(buf[0]))
		? 0
		: ((*static_cast<UInt32*>(buf[1]) - *static_cast<UInt32*>(buf[0])) / 86400) * 1000;
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s) * 1000;
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(static_cast<double>(data) / 1000, 3);
	}
};


struct UserNewnessInterval : public UserNewness
{
	std::string getName() const { return "UserNewnessInterval"; };
	
	BinaryData extract(const std::vector<void*> & buf) const
	{
		return roundToExp2((*static_cast<UInt32*>(buf[1]) <= *static_cast<UInt32*>(buf[0]))
		? 0
		: ((*static_cast<UInt32*>(buf[1]) - *static_cast<UInt32*>(buf[0])) / 86400));
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct UserReturnTime : public IAttributeMetadata
{
	std::string getName() const { return "UserReturnTime"; };
	
	std::vector<std::string> getFileNames() const
	{
		std::vector<std::string> res;
		res.push_back("PredLastVisit");
		res.push_back("VisitStart");
		return res;
	}
	
	std::vector<size_t> getLengthInFiles() const
	{
		std::vector<size_t> res;
		res.push_back(4);
		res.push_back(4);
		return res;
	}
	
	size_t getLength() const { return 2; };
	
	BinaryData extract(const std::vector<void*> & buf) const
	{
		return (*static_cast<UInt32*>(buf[1]) <= *static_cast<UInt32*>(buf[0]))
		? 0
		: ((*static_cast<UInt32*>(buf[1]) - *static_cast<UInt32*>(buf[0])) / 86400) * 1000;
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s) * 1000;
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(static_cast<double>(data) / 1000, 3);
	}
};


struct UserReturnTimeInterval : public UserReturnTime
{
	std::string getName() const { return "UserReturnTimeInterval"; };
	
	BinaryData extract(const std::vector<void*> & buf) const
	{
		return roundToExp2((*static_cast<UInt32*>(buf[1]) <= *static_cast<UInt32*>(buf[0]))
		? 0
		: ((*static_cast<UInt32*>(buf[1]) - *static_cast<UInt32*>(buf[0])) / 86400));
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct UserVisitsPeriod : public IAttributeMetadata
{
	std::string getName() const { return "UserVisitsPeriod"; };
	
	std::vector<std::string> getFileNames() const
	{
		std::vector<std::string> res;
		res.push_back("FirstVisit");
		res.push_back("VisitStart");
		res.push_back("TotalVisits");
		return res;
	}
	
	std::vector<size_t> getLengthInFiles() const
	{
		std::vector<size_t> res;
		res.push_back(4);
		res.push_back(4);
		res.push_back(4);
		return res;
	}
	
	size_t getLength() const { return 2; };
	
	/// TODO: баг при TotalVisits <= 1.
	BinaryData extract(const std::vector<void*> & buf) const
	{
		return (*static_cast<UInt32*>(buf[1]) <= *static_cast<UInt32*>(buf[0]) || *static_cast<UInt32*>(buf[2]) <= 1)
		? 0
		: ((*static_cast<UInt32*>(buf[1]) - *static_cast<UInt32*>(buf[0]))
		/ (*static_cast<UInt32*>(buf[2]) - 1) / 86400) * 1000;
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s) * 1000;
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(static_cast<double>(data) / 1000, 3);
	}
};


struct UserVisitsPeriodInterval : public UserVisitsPeriod
{
	std::string getName() const { return "UserVisitsPeriodInterval"; };
	
	BinaryData extract(const std::vector<void*> & buf) const
	{
		return roundToExp2(
			(*static_cast<UInt32*>(buf[1]) <= *static_cast<UInt32*>(buf[0]) || *static_cast<UInt32*>(buf[2]) <= 1)
			? 0
			: ((*static_cast<UInt32*>(buf[1]) - *static_cast<UInt32*>(buf[0]))
			/ (*static_cast<UInt32*>(buf[2]) - 1) / 86400));
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct VisitTime : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "VisitTime"; }
};


struct VisitTimeInterval : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "VisitTimeInterval"; }
	std::string getFileName() const { return "VisitTime"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return roundDuration(*static_cast<UInt32*>(buf));
	}
};


struct PageViews : public AttributeFixedPointBase<UInt32>
{
	std::string getName() const { return "PageViews"; }
};


struct PageViewsInterval : public AttributeUIntLogIntervalBase<UInt32>
{
	std::string getName() const { return "PageViewsInterval"; }
	std::string getFileName() const { return "PageViews"; }
};


struct Bounce : public AttributeFixedPointBase<UInt32>
{
	std::string getName() const { return "Bounce"; }
	std::string getFileName() const { return "PageViews"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (*static_cast<UInt32*>(buf) <= 1 ? 1 : 0) * 1000;
	}
};


struct BouncePrecise : public AttributeFixedPointBase<UInt8>
{
	std::string getName() const { return "BouncePrecise"; }
	std::string getFileName() const { return "Bounce"; }
};


struct IsYandex : public AttributeFixedPointBase<UInt8>
{
	std::string getName() const { return "IsYandex"; }
};


struct UserID : public AttributeUIntBase<UInt64>
{
	std::string getName() const { return "UserID"; }
};


struct UserIDCreateDateTime : public AttributeDateTimeBase
{
	std::string getName() const { return "UserIDCreateDateTime"; }
	std::string getFileName() const { return "UserID"; }
	size_t getLengthInFile() const { return 8; }
	
	BinaryData extractFromOne(void* buf) const
	{
		UInt64 user_id = *static_cast<UInt64*>(buf);
		
		if (user_id > 10000000000000000000ULL)
			return 0;
		
		BinaryData res = user_id % 10000000000ULL;
		if (unlikely(res > 2000000000 || res < 1000000000))
			return 0;
		
		return res;
	}
};


struct UserIDCreateDate : public AttributeDateBase
{
	std::string getName() const { return "UserIDCreateDate"; }
	std::string getFileName() const { return "UserID"; }
	size_t getLengthInFile() const { return 8; }
	
	BinaryData extractFromOne(void* buf) const
	{
		UInt64 user_id = *static_cast<UInt64*>(buf);
		
		if (user_id > 10000000000000000000ULL)
			return 0;
		
		BinaryData res = user_id % 10000000000ULL;
		if (unlikely(res > 2000000000 || res < 1000000000))
			return 0;
		
		return Yandex::DateLUTSingleton::instance().toDate(res);
	}
};


struct UserIDAge : public AttributeDateTimeBase
{
	std::string getName() const { return "UserIDAge"; }
	
	std::vector<std::string> getFileNames() const
	{
		std::vector<std::string> res;
		res.push_back("UserID");
		res.push_back("VisitStart");
		return res;
	}
	
	std::vector<size_t> getLengthInFiles() const
	{
		std::vector<size_t> res;
		res.push_back(8);
		res.push_back(4);
		return res;
	}
	
	size_t getLength() const { return 2; };
	
	BinaryData extract(const std::vector<void*> & buf) const
	{
		UInt64 user_id = *static_cast<UInt64*>(buf[0]);
		UInt32 visit_time = *static_cast<UInt32*>(buf[1]);
		
		if (user_id > 10000000000000000000ULL)
			return -1000;
		
		BinaryData user_time = user_id % 10000000000ULL;
		if (unlikely(user_time > visit_time || user_time < 1000000000))
			return -1000;
		
		return (visit_time - user_time) / 86400 * 1000;
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s) * 1000;
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(static_cast<double>(data) / 1000, 3);
	}
};


struct UserIDAgeInterval : public UserIDAge
{
	std::string getName() const { return "UserIDAgeInterval"; };
	
	BinaryData extract(const std::vector<void*> & buf) const
	{
		UInt64 user_id = *static_cast<UInt64*>(buf[0]);
		UInt32 visit_time = *static_cast<UInt32*>(buf[1]);
		
		if (user_id > 10000000000000000000ULL)
			return -1;
		
		BinaryData user_time = user_id % 10000000000ULL;
		if (unlikely(user_time > visit_time || user_time < 1000000000))
			return -1;
		
		return roundToExp2((visit_time - user_time) / 86400);
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct TotalVisits : public AttributeFixedPointBase<UInt32>
{
	std::string getName() const { return "TotalVisits"; }
};


struct TotalVisitsInterval : public AttributeUIntLogIntervalBase<UInt32>
{
	std::string getName() const { return "TotalVisitsInterval"; }
	std::string getFileName() const { return "TotalVisits"; }
};


struct Age : public AttributeFixedPointBase<UInt8>
{
	std::string getName() const { return "Age"; }
};


struct AgeInterval : public AttributeUIntBase<UInt8>
{
	std::string getName() const { return "AgeInterval"; }
	std::string getFileName() const { return "Age"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return roundAge(*static_cast<UInt8*>(buf));
	}
};


struct Sex : public AttributeFixedPointBase<UInt8>
{
	std::string getName() const { return "Sex"; }
};


struct Income : public AttributeFixedPointBase<UInt8>
{
	std::string getName() const { return "Income"; }
};


struct AdvEngineID : public AttributeUIntBase<UInt8>
{
	std::string getName() const { return "AdvEngineID"; }
};


struct DotNet : public AttributeInOneFileBase
{
	std::string getName() const { return "DotNet"; }
	std::string getFileName() const { return "DotNet"; }
	size_t getLengthInFile() const { return 2; }
	size_t getLength() const { return 2; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>(static_cast<UInt8*>(buf)[0]) << 8) + static_cast<UInt8*>(buf)[1];
	}
	
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8)
		: ((Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8) + Poco::NumberParser::parseUnsigned(tokenizer[1])));
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data >> 8) + "." + Poco::NumberFormatter::format(data & 0xFF);
	}
};


struct DotNetMajor : public AttributeInOneFileBase
{
	std::string getName() const { return "DotNetMajor"; }
	std::string getFileName() const { return "DotNet"; }
	size_t getLengthInFile() const { return 2; }
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return *static_cast<UInt8*>(buf);
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct Flash : public AttributeInOneFileBase
{
	std::string getName() const { return "Flash"; }
	std::string getFileName() const { return "Flash"; }
	size_t getLengthInFile() const { return 2; }
	size_t getLength() const { return 2; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>(static_cast<UInt8*>(buf)[0]) << 8) + static_cast<UInt8*>(buf)[1];
	}
	
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8)
		: ((Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8) + Poco::NumberParser::parseUnsigned(tokenizer[1])));
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data >> 8) + "." + Poco::NumberFormatter::format(data & 0xFF);
	}
};


struct FlashExists : public AttributeInOneFileBase
{
	std::string getName() const { return "FlashExists"; }
	std::string getFileName() const { return "Flash"; }
	size_t getLengthInFile() const { return 2; }
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return *static_cast<UInt8*>(buf) ? 1 : 0;
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct FlashMajor : public AttributeInOneFileBase
{
	std::string getName() const { return "FlashMajor"; }
	std::string getFileName() const { return "Flash"; }
	size_t getLengthInFile() const { return 2; }
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return *static_cast<UInt8*>(buf);
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct Silverlight : public AttributeUIntBase<UInt64>
{
	std::string getName() const { return "Silverlight"; }
	
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
	
	std::string toString(BinaryData data) const
	{
		std::stringstream s;
		s << (data >> 56) << '.' << ((data >> 48) & 0xFF) << '.' << ((data >> 16) & 0xFFFFFFFFU) << '.' << (data & 0xFFFF);
		return s.str();
	}
};


struct SilverlightMajor : public AttributeUIntBase<UInt64>
{
	std::string getName() const { return "SilverlightMajor"; }
	std::string getFileName() const { return "Silverlight"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return static_cast<UInt8*>(buf)[7];
	}
};


struct Hits : public AttributeFixedPointBase<UInt32>
{
	std::string getName() const { return "Hits"; }
};


struct HitsInterval : public AttributeUIntLogIntervalBase<UInt32>
{
	std::string getName() const { return "HitsInterval"; }
	std::string getFileName() const { return "Hits"; }
};


struct JavaEnable : public AttributeFixedPointBase<UInt8>
{
	std::string getName() const { return "JavaEnable"; }
};


struct CookieEnable : public AttributeFixedPointBase<UInt8>
{
	std::string getName() const { return "CookieEnable"; }
};


struct JavascriptEnable : public AttributeFixedPointBase<UInt8>
{
	std::string getName() const { return "JavascriptEnable"; }
};


struct IsMobile : public AttributeFixedPointBase<UInt8>
{
	std::string getName() const { return "IsMobile"; }
};


struct MobilePhoneID : public AttributeUIntBase<UInt8>
{
	std::string getName() const { return "MobilePhoneID"; }
};


struct MobilePhoneModelHash : public AttributeHashBase
{
	std::string getName() const { return "MobilePhoneModelHash"; }
};


struct MobilePhoneModel : public AttributeShortStringBase
{
	std::string getName() const { return "MobilePhoneModel"; }
};


struct BrowserLanguage : public AttributeUIntBase<UInt16>
{
	std::string getName() const { return "BrowserLanguage"; }
	
	BinaryData parse(const std::string & s) const
	{
		std::string tmp = s;
		tmp.resize(sizeof(UInt16));
		return *reinterpret_cast<const UInt16 *>(tmp.data());
	}
	
	std::string toString(BinaryData data) const
	{
		return std::string(reinterpret_cast<const char *>(&data), 2);
	}
};


struct BrowserCountry : public BrowserLanguage
{
	std::string getName() const { return "BrowserCountry"; }
};


struct TopLevelDomain : public AttributeShortStringBase
{
	std::string getName() const { return "TopLevelDomain"; }
};


struct URLScheme : public AttributeShortStringBase
{
	std::string getName() const { return "URLScheme"; }
};


struct IPNetworkID : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "IPNetworkID"; }
};


struct ClientTimeZone : public AttributeIntBase<Poco::Int16>
{
	std::string getName() const { return "ClientTimeZone"; }
};


struct OSID : public AttributeUIntBase<UInt8>
{
	std::string getName() const { return "OSID"; }
};


struct OSMostAncestor : public AttributeUIntBase<UInt8>
{
	std::string getName() const { return "OSMostAncestor"; }
	std::string getFileName() const { return "OSID"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return TechDataHierarchySingleton::instance().OSToMostAncestor(*static_cast<UInt8*>(buf));
	}
};


struct ClientIP : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "ClientIP"; }
	
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
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(static_cast<UInt8>((data >> 24) & 0xFF))
		+ "." + Poco::NumberFormatter::format(static_cast<UInt8>((data >> 16) & 0xFF))
		+ "." + Poco::NumberFormatter::format(static_cast<UInt8>((data >> 8) & 0xFF))
		+ "." + Poco::NumberFormatter::format(static_cast<UInt8>((data & 0xFF)));
	}
};


struct Resolution : public AttributeInOneFileBase
{
	std::string getName() const { return "Resolution"; }
	std::string getFileName() const { return "Resolution"; }
	size_t getLengthInFile() const { return 8; }
	size_t getLength() const { return 8; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>(static_cast<UInt16*>(buf)[0]) << 24)
		+ (static_cast<UInt64>(static_cast<UInt16*>(buf)[1]) << 8)
		+ static_cast<UInt8*>(buf)[4];
	}
	
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
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data >> 24)
		+ "x" + Poco::NumberFormatter::format((data >> 8) & 0xFFFF)
		+ "x" + Poco::NumberFormatter::format(data & 0xFF);
	}
};


struct ResolutionWidthHeight : public AttributeInOneFileBase
{
	std::string getName() const { return "ResolutionWidthHeight"; }
	std::string getFileName() const { return "Resolution"; }
	size_t getLengthInFile() const { return 8; }
	size_t getLength() const { return 4; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>(static_cast<UInt16*>(buf)[0]) << 16) + static_cast<UInt16*>(buf)[1];
	}
	
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, "x");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 16)
		: ((Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 16)
		+ Poco::NumberParser::parseUnsigned(tokenizer[1])));
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data >> 16)
		+ "x" + Poco::NumberFormatter::format(data & 0xFFFF);
	}
};


struct ResolutionWidth : public AttributeInOneFileBase
{
	std::string getName() const { return "ResolutionWidth"; }
	std::string getFileName() const { return "Resolution"; }
	size_t getLengthInFile() const { return 8; }
	size_t getLength() const { return 2; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return *static_cast<UInt16*>(buf);
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct ResolutionHeight : public AttributeInOneFileBase
{
	std::string getName() const { return "ResolutionHeight"; }
	std::string getFileName() const { return "Resolution"; }
	size_t getLengthInFile() const { return 8; }
	size_t getLength() const { return 2; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return static_cast<UInt16*>(buf)[1];
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct ResolutionWidthInterval : public ResolutionWidth
{
	std::string getName() const { return "ResolutionWidthInterval"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return roundTo100(*static_cast<UInt16*>(buf));
	}
};


struct ResolutionHeightInterval : public ResolutionHeight
{
	std::string getName() const { return "ResolutionHeightInterval"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return roundTo100(static_cast<UInt16*>(buf)[1]);
	}
};


struct ResolutionColor : public AttributeInOneFileBase
{
	std::string getName() const { return "ResolutionColor"; }
	std::string getFileName() const { return "Resolution"; }
	size_t getLengthInFile() const { return 8; }
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return static_cast<UInt8*>(buf)[4];
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct WindowClientArea : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "WindowClientArea"; }
	
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, "x");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 16)
		: ((Poco::NumberParser::parseUnsigned64(tokenizer[0]) << 16)
		+ Poco::NumberParser::parseUnsigned(tokenizer[1])));
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data >> 16)
		+ "x" + Poco::NumberFormatter::format(data & 0xFFFF);
	}
};


struct WindowClientAreaInterval : public WindowClientArea
{
	std::string getName() const { return "WindowClientAreaInterval"; }
	std::string getFileName() const { return "WindowClientArea"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return roundTo100(static_cast<UInt16*>(buf)[0])
		+ (roundTo100(static_cast<UInt16*>(buf)[1]) << 16);
	}
};


struct WindowClientWidth : public AttributeUIntBase<UInt16>
{
	std::string getName() const { return "WindowClientWidth"; }
	std::string getFileName() const { return "WindowClientArea"; }
	size_t getLengthInFile() const { return 4; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return static_cast<UInt16*>(buf)[1];
	}
};


struct WindowClientWidthInterval : public WindowClientWidth
{
	std::string getName() const { return "WindowClientWidthInterval"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return roundTo100(static_cast<UInt16*>(buf)[1]);
	}
};


struct WindowClientHeight : public AttributeUIntBase<UInt16>
{
	std::string getName() const { return "WindowClientHeight"; }
	std::string getFileName() const { return "WindowClientArea"; }
	size_t getLengthInFile() const { return 4; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return static_cast<UInt16*>(buf)[0];
	}
};


struct WindowClientHeightInterval : public WindowClientHeight
{
	std::string getName() const { return "WindowClientHeightInterval"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return roundTo100(static_cast<UInt16*>(buf)[0]);
	}
};


struct SearchEngineID : public AttributeUIntBase<UInt8>
{
	std::string getName() const { return "SearchEngineID"; }
};


struct SearchEngineMostAncestor : public AttributeUIntBase<UInt8>
{
	std::string getName() const { return "SEMostAncestor"; }
	std::string getFileName() const { return "SearchEngineID"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return TechDataHierarchySingleton::instance().SEToMostAncestor(*static_cast<UInt8*>(buf));
	}
};


struct CodeVersion : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "CodeVersion"; }
};


/// формат строки вида "10 7.5b", где первое число - UserAgentID, дальше - версия.
struct UserAgent : public AttributeInOneFileBase
{
	std::string getName() const { return "UserAgent"; }
	std::string getFileName() const { return "UserAgent"; }
	size_t getLengthInFile() const { return 4; }
	size_t getLength() const { return 4; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>(static_cast<UInt8*>(buf)[0]) << 24)
		+ (static_cast<UInt64>(static_cast<UInt8*>(buf)[1]) << 16)
		+ (static_cast<UInt64>(static_cast<UInt8*>(buf)[2]) << 8)
		+ static_cast<UInt8*>(buf)[3];
	}
	
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, " .");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 24)
		: (tokenizer.count() == 2 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 24)
		+ (Poco::NumberParser::parseUnsigned(tokenizer[1]) << 16)
		: ((Poco::NumberParser::parseUnsigned(tokenizer[0]) << 24)
		+ (Poco::NumberParser::parseUnsigned(tokenizer[1]) << 16)
		+ (tokenizer[2][0] << 8)
		+ (tokenizer[2][1]))));
	}
	
	std::string toString(BinaryData data) const
	{
		std::stringstream s;
		s << Poco::NumberFormatter::format(data >> 24)
		<< " " << Poco::NumberFormatter::format((data >> 16) & 0xFF);
		
		if (data & 0xFFFF)
		{
			char char1 = (data >> 8) & 0xFF;
			char char2 = data & 0xFF;
			
			s << ".";
			
			/// Не выводим плохие символы.
			if (isalnum(char1))
				s << char1;
			if (isalnum(char2))
				s << char2;
		}
		
		return s.str();
	}
};


struct UserAgentVersion : public AttributeInOneFileBase
{
	std::string getName() const { return "UserAgentVersion"; }
	std::string getFileName() const { return "UserAgent"; }
	size_t getLengthInFile() const { return 4; }
	size_t getLength() const { return 4; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>(static_cast<UInt8*>(buf)[1]) << 16)
		+ (static_cast<UInt64>(static_cast<UInt8*>(buf)[2]) << 8)
		+ static_cast<UInt8*>(buf)[3];
	}
	
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, ".");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 16)
		: ((Poco::NumberParser::parseUnsigned(tokenizer[0]) << 16)
		+ (tokenizer[1][0] << 8)
		+ (tokenizer[1][1])));
	}
	
	std::string toString(BinaryData data) const
	{
		std::stringstream s;
		s << Poco::NumberFormatter::format((data >> 16) & 0xFF);
		
		if (data & 0xFFFF)
		{
			char char1 = (data >> 8) & 0xFF;
			char char2 = data & 0xFF;
			
			s << ".";
			
			/// Не выводим плохие символы.
			if (isalnum(char1))
				s << char1;
			if (isalnum(char2))
				s << char2;
		}
		
		return s.str();
	}
};


struct UserAgentMajor : public AttributeInOneFileBase
{
	std::string getName() const { return "UserAgentMajor"; }
	std::string getFileName() const { return "UserAgent"; }
	size_t getLengthInFile() const { return 4; }
	size_t getLength() const { return 2; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>(static_cast<UInt8*>(buf)[0]) << 8)
		+ static_cast<UInt64>(static_cast<UInt8*>(buf)[1]);
	}
	
	BinaryData parse(const std::string & s) const
	{
		Poco::StringTokenizer tokenizer(s, " ");
		return tokenizer.count() == 0 ? 0
		: (tokenizer.count() == 1 ? (Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8)
		: ((Poco::NumberParser::parseUnsigned(tokenizer[0]) << 8)
		+ Poco::NumberParser::parseUnsigned(tokenizer[1])));
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data >> 8)
		+ " " + Poco::NumberFormatter::format(data & 0xFF);
	}
};


struct UserAgentID : public AttributeInOneFileBase
{
	std::string getName() const { return "UserAgentID"; }
	std::string getFileName() const { return "UserAgent"; }
	size_t getLengthInFile() const { return 4; }
	size_t getLength() const { return 1; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return *static_cast<UInt8*>(buf);
	}
	
	BinaryData parse(const std::string & s) const
	{
		return Poco::NumberParser::parseUnsigned(s);
	}
	
	std::string toString(BinaryData data) const
	{
		return Poco::NumberFormatter::format(data);
	}
};


struct ClickGoodEvent : public AttributeIntBase<Poco::Int8>
{
	std::string getName() const { return "ClickGoodEvent"; }
};

struct ClickPriorityID : public AttributeIntBase<Poco::Int32>
{
	std::string getName() const { return "ClickPriorityID"; }
};

struct ClickBannerID : public AttributeIntBase<Poco::Int32>
{
	std::string getName() const { return "ClickBannerID"; }
};

struct ClickPhraseID : public AttributeIntBase<Poco::Int32>
{
	std::string getName() const { return "ClickPhraseID"; }
};

struct ClickPageID : public AttributeIntBase<Poco::Int32>
{
	std::string getName() const { return "ClickPageID"; }
};

struct ClickPlaceID : public AttributeIntBase<Poco::Int32>
{
	std::string getName() const { return "ClickPlaceID"; }
};

struct ClickTypeID : public AttributeIntBase<Poco::Int32>
{
	std::string getName() const { return "ClickTypeID"; }
};

struct ClickResourceID : public AttributeIntBase<Poco::Int32>
{
	std::string getName() const { return "ClickResourceID"; }
};

struct ClickDomainID : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "ClickDomainID"; }
};

struct ClickCost : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "ClickCost"; }
};

struct ClickURLHash : public AttributeHashBase
{
	std::string getName() const { return "ClickURLHash"; }
};

struct ClickOrderID : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "ClickOrderID"; }
};


struct GoalReachesAny : public AttributeUIntBase<Poco::Int16>
{
	std::string getName() const { return "GoalReachesAny"; }
};

struct GoalReachesDepth : public AttributeUIntBase<Poco::Int16>
{
	std::string getName() const { return "GoalReachesDepth"; }
};

struct GoalReachesURL : public AttributeUIntBase<Poco::Int16>
{
	std::string getName() const { return "GoalReachesURL"; }
};

struct ConvertedAny : public AttributeFixedPointBase<Poco::Int16>
{
	std::string getName() const { return "ConvertedAny"; }
	std::string getFileName() const { return "GoalReachesAny"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		BinaryData value = *static_cast<Poco::Int16*>(buf);
		return (value == -1) ? -1 : (value != 0 ? 1000 : 0);
	}
};

struct ConvertedDepth : public AttributeFixedPointBase<Poco::Int16>
{
	std::string getName() const { return "ConvertedDepth"; }
	std::string getFileName() const { return "GoalReachesDepth"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		BinaryData value = *static_cast<Poco::Int16*>(buf);
		return (value == -1) ? -1 : (value != 0 ? 1000 : 0);
	}
};

struct ConvertedURL : public AttributeFixedPointBase<Poco::Int16>
{
	std::string getName() const { return "ConvertedURL"; }
	std::string getFileName() const { return "GoalReachesURL"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		BinaryData value = *static_cast<Poco::Int16*>(buf);
		return (value == -1) ? -1 : (value != 0 ? 1000 : 0);
	}
};


struct GoalReaches : public AttributeUIntBase<UInt16>
{
	std::string getName() const { return "GoalReaches"; }
	bool hasParameter() const { return true; }
};

struct Converted : public AttributeFixedPointBase<UInt16>
{
	std::string getName() const { return "Converted"; }
	std::string getFileName() const { return "GoalReaches"; }
	bool hasParameter() const { return true; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (*static_cast<UInt16*>(buf) != 0 ? 1000 : 0);
	}
};


struct CounterID : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "CounterID"; }
};

struct VisitID : public AttributeUIntBase<UInt64>
{
	std::string getName() const { return "VisitID"; }
};

struct Interests : public AttributeUIntBase<UInt16>
{
	std::string getName() const { return "Interests"; }
	
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
			value |= (interest == 512 ? 512 : (interest == 256 ? 256 :
			(interest == 128 ? 128 : ( interest == 64 ? 64 :(interest == 32 ? 32 :
			(interest == 16 ? 16 : (interest == 8 ? 8 :(interest == 4 ? 4 :
			(interest == 2 ? 2 :(interest == 1 ? 1 : 0))))))))));
		}
		
		return value;
	}
	
	std::string toString(BinaryData data) const
	{
		std::stringstream out;
		bool comma_required = false;
		for(unsigned char i = 0; i < getLength() * 8; ++i)
		{
			if(static_cast<UInt16>(data) & 1 << i)
			{
				if(comma_required)
					out << ",";
				out << Poco::NumberFormatter::format(1 << i);
				comma_required = true;
			}
		}
		return out.str();
	}
};

struct HasInterestPhoto : public  AttributeUIntBase<UInt16>
{
	std::string getName() const { return "HasInterestPhoto"; }
	std::string getFileName() const { return "Interests"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>((static_cast<UInt16 *>(buf)[0] & PHOTO) >> 7));
	}
};

struct HasInterestMoviePremieres : public  AttributeUIntBase<UInt16>
{
	std::string getName() const { return "HasInterestMoviePremieres"; }
	std::string getFileName() const { return "Interests"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>((static_cast<UInt16 *>(buf)[0] & MOVIE_PREMIERES) >> 6));
	}
};

struct HasInterestTourism : public  AttributeUIntBase<UInt16>
{
	std::string getName() const { return "HasInterestTourism"; }
	std::string getFileName() const { return "Interests"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>((static_cast<UInt16 *>(buf)[0] & TOURISM) >> 5));
	}
};

struct HasInterestFamilyAndChildren : public  AttributeUIntBase<UInt16>
{
	std::string getName() const { return "HasInterestFamilyAndChildren"; }
	std::string getFileName() const { return "Interests"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>((static_cast<UInt16 *>(buf)[0] & FAMILY_AND_CHILDREN) >> 4));
	}
};

struct HasInterestFinance : public  AttributeUIntBase<UInt16>
{
	std::string getName() const { return "HasInterestFinance"; }
	std::string getFileName() const { return "Interests"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>((static_cast<UInt16 *>(buf)[0] & FINANCE) >> 3));
	}
};

struct HasInterestB2B : public  AttributeUIntBase<UInt16>
{
	std::string getName() const { return "HasInterestB2B"; }
	std::string getFileName() const { return "Interests"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>((static_cast<UInt16 *>(buf)[0] & B2B) >> 2));
	}
};

struct HasInterestCars : public  AttributeUIntBase<UInt16>
{
	std::string getName() const { return "HasInterestCars"; }
	std::string getFileName() const { return "Interests"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>((static_cast<UInt16 *>(buf)[0] & CARS) >> 1));
	}
};

struct HasInterestMobileAndInternetCommunications : public  AttributeUIntBase<UInt16>
{
	std::string getName() const { return "HasInterestMobileAndInternetCommunications"; }
	std::string getFileName() const { return "Interests"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>(static_cast<UInt16 *>(buf)[0] & MOBILE_AND_INTERNET_COMMUNICATIONS));
	}
};

struct HasInterestBuilding : public  AttributeUIntBase<UInt16>
{
	std::string getName() const { return "HasInterestBuilding"; }
	std::string getFileName() const { return "Interests"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>(static_cast<UInt16 *>(buf)[0] & BUILDING));
	}
};

struct HasInterestCulinary : public  AttributeUIntBase<UInt16>
{
	std::string getName() const { return "HasInterestCulinary"; }
	std::string getFileName() const { return "Interests"; }
	
	BinaryData extractFromOne(void* buf) const
	{
		return (static_cast<UInt64>(static_cast<UInt16 *>(buf)[0] & CULINARY));
	}
};

struct OpenstatServiceNameHash : public AttributeHashBase
{
	std::string getName() const { return "OpenstatServiceNameHash"; }
};

struct OpenstatCampaignIDHash : public AttributeHashBase
{
	std::string getName() const { return "OpenstatCampaignIDHash"; }
};

struct OpenstatAdIDHash : public AttributeHashBase
{
	std::string getName() const { return "OpenstatAdIDHash"; }
};

struct OpenstatSourceIDHash: public AttributeHashBase
{
	std::string getName() const { return "OpenstatSourceIDHash"; }
};

struct UTMSourceHash : public AttributeHashBase
{
	std::string getName() const { return "UTMSourceHash"; }
};

struct UTMMediumHash : public AttributeHashBase
{
	std::string getName() const { return "UTMMediumHash"; }
};

struct UTMCampaignHash : public AttributeHashBase
{
	std::string getName() const { return "UTMCampaignHash"; }
};

struct UTMContentHash : public AttributeHashBase
{
	std::string getName() const { return "UTMContentHash"; }
};

struct UTMTermHash : public AttributeHashBase
{
	std::string getName() const { return "UTMTermHash"; }
};

struct FromHash : public AttributeHashBase
{
	std::string getName() const { return "FromHash"; }
};

struct CLID : public AttributeUIntBase<UInt32>
{
	std::string getName() const { return "CLID"; }
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
	metadata["HasInterestMoviePremieres"]= new HasInterestMoviePremieres;
	metadata["HasInterestMobileAndInternetCommunications"]	= new HasInterestMobileAndInternetCommunications;
	metadata["HasInterestFinance"]		= new HasInterestFinance;
	metadata["HasInterestFamilyAndChildren"]= new HasInterestFamilyAndChildren;
	metadata["HasInterestCars"]			= new HasInterestCars;
	metadata["HasInterestB2B"]			= new HasInterestB2B;
	metadata["HasInterestTourism"]		= new HasInterestTourism;
	metadata["HasInterestBuilding"]	= new HasInterestBuilding;
	metadata["HasInterestCulinary"]	= new HasInterestCulinary;
	
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
