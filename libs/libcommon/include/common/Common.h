#pragma once

#include <string>
#include <functional>

#include <Poco/Types.h>

#include <common/strong_typedef.h>


typedef Poco::Int8 Int8;
typedef Poco::Int16 Int16;
typedef Poco::Int32 Int32;
typedef Poco::Int64 Int64;

typedef Poco::UInt8 UInt8;
typedef Poco::UInt16 UInt16;
typedef Poco::UInt32 UInt32;
typedef Poco::UInt64 UInt64;


/// Обход проблемы с тем, что KDevelop не видит time_t и size_t (для подсветки синтаксиса).
#ifdef IN_KDEVELOP_PARSER
	typedef Int64 time_t;
	typedef UInt64 size_t;
#endif

		
/** Тип данных для хранения идентификатора пользователя. */
typedef UInt64 UserID_t;

/** Тип данных для хранения идентификатора счетчика. */
typedef UInt32 CounterID_t;

/** Идентификатор хита */
typedef UInt64 WatchID_t;

/** Идентификатор визита */
STRONG_TYPEDEF(UInt64, VisitID_t);

/** Идентификатор клика */
typedef UInt64 ClickID_t;

/** Идентификатор цели */
typedef UInt32 GoalID_t;


namespace std
{
	template<>
	struct hash<VisitID_t> : public unary_function<VisitID_t, size_t>
	{
		size_t operator()(VisitID_t x) const { return x; }
	};
}
