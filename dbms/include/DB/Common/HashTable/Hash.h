#pragma once

#include <stats/IntHash.h>
#include <DB/Core/Types.h>


/** Хэш функции, которые лучше чем тривиальная функция std::hash.
  * (при агрегации по идентификатору посетителя, прирост производительности более чем в 5 раз)
  */
template <typename T> struct DefaultHash;

template <typename T>
inline size_t DefaultHash64(T key)
{
	union
	{
		T in;
		UInt64 out;
	} u;
	u.out = 0;
	u.in = key;
	return intHash32<0>(u.out);
}

#define DEFAULT_HASH_64(T) \
template <> struct DefaultHash<T>\
{\
	size_t operator() (T key) const\
	{\
		return DefaultHash64<T>(key);\
	}\
};

DEFAULT_HASH_64(DB::UInt8)
DEFAULT_HASH_64(DB::UInt16)
DEFAULT_HASH_64(DB::UInt32)
DEFAULT_HASH_64(DB::UInt64)
DEFAULT_HASH_64(DB::Int8)
DEFAULT_HASH_64(DB::Int16)
DEFAULT_HASH_64(DB::Int32)
DEFAULT_HASH_64(DB::Int64)
DEFAULT_HASH_64(DB::Float32)
DEFAULT_HASH_64(DB::Float64)

#undef DEFAULT_HASH_64


/// Может быть специализировано для пользовательских типов.
template <typename T>
struct ZeroTraits
{
	static bool check(const T x) { return x == 0; }
	static void set(T & x) { x = 0; }
};
