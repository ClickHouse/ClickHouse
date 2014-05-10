#pragma once

#include <DB/Core/Types.h>


/** Хэш функции, которые лучше чем тривиальная функция std::hash.
  * (при агрегации по идентификатору посетителя, прирост производительности более чем в 5 раз)
  */

/** https://code.google.com/p/fast-hash/
  * Быстрее, чем intHash32 на 21.5% при вставке в хэш-таблицу UInt64 -> UInt64, где ключ - идентификатор посетителя.
  */
inline DB::UInt64 intHash64(DB::UInt64 x)
{
	x ^= x >> 23;
	x *= 0x2127599bf4325c37ULL;
	x ^= x >> 47;

	return x;
}

template <typename T> struct DefaultHash;

template <typename T>
inline size_t DefaultHash64(T key)
{
	union
	{
		T in;
		DB::UInt64 out;
	} u;
	u.out = 0;
	u.in = key;
	return intHash64(u.out);
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
