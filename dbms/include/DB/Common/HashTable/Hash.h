#pragma once

#include <DB/Core/Types.h>


/** Хэш функции, которые лучше чем тривиальная функция std::hash.
  * (при агрегации по идентификатору посетителя, прирост производительности более чем в 5 раз)
  */

/** Взято из MurmurHash.
  * Быстрее, чем intHash32 при вставке в хэш-таблицу UInt64 -> UInt64, где ключ - идентификатор посетителя.
  */
inline DB::UInt64 intHash64(DB::UInt64 x)
{
	x ^= x >> 33;
	x *= 0xff51afd7ed558ccdULL;
	x ^= x >> 33;
	x *= 0xc4ceb9fe1a85ec53ULL;
	x ^= x >> 33;

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
