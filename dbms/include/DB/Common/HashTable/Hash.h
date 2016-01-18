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

/** CRC32C является не очень качественной в роли хэш функции,
  *  согласно avalanche и bit independence тестам, а также малым количеством бит,
  *  но может вести себя хорошо при использовании в хэш-таблицах,
  *  за счёт высокой скорости (latency 3 + 1 такт, througput 1 такт).
  * Работает только при поддержке SSE 4.2.
  * Используется asm вместо интринсика, чтобы не обязательно было собирать весь проект с -msse4.
  */
inline DB::UInt64 intHashCRC32(DB::UInt64 x)
{
#if defined(__x86_64__)
	DB::UInt64 crc = -1ULL;
	asm("crc32q %[x], %[crc]\n" : [crc] "+r" (crc) : [x] "rm" (x));
	return crc;
#else
	/// На других платформах используем не обязательно CRC32. NOTE Это может сбить с толку.
	return intHash64(x);
#endif
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

#define DEFINE_HASH(T) \
template <> struct DefaultHash<T>\
{\
	size_t operator() (T key) const\
	{\
		return DefaultHash64<T>(key);\
	}\
};

DEFINE_HASH(DB::UInt8)
DEFINE_HASH(DB::UInt16)
DEFINE_HASH(DB::UInt32)
DEFINE_HASH(DB::UInt64)
DEFINE_HASH(DB::Int8)
DEFINE_HASH(DB::Int16)
DEFINE_HASH(DB::Int32)
DEFINE_HASH(DB::Int64)
DEFINE_HASH(DB::Float32)
DEFINE_HASH(DB::Float64)

#undef DEFINE_HASH


template <typename T> struct HashCRC32;

template <typename T>
inline size_t hashCRC32(T key)
{
	union
	{
		T in;
		DB::UInt64 out;
	} u;
	u.out = 0;
	u.in = key;
	return intHashCRC32(u.out);
}

#define DEFINE_HASH(T) \
template <> struct HashCRC32<T>\
{\
	size_t operator() (T key) const\
	{\
		return hashCRC32<T>(key);\
	}\
};

DEFINE_HASH(DB::UInt8)
DEFINE_HASH(DB::UInt16)
DEFINE_HASH(DB::UInt32)
DEFINE_HASH(DB::UInt64)
DEFINE_HASH(DB::Int8)
DEFINE_HASH(DB::Int16)
DEFINE_HASH(DB::Int32)
DEFINE_HASH(DB::Int64)
DEFINE_HASH(DB::Float32)
DEFINE_HASH(DB::Float64)

#undef DEFINE_HASH


/// Разумно использовать для UInt8, UInt16 при достаточном размере хэш-таблицы.
struct TrivialHash
{
	template <typename T>
	size_t operator() (T key) const
	{
		return key;
	}
};


/** Сравнительно неплохая некриптографическая хэш функция из UInt64 в UInt32.
  * Но хуже (и по качеству и по скорости), чем просто срезка intHash64.
  * Взята отсюда: http://www.concentric.net/~ttwang/tech/inthash.htm
  *
  * Немного изменена по сравнению с функцией по ссылке: сдвиги вправо случайно заменены на цикличесвие сдвиги вправо.
  * Это изменение никак не повлияло на результаты тестов smhasher.
  *
  * Рекомендуется для разных задач использовать разные salt.
  * А то был случай, что в БД значения сортировались по хэшу (для некачественного псевдослучайного разбрасывания),
  *  а в другом месте, в агрегатной функции, в хэш таблице использовался такой же хэш,
  *  в результате чего, эта агрегатная функция чудовищно тормозила из-за коллизий.
  */
template <DB::UInt64 salt>
inline DB::UInt32 intHash32(DB::UInt64 key)
{
	key ^= salt;

	key = (~key) + (key << 18);
	key = key ^ ((key >> 31) | (key << 33));
	key = key * 21;
	key = key ^ ((key >> 11) | (key << 53));
	key = key + (key << 6);
	key = key ^ ((key >> 22) | (key << 42));

	return key;
}


/// Для контейнеров.
template <typename T, DB::UInt64 salt = 0>
struct IntHash32
{
	size_t operator() (const T & key) const
	{
		return intHash32<salt>(key);
	}
};
