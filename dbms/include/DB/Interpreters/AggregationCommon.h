#pragma once

#include <city.h>
#include <openssl/md5.h>

#include <DB/Common/SipHash.h>
#include <DB/Core/Row.h>
#include <DB/Core/StringRef.h>
#include <DB/Columns/IColumn.h>
#include <DB/Interpreters/HashMap.h>


namespace DB
{


/// Для агрегации по SipHash.
struct UInt128
{
	UInt64 first;
	UInt64 second;

	bool operator== (const UInt128 rhs) const { return first == rhs.first && second == rhs.second; }
	bool operator!= (const UInt128 rhs) const { return first != rhs.first || second != rhs.second; }
};

struct UInt128Hash
{
	default_hash<UInt64> hash64;
	size_t operator()(UInt128 x) const { return hash64(x.first ^ 0xB15652B8790A0D36ULL) ^ hash64(x.second); }
};

struct UInt128ZeroTraits
{
	static inline bool check(UInt128 x) { return x.first == 0 && x.second == 0; }
	static inline void set(UInt128 & x) { x.first = 0; x.second = 0; }
};


/// Немного быстрее стандартного
struct StringHash
{
	size_t operator()(const String & x) const { return CityHash64(x.data(), x.size()); }
};


typedef std::vector<size_t> Sizes;


/// Записать набор ключей в UInt128. Либо уложив их подряд, либо вычислив SipHash.
inline UInt128 __attribute__((__always_inline__)) pack128(
	size_t i, bool keys_fit_128_bits, size_t keys_size, Row & key, const ConstColumnPlainPtrs & key_columns, const Sizes & key_sizes)
{
	union
	{
		UInt128 key_hash;
		char bytes[16];
	} key_hash_union;

	/// Если все ключи числовые и помещаются в 128 бит
	if (keys_fit_128_bits)
	{
		memset(key_hash_union.bytes, 0, 16);
		size_t offset = 0;
		for (size_t j = 0; j < keys_size; ++j)
		{
			key_columns[j]->get(i, key[j]);
			StringRef key_data = key_columns[j]->getDataAt(i);
			memcpy(key_hash_union.bytes + offset, key_data.data, key_sizes[j]);
			offset += key_sizes[j];
		}
	}
	else	/// Иначе используем SipHash.
	{
		SipHash hash;

		for (size_t j = 0; j < keys_size; ++j)
		{
			key_columns[j]->get(i, key[j]);
			StringRef key_data = key_columns[j]->getDataAtWithTerminatingZero(i);
			hash.update(key_data.data, key_data.size);
		}

		hash.final(key_hash_union.bytes);
	}

	return key_hash_union.key_hash;
}

/// То же самое, но не формирует ключ.
inline UInt128 __attribute__((__always_inline__)) pack128(
	size_t i, bool keys_fit_128_bits, size_t keys_size, const ConstColumnPlainPtrs & key_columns, const Sizes & key_sizes)
{
	union
	{
		UInt128 key_hash;
		char bytes[16];
	} key_hash_union;

	/// Если все ключи числовые и помещаются в 128 бит
	if (keys_fit_128_bits)
	{
		memset(key_hash_union.bytes, 0, 16);
		size_t offset = 0;
		for (size_t j = 0; j < keys_size; ++j)
		{
			StringRef key_data = key_columns[j]->getDataAt(i);
			memcpy(key_hash_union.bytes + offset, key_data.data, key_sizes[j]);
			offset += key_sizes[j];
		}
	}
	else	/// Иначе используем SipHash.
	{
		SipHash hash;

		for (size_t j = 0; j < keys_size; ++j)
		{
			StringRef key_data = key_columns[j]->getDataAtWithTerminatingZero(i);
			hash.update(key_data.data, key_data.size);
		}

		hash.final(key_hash_union.bytes);
	}

	return key_hash_union.key_hash;
}


}
