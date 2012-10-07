#pragma once

#include <city.h>
#include <openssl/md5.h>

#include <DB/Core/Row.h>
#include <DB/Core/StringRef.h>
#include <DB/Columns/IColumn.h>
#include <DB/Interpreters/HashMap.h>


namespace DB
{


/// Для агрегации по md5.
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


class FieldVisitorHash : public boost::static_visitor<>
{
public:
	MD5_CTX state;
	
	FieldVisitorHash()
	{
		MD5_Init(&state);
	}

	void finalize(unsigned char * place)
	{
		MD5_Final(place, &state);
	}

	void operator() (const Null 	& x)
	{
		char type = FieldType::Null;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
	}
	
	void operator() (const UInt64 	& x)
	{
		char type = FieldType::UInt64;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		MD5_Update(&state, reinterpret_cast<const char *>(&x), sizeof(x));
	}
	
 	void operator() (const Int64 	& x)
	{
		char type = FieldType::Int64;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		MD5_Update(&state, reinterpret_cast<const char *>(&x), sizeof(x));
	}

	void operator() (const Float64 	& x)
	{
		char type = FieldType::Float64;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		MD5_Update(&state, reinterpret_cast<const char *>(&x), sizeof(x));
	}

	void operator() (const String 	& x)
	{
		char type = FieldType::String;
		MD5_Update(&state, reinterpret_cast<const char *>(&type), sizeof(type));
		/// Используем ноль на конце.
		MD5_Update(&state, x.c_str(), x.size() + 1);
	}

	void operator() (const Array 	& x)
	{
		throw Exception("Cannot aggregate by array", ErrorCodes::ILLEGAL_KEY_OF_AGGREGATION);
	}

	void operator() (const SharedPtr<IAggregateFunction> & x)
	{
		throw Exception("Cannot aggregate by state of aggregate function", ErrorCodes::ILLEGAL_KEY_OF_AGGREGATION);
	}
};


/** Преобразование значения в 64 бита. Для чисел - однозначное, для строк - некриптографический хэш. */
class FieldVisitorToUInt64 : public boost::static_visitor<UInt64>
{
public:
	FieldVisitorToUInt64() {}
	
	UInt64 operator() (const Null 		& x) const { return 0; }
	UInt64 operator() (const UInt64 	& x) const { return x; }
	UInt64 operator() (const Int64 		& x) const { return x; }

	UInt64 operator() (const Float64 	& x) const
	{
		UInt64 res = 0;
		memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<const char *>(&x), sizeof(x));
		return res;
	}

	UInt64 operator() (const String 	& x) const
	{
		return CityHash64(x.data(), x.size());
	}

	UInt64 operator() (const Array 	& x) const
	{
		throw Exception("Cannot aggregate by array", ErrorCodes::ILLEGAL_KEY_OF_AGGREGATION);
	}

	UInt64 operator() (const SharedPtr<IAggregateFunction> & x) const
	{
		throw Exception("Cannot aggregate by state of aggregate function", ErrorCodes::ILLEGAL_KEY_OF_AGGREGATION);
	}
};


typedef std::vector<size_t> Sizes;


/// Записать набор ключей в UInt128. Либо уложив их подряд, либо вычислив md5.
inline UInt128 __attribute__((__always_inline__)) pack128(
	size_t i, bool keys_fit_128_bits, size_t keys_size, Row & key, const Columns & key_columns, const Sizes & key_sizes)
{
	const FieldVisitorToUInt64 to_uint64_visitor;
	
	union
	{
		UInt128 key_hash;
		unsigned char bytes[16];
	} key_hash_union;

	/// Если все ключи числовые и помещаются в 128 бит
	if (keys_fit_128_bits)
	{
		memset(key_hash_union.bytes, 0, 16);
		size_t offset = 0;
		for (size_t j = 0; j < keys_size; ++j)
		{
			key[j] = (*key_columns[j])[i];
			UInt64 tmp = boost::apply_visitor(to_uint64_visitor, key[j]);
			/// Работает только на little endian
			memcpy(key_hash_union.bytes + offset, reinterpret_cast<const char *>(&tmp), key_sizes[j]);
			offset += key_sizes[j];
		}
	}
	else	/// Иначе используем md5.
	{
		FieldVisitorHash key_hash_visitor;

		for (size_t j = 0; j < keys_size; ++j)
		{
			key[j] = (*key_columns[j])[i];
			boost::apply_visitor(key_hash_visitor, key[j]);
		}

		key_hash_visitor.finalize(key_hash_union.bytes);
	}

	return key_hash_union.key_hash;
}


}
