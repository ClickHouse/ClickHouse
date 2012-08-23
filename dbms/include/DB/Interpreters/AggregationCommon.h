#include <openssl/md5.h>

#include <DB/Core/Field.h>


namespace DB
{

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
		return std::tr1::hash<String>()(x);
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


}
