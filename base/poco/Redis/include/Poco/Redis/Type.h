//
// Type.h
//
// Library: Redis
// Package: Redis
// Module:  Type
//
// Definition of the Type class.
//
// Copyright (c) 2016, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Redis_Type_INCLUDED
#define Redis_Type_INCLUDED


#include "Poco/LineEndingConverter.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NumberParser.h"
#include "Poco/SharedPtr.h"
#include "Poco/Nullable.h"
#include "Poco/Redis/Redis.h"
#include "Poco/Redis/RedisStream.h"


namespace Poco {
namespace Redis {


class Redis_API RedisType
	/// Base class for all Redis types. This class makes it possible to store
	/// element with different types in Array.
{
public:
	enum Types 
	{
		REDIS_INTEGER,       /// Redis Integer
		REDIS_SIMPLE_STRING, /// Redis Simple String
		REDIS_BULK_STRING,   /// Redis Bulkstring
		REDIS_ARRAY,         /// Redis Array
		REDIS_ERROR          /// Redis Error
	};

	typedef SharedPtr<RedisType> Ptr;

	RedisType();
		/// Creates the RedisType.

	virtual ~RedisType();
		/// Destroys the RedisType.

	bool isArray() const;
		/// Returns true when the value is a Redis array.

	bool isBulkString() const;
		/// Returns true when the value is a Redis bulkstring.

	bool isError() const;
		/// Returns true when the value is a Redis error.

	bool isInteger() const;
		/// Returns true when the value is a Redis integer (64 bit integer).

	bool isSimpleString() const;
		/// Returns true when the value is a simple string.

	virtual int type() const = 0;
		/// Returns the type of the value.

	virtual void read(RedisInputStream& input) = 0;
		/// Reads the value from the stream.

	virtual std::string toString() const = 0;
		/// Converts the value to a RESP (REdis Serialization Protocol) string.

	static RedisType::Ptr createRedisType(char marker);
		/// Create a Redis type based on the marker:
		///
		///     - '+': a simple string (std::string)
		///     - '-': an error (Error)
		///     - '$': a bulk string (BulkString)
		///     - '*': an array (Array)
		///     - ':': a signed 64 bit integer (Int64)
};


//
// inlines
//


inline bool RedisType::isArray() const
{
	return type() == REDIS_ARRAY;
}


inline bool RedisType::isBulkString() const
{
	return type() == REDIS_BULK_STRING;
}


inline bool RedisType::isError() const
{
	return type() == REDIS_ERROR;
}


inline bool RedisType::isInteger() const
{
	return type() == REDIS_INTEGER;
}


inline bool RedisType::isSimpleString() const
{
	return type() == REDIS_SIMPLE_STRING;
}


template<typename T>
struct RedisTypeTraits
{
};


template<>
struct RedisTypeTraits<Int64>
{
	enum 
	{
		TypeId = RedisType::REDIS_INTEGER 
	};

	static const char marker = ':';

	static std::string toString(const Int64& value)
	{
		return marker + NumberFormatter::format(value) + "\r\n";
	}

	static void read(RedisInputStream& input, Int64& value)
	{
		std::string number = input.getline();
		value = NumberParser::parse64(number);
	}
};


template<>
struct RedisTypeTraits<std::string>
{
	enum 
	{ 
		TypeId = RedisType::REDIS_SIMPLE_STRING 
	};

	static const char marker = '+';

	static std::string toString(const std::string& value)
	{
		return marker + value + LineEnding::NEWLINE_CRLF;
	}

	static void read(RedisInputStream& input, std::string& value)
	{
		value = input.getline();
	}
};


typedef Nullable<std::string> BulkString;
	/// A bulk string is a string that can contain a NULL value.
	/// So, BulkString is a typedef for Nullable<std::string>.


template<>
struct RedisTypeTraits<BulkString>
{
	enum 
	{ 
		TypeId = RedisType::REDIS_BULK_STRING 
	};

	static const char marker = '$';

	static std::string toString(const BulkString& value)
	{
		if ( value.isNull() )
		{
			return marker + std::string("-1") + LineEnding::NEWLINE_CRLF;
		}
		else
		{
			std::string s = value.value();
			return marker
				+ NumberFormatter::format(s.length())
				+ LineEnding::NEWLINE_CRLF
				+ s
				+ LineEnding::NEWLINE_CRLF;
		}
	}

	static void read(RedisInputStream& input, BulkString& value)
	{
		value.clear();

		std::string line = input.getline();
		int length = NumberParser::parse(line);

		if ( length >= 0 )
		{
			std::string s;
			s.resize(length, ' ');
			input.read(&*s.begin(), length);
			value.assign(s);

			input.getline(); // Read and ignore /r/n
		}
	}
};


template<typename T>
class Type: public RedisType
	/// Template class for all Redis types. This class will use
	/// RedisTypeTraits structure for calling the type specific code.
{
public:
	Type()
		/// Creates the Type.
	{
	}

	Type(const T& t) : _value(t)
		/// Creates the Type from another one.
	{
	}

	Type(const Type& copy) : _value(copy._value)
		/// Creates the Type by copying another one.
	{
	}

	virtual ~Type()
		/// Destroys the Type.
	{
	}

	int type() const
		/// Returns the type of the value
	{
		return RedisTypeTraits<T>::TypeId;
	}

	void read(RedisInputStream& socket)
		/// Reads the value from the stream (RESP).
	{
		RedisTypeTraits<T>::read(socket, _value);
	}

	std::string toString() const
		/// Converts the value to a string based on the RESP protocol.
	{
		return RedisTypeTraits<T>::toString(_value);
	}

	T& value()
		/// Returns the value
	{
		return _value;
	}

	const T& value() const
		/// Returns a const value
	{
		return _value;
	}

private:
	T _value;
};


} } // namespace Poco/Redis


#endif // Redis_Type_INCLUDED
