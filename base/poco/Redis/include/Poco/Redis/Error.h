//
// Error.h
//
// Library: Redis
// Package: Redis
// Module:  Error
//
// Definition of the Error class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Redis_Error_INCLUDED
#define Redis_Error_INCLUDED


#include "Poco/Redis/Type.h"


namespace Poco {
namespace Redis {


class Redis_API Error
	/// Represent a Redis error.
{
public:
	Error();
		/// Creates an empty Error.

	Error(const std::string& message);
		/// Creates an Error with the given message.

	virtual ~Error();
		/// Destroys the Error.

	const std::string& getMessage() const;
		/// Returns the error message.

	void setMessage(const std::string& message);
		/// Sets the error message.

private:
	std::string _message;
};


//
// inlines
//


inline const std::string& Error::getMessage() const
{
	return _message;
}


inline void Error::setMessage(const std::string& message)
{
	_message = message;
}


template<>
struct RedisTypeTraits<Error>
{
	enum { TypeId = RedisType::REDIS_ERROR };

	static const char marker = '-';

	static std::string toString(const Error& value)
	{
		return marker + value.getMessage()  + LineEnding::NEWLINE_CRLF;
	}

	static void read(RedisInputStream& input, Error& value)
	{
		value.setMessage(input.getline());
	}
};


} } // namespace Poco::Redis


#endif // Redis_Error_INCLUDED
