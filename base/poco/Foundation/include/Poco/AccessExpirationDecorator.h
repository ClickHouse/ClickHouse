//
// AccessExpirationDecorator.h
//
// Library: Foundation
// Package: Cache
// Module:  AccessExpirationDecorator
//
// Implementation of the AccessExpirationDecorator template.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_AccessExpirationDecorator_INCLUDED
#define Foundation_AccessExpirationDecorator_INCLUDED


#include "Poco/Timestamp.h"
#include "Poco/Timespan.h"


namespace Poco {


template <typename TArgs>
class AccessExpirationDecorator
	/// AccessExpirationDecorator adds an expiration method to values so that they can be used
	/// with the UniqueAccessExpireCache
{
public:
	AccessExpirationDecorator():
		_value(),
		_span()
	{
	}

	AccessExpirationDecorator(const TArgs& p, const Poco::Timespan::TimeDiff& diffInMs):
			/// Creates an element that will expire in diff milliseconds
		_value(p),
		_span(diffInMs*1000)
	{
	}

	AccessExpirationDecorator(const TArgs& p, const Poco::Timespan& timeSpan):
		/// Creates an element that will expire after the given timeSpan
		_value(p),
		_span(timeSpan)
	{
	}


	~AccessExpirationDecorator()
	{
	}
	
	const Poco::Timespan& getTimeout() const
	{
		return _span;
	}

	const TArgs& value() const
	{
		return _value;
	}

	TArgs& value()
	{
		return _value;
	}

private:
	TArgs     _value;
	Timespan  _span;
};


} // namespace Poco


#endif // Foundation_AccessExpirationDecorator_INCLUDED
