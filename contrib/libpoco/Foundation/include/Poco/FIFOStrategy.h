//
// FIFOStrategy.h
//
// $Id: //poco/1.4/Foundation/include/Poco/FIFOStrategy.h#3 $
//
// Library: Foundation
// Package: Events
// Module:  FIFOStragegy
//
// Implementation of the FIFOStrategy template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_FIFOStrategy_INCLUDED
#define Foundation_FIFOStrategy_INCLUDED


#include "Poco/DefaultStrategy.h"


namespace Poco {


//@ deprecated
template <class TArgs, class TDelegate> 
class FIFOStrategy: public DefaultStrategy<TArgs, TDelegate>
	/// Note: As of release 1.4.2, DefaultStrategy already 
	/// implements FIFO behavior, so this class is provided
	/// for backwards compatibility only.
{
public:
	FIFOStrategy()
	{
	}

	FIFOStrategy(const FIFOStrategy& s):
		DefaultStrategy<TArgs, TDelegate>(s)
	{
	}

	~FIFOStrategy()
	{
	}

	FIFOStrategy& operator = (const FIFOStrategy& s)
	{
		DefaultStrategy<TArgs, TDelegate>::operator = (s);
		return *this;
	}
};


} // namespace Poco


#endif // Foundation_FIFOStrategy_INCLUDED
