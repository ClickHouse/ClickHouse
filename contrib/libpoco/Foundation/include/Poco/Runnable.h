//
// Runnable.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Runnable.h#1 $
//
// Library: Foundation
// Package: Threading
// Module:  Thread
//
// Definition of the Runnable class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Runnable_INCLUDED
#define Foundation_Runnable_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class Foundation_API Runnable
	/// The Runnable interface with the run() method
	/// must be implemented by classes that provide
	/// an entry point for a thread.
{
public:	
	Runnable();
	virtual ~Runnable();
	
	virtual void run() = 0;
		/// Do whatever the thread needs to do. Must
		/// be overridden by subclasses.
};


} // namespace Poco


#endif // Foundation_Runnable_INCLUDED
