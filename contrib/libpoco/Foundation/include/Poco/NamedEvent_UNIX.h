//
// NamedEvent_UNIX.h
//
// $Id: //poco/1.4/Foundation/include/Poco/NamedEvent_UNIX.h#1 $
//
// Library: Foundation
// Package: Processes
// Module:  NamedEvent
//
// Definition of the NamedEventImpl class for Unix.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NamedEvent_UNIX_INCLUDED
#define Foundation_NamedEvent_UNIX_INCLUDED


#include "Poco/Foundation.h"
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
#include <semaphore.h>
#endif


namespace Poco {


class Foundation_API NamedEventImpl
{
protected:
	NamedEventImpl(const std::string& name);	
	~NamedEventImpl();
	void setImpl();
	void waitImpl();
	
private:
	std::string getFileName();

	std::string _name;
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	sem_t* _sem;
#else
	int _lockfd; // lock file descriptor
	int _semfd;  // file used to identify semaphore
	int _semid;  // semaphore id
#endif
};


} // namespace Poco


#endif // Foundation_NamedEvent_UNIX_INCLUDED
