//
// ThreadTarget.h
//
// $Id: ThreadTarget.h 1327 2010-02-09 13:56:31Z obiltschnig $
//
// Library: Foundation
// Package: Threading
// Module:  ThreadTarget
//
// Definition of the ThreadTarget class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ThreadTarget_INCLUDED
#define Foundation_ThreadTarget_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Runnable.h"


namespace Poco {


class Foundation_API ThreadTarget: public Runnable
	/// This adapter simplifies using static member functions as well as 
	/// standalone functions as targets for threads.
	/// Note that it is possible to pass those entities directly to Thread::start().
	/// This adapter is provided as a convenience for higher abstraction level
	/// scenarios where Runnable abstract class is used.
	///
	/// For using a non-static member function as a thread target, please
	/// see the RunnableAdapter class.
	/// 
	/// Usage:
	///    class MyObject
	///    {
	///        static void doSomething() {}
	///    };
	///    ThreadTarget ra(&MyObject::doSomething);
	///    Thread thr;
	///    thr.start(ra);
	///
	/// or:
	/// 
	///    void doSomething() {}
	/// 
	///    ThreadTarget ra(doSomething);
	///    Thread thr;
	///    thr.start(ra);
{
public:
	typedef void (*Callback)();
	
	ThreadTarget(Callback method);
	
	ThreadTarget(const ThreadTarget& te);

	~ThreadTarget();

	ThreadTarget& operator = (const ThreadTarget& te);

	void run();
	
private:
	ThreadTarget();

	Callback _method;
};


//
// inlines
//
inline void ThreadTarget::run()
{
	_method();
}


} // namespace Poco


#endif // Foundation_ThreadTarget_INCLUDED
