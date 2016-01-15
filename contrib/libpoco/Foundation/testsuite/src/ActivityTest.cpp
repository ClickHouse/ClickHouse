//
// ActivityTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ActivityTest.cpp#2 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ActivityTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Activity.h"
#include "Poco/Thread.h"


using Poco::Activity;
using Poco::Thread;


namespace
{
	class ActiveObject
	{
	public:
		ActiveObject(): 
			_activity(this, &ActiveObject::run),
			_count(0)
		{
		}
		
		~ActiveObject()
		{
		}
		
		Activity<ActiveObject>& activity()
		{
			return _activity;
		}
		
		Poco::UInt64 count() const
		{
			return _count;
		}

	protected:
		void run()
		{
			while (!_activity.isStopped()) 
				++_count;
		}

	private:
		Activity<ActiveObject> _activity;
		Poco::UInt64           _count;
	};
}
 

ActivityTest::ActivityTest(const std::string& name): CppUnit::TestCase(name)
{
}


ActivityTest::~ActivityTest()
{
}


void ActivityTest::testActivity()
{
	ActiveObject activeObj;
	assert (activeObj.activity().isStopped());
	activeObj.activity().start();
	assert (!activeObj.activity().isStopped());
	Thread::sleep(1000);
	assert (activeObj.activity().isRunning());
	activeObj.activity().stop();
	activeObj.activity().wait();
	assert (activeObj.count() > 0);
}


void ActivityTest::setUp()
{
}


void ActivityTest::tearDown()
{
}


CppUnit::Test* ActivityTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ActivityTest");

	CppUnit_addTest(pSuite, ActivityTest, testActivity);

	return pSuite;
}
