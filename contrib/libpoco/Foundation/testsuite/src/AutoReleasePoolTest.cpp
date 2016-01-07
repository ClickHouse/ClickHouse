//
// AutoReleasePoolTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/AutoReleasePoolTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "AutoReleasePoolTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/AutoReleasePool.h"


using Poco::AutoReleasePool;


namespace
{
	class TestObj
	{
	public:
		TestObj(): _rc(1)
		{
			++_count;
		}
				
		void duplicate()
		{
			++_rc;
		}
		
		void release()
		{
			if (--_rc == 0)
				delete this;
		}
		
		int rc() const
		{
			return _rc;
		}
		
		static int count()
		{
			return _count;
		}
		
	protected:
		~TestObj()
		{
			--_count;
		}
		
	private:
		int _rc;
		static int _count;
	};
	
	int TestObj::_count = 0;
}


AutoReleasePoolTest::AutoReleasePoolTest(const std::string& name): CppUnit::TestCase(name)
{
}


AutoReleasePoolTest::~AutoReleasePoolTest()
{
}


void AutoReleasePoolTest::testAutoReleasePool()
{
	AutoReleasePool<TestObj> arp;
	arp.add(new TestObj);
	arp.add(new TestObj);
	assert (TestObj::count() == 2);
	arp.release();
	assert (TestObj::count() == 0);
}


void AutoReleasePoolTest::setUp()
{
}


void AutoReleasePoolTest::tearDown()
{
}


CppUnit::Test* AutoReleasePoolTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("AutoReleasePoolTest");

	CppUnit_addTest(pSuite, AutoReleasePoolTest, testAutoReleasePool);

	return pSuite;
}
