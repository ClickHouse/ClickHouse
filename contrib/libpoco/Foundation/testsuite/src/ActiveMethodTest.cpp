//
// ActiveMethodTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ActiveMethodTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ActiveMethodTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/ActiveMethod.h"
#include "Poco/Thread.h"
#include "Poco/Event.h"
#include "Poco/Exception.h"


using Poco::ActiveMethod;
using Poco::ActiveResult;
using Poco::Thread;
using Poco::Event;
using Poco::Exception;


namespace
{
	class ActiveObject
	{
	public:
		typedef ActiveMethod<int, int, ActiveObject>   IntIntType;
		typedef ActiveMethod<void, int, ActiveObject>  VoidIntType;
		typedef ActiveMethod<void, void, ActiveObject> VoidVoidType;
		typedef ActiveMethod<int, void, ActiveObject>  IntVoidType;

		ActiveObject():
			testMethod(this, &ActiveObject::testMethodImpl),
			testVoid(this,&ActiveObject::testVoidOutImpl),
			testVoidInOut(this,&ActiveObject::testVoidInOutImpl),
			testVoidIn(this,&ActiveObject::testVoidInImpl)
		{
		}
		
		~ActiveObject()
		{
		}
		
		IntIntType testMethod;

		VoidIntType testVoid;

		VoidVoidType testVoidInOut;

		IntVoidType testVoidIn;
		
		void cont()
		{
			_continue.set();
		}
		
	protected:
		int testMethodImpl(const int& n)
		{
			if (n == 100) throw Exception("n == 100");
			_continue.wait();
			return n;
		}

		void testVoidOutImpl(const int& n)
		{
			if (n == 100) throw Exception("n == 100");
			_continue.wait();
		}

		void testVoidInOutImpl()
		{
			_continue.wait();
		}

		int testVoidInImpl()
		{
			_continue.wait();
			return 123;
		}
		
	private:
		Event _continue;
	};
}


ActiveMethodTest::ActiveMethodTest(const std::string& name): CppUnit::TestCase(name)
{
}


ActiveMethodTest::~ActiveMethodTest()
{
}


void ActiveMethodTest::testWait()
{
	ActiveObject activeObj;
	ActiveResult<int> result = activeObj.testMethod(123);
	assert (!result.available());
	activeObj.cont();
	result.wait();
	assert (result.available());
	assert (result.data() == 123);
	assert (!result.failed());
}


void ActiveMethodTest::testCopy()
{
	ActiveObject activeObj;

	ActiveObject::IntIntType ii = activeObj.testMethod;
	ActiveResult<int> rii = ii(123);
	assert (!rii.available());
	activeObj.cont();
	rii.wait();
	assert (rii.available());
	assert (rii.data() == 123);
	assert (!rii.failed());

	ActiveObject::VoidIntType  vi = activeObj.testVoid;
	ActiveResult<void> rvi = vi(123);
	assert (!rvi.available());
	activeObj.cont();
	rvi.wait();
	assert (rvi.available());
	assert (!rvi.failed());

	ActiveObject::VoidVoidType vv = activeObj.testVoidInOut;
	ActiveResult<void> rvv = vv();
	assert (!rvv.available());
	activeObj.cont();
	rvv.wait();
	assert (rvv.available());
	assert (!rvv.failed());

	ActiveObject::IntVoidType  iv = activeObj.testVoidIn;
	ActiveResult<int> riv = iv();
	assert (!riv.available());
	activeObj.cont();
	riv.wait();
	assert (riv.available());
	assert (riv.data() == 123);
	assert (!riv.failed());
}


void ActiveMethodTest::testWaitInterval()
{
	ActiveObject activeObj;
	ActiveResult<int> result = activeObj.testMethod(123);
	assert (!result.available());
	try
	{
		result.wait(100);
		fail("wait must fail");
	}
	catch (Exception&)
	{
	}
	activeObj.cont();
	result.wait(10000);
	assert (result.available());
	assert (result.data() == 123);
	assert (!result.failed());
}


void ActiveMethodTest::testTryWait()
{
	ActiveObject activeObj;
	ActiveResult<int> result = activeObj.testMethod(123);
	assert (!result.available());
	assert (!result.tryWait(200));
	activeObj.cont();
	assert (result.tryWait(10000));
	assert (result.available());
	assert (result.data() == 123);
	assert (!result.failed());
}


void ActiveMethodTest::testFailure()
{
	ActiveObject activeObj;
	ActiveResult<int> result = activeObj.testMethod(100);
	result.wait();
	assert (result.available());
	assert (result.failed());
	std::string msg = result.error();
	assert (msg == "n == 100");
}


void ActiveMethodTest::testVoidOut()
{
	ActiveObject activeObj;
	ActiveResult<void> result = activeObj.testVoid(101);
	activeObj.cont();
	result.wait();
	assert (result.available());
	assert (!result.failed());
}


void ActiveMethodTest::testVoidInOut()
{
	ActiveObject activeObj;
	ActiveResult<void> result = activeObj.testVoidInOut();
	activeObj.cont();
	result.wait();
	assert (result.available());
	assert (!result.failed());
}


void ActiveMethodTest::testVoidIn()
{
	ActiveObject activeObj;
	ActiveResult<int> result = activeObj.testVoidIn();
	activeObj.cont();
	result.wait();
	assert (result.available());
	assert (!result.failed());
	assert (result.data() == 123);
}


void ActiveMethodTest::setUp()
{
}


void ActiveMethodTest::tearDown()
{
}


CppUnit::Test* ActiveMethodTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ActiveMethodTest");

	CppUnit_addTest(pSuite, ActiveMethodTest, testWait);
	CppUnit_addTest(pSuite, ActiveMethodTest, testCopy);
	CppUnit_addTest(pSuite, ActiveMethodTest, testWaitInterval);
	CppUnit_addTest(pSuite, ActiveMethodTest, testTryWait);
	CppUnit_addTest(pSuite, ActiveMethodTest, testFailure);
	CppUnit_addTest(pSuite, ActiveMethodTest, testVoidOut);
	CppUnit_addTest(pSuite, ActiveMethodTest, testVoidIn);
	CppUnit_addTest(pSuite, ActiveMethodTest, testVoidInOut);

	return pSuite;
}
