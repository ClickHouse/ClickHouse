//
// BasicEventTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/BasicEventTest.h#1 $
//
// Tests for BasicEvent
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef BasicEventTest_INCLUDED
#define BasicEventTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"
#include "Poco/BasicEvent.h"
#include "Poco/EventArgs.h"


class BasicEventTest: public CppUnit::TestCase
{
	Poco::BasicEvent<void> Void;
	Poco::BasicEvent<int> Simple;
	Poco::BasicEvent<const int> ConstSimple;
	Poco::BasicEvent<Poco::EventArgs*> Complex;
	Poco::BasicEvent<Poco::EventArgs> Complex2;
	Poco::BasicEvent<const Poco::EventArgs*> ConstComplex;
	Poco::BasicEvent<const Poco::EventArgs * const> Const2Complex;
public:
	BasicEventTest(const std::string& name);
	~BasicEventTest();

	void testNoDelegate();
	void testSingleDelegate();
	void testDuplicateRegister();
	void testDuplicateUnregister();
	void testDisabling();
	void testExpire();
	void testExpireReRegister();
	void testReturnParams();
	void testOverwriteDelegate();
	void testAsyncNotify();
	void testNullMutex();
	
	void setUp();
	void tearDown();
	static CppUnit::Test* suite();

protected:

	static void onStaticVoid(const void* pSender);

	void onVoid(const void* pSender);

	static void onStaticSimple(const void* pSender, int& i);
	static void onStaticSimple2(void* pSender, int& i);
	static void onStaticSimple3(int& i);

	void onSimpleNoSender(int& i);
	void onSimple(const void* pSender, int& i);
	void onSimpleOther(const void* pSender, int& i);
	void onConstSimple(const void* pSender, const int& i);
	void onComplex(const void* pSender, Poco::EventArgs* & i);
	void onComplex2(const void* pSender, Poco::EventArgs & i);
	void onConstComplex(const void* pSender, const Poco::EventArgs*& i);
	void onConst2Complex(const void* pSender, const Poco::EventArgs * const & i);
	void onAsync(const void* pSender, int& i);

	int getCount() const;
private:
	int		_count;
};


#endif // BasicEventTest_INCLUDED
