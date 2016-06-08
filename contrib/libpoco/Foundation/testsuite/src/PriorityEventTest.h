//
// PriorityEventTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/PriorityEventTest.h#1 $
//
// Definition of the PriorityEventTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PriorityEventTest_INCLUDED
#define PriorityEventTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"
#include "Poco/PriorityEvent.h"
#include "Poco/EventArgs.h"


class PriorityEventTest: public CppUnit::TestCase
{
	Poco::PriorityEvent<void> Void;
	Poco::PriorityEvent<int> Simple;
	Poco::PriorityEvent<const int> ConstSimple;
	Poco::PriorityEvent<Poco::EventArgs*> Complex;
	Poco::PriorityEvent<Poco::EventArgs> Complex2;
	Poco::PriorityEvent<const Poco::EventArgs*> ConstComplex;
	Poco::PriorityEvent<const Poco::EventArgs * const> Const2Complex;
public:
	PriorityEventTest(const std::string& name);
	~PriorityEventTest();

	void testNoDelegate();
	void testSingleDelegate();
	void testDuplicateRegister();
	void testDuplicateUnregister();
	void testDisabling();
	void testPriorityOrder();
	void testPriorityOrderExpire();
	void testExpire();
	void testExpireReRegister();
	void testReturnParams();
	void testOverwriteDelegate();
	void testAsyncNotify();

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


#endif // PriorityEventTest_INCLUDED
