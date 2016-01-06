//
// FIFOEventTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/FIFOEventTest.h#1 $
//
// Definition of the FIFOEventTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef FIFOEventTest_INCLUDED
#define FIFOEventTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"
#include "Poco/FIFOEvent.h"
#include "Poco/EventArgs.h"


class FIFOEventTest: public CppUnit::TestCase
{
	Poco::FIFOEvent<void> Void;
	Poco::FIFOEvent<int> Simple;
	Poco::FIFOEvent<const int> ConstSimple;
	Poco::FIFOEvent<Poco::EventArgs*> Complex;
	Poco::FIFOEvent<Poco::EventArgs> Complex2;
	Poco::FIFOEvent<const Poco::EventArgs*> ConstComplex;
	Poco::FIFOEvent<const Poco::EventArgs * const> Const2Complex;
public:
	FIFOEventTest(const std::string& name);
	~FIFOEventTest();

	void testNoDelegate();
	void testSingleDelegate();
	void testDuplicateRegister();
	void testDuplicateUnregister();
	void testDisabling();
	void testFIFOOrder();
	void testFIFOOrderExpire();
	void testExpire();
	void testExpireReRegister();
	void testReturnParams();
	void testOverwriteDelegate();
	void testAsyncNotify();

	void setUp();
	void tearDown();
	static CppUnit::Test* suite();

protected:
	void onVoid(const void* pSender);
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


#endif // FIFOEventTest_INCLUDED
