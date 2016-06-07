//
// FPETest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/FPETest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "FPETest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/FPEnvironment.h"


using Poco::FPE;


FPETest::FPETest(const std::string& name): CppUnit::TestCase(name)
{
}


FPETest::~FPETest()
{
}


void FPETest::testClassify()
{
	{
		float a = 0.0f;
		float b = 0.0f;
		float nan = a/b;
		float inf = 1.0f/b;
		
		assert (FPE::isNaN(nan));
		assert (!FPE::isNaN(a));
		assert (FPE::isInfinite(inf));
		assert (!FPE::isInfinite(a));
	}
	{
		double a = 0;
		double b = 0;
		double nan = a/b;
		double inf = 1.0/b;
		
		assert (FPE::isNaN(nan));
		assert (!FPE::isNaN(a));
		assert (FPE::isInfinite(inf));
		assert (!FPE::isInfinite(a));
	}
}


#if defined(__HP_aCC)
	#pragma OPTIMIZE OFF
#elif defined(_MSC_VER)
	#pragma optimize("", off)
#elif defined(__APPLE__) && defined(POCO_COMPILER_GCC)
	#pragma GCC optimization_level 0
#endif


double mult(double a, double b)
{
	return a*b;
}


double div(double a, double b)
{
	return a/b;
}


void FPETest::testFlags()
{
	FPE::clearFlags();

	// some compilers are intelligent enough to optimize the calculations below away.
	// unfortunately this leads to a failing test, so we have to trick out the
	// compiler's optimizer a little bit by doing something with the results.
	volatile double a = 10;
	volatile double b = 0;
	volatile double c = div(a, b);

	assert (FPE::isFlag(FPE::FP_DIVIDE_BY_ZERO));
	assert (FPE::isInfinite(c)); 

	FPE::clearFlags();
	a = 1.23456789e210;
	b = 9.87654321e210;
	c = mult(a, b);
	assert (FPE::isFlag(FPE::FP_OVERFLOW));
	assertEqualDelta(c, c, 0);

	FPE::clearFlags();
	a = 1.23456789e-99;
	b = 9.87654321e210;
	c = div(a, b);	
	assert (FPE::isFlag(FPE::FP_UNDERFLOW));
	assertEqualDelta(c, c, 0);
}


#if defined(__HP_aCC)
	#pragma OPTIMIZE ON
#elif defined(_MSC_VER)
	#pragma optimize("", on)
#elif defined(__APPLE__) && defined(POCO_COMPILER_GCC)
	#pragma GCC optimization_level reset
#endif


void FPETest::testRound()
{
#if !defined(__osf__) && !defined(__VMS)
	FPE::setRoundingMode(FPE::FP_ROUND_TONEAREST);			
	assert (FPE::getRoundingMode() == FPE::FP_ROUND_TONEAREST);
	{
		FPE env(FPE::FP_ROUND_TOWARDZERO);
		assert (FPE::getRoundingMode() == FPE::FP_ROUND_TOWARDZERO);
	}
	assert (FPE::getRoundingMode() == FPE::FP_ROUND_TONEAREST);	
#endif
}


void FPETest::setUp()
{
}


void FPETest::tearDown()
{
}


CppUnit::Test* FPETest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("FPETest");

	CppUnit_addTest(pSuite, FPETest, testClassify);
	CppUnit_addTest(pSuite, FPETest, testFlags);
	CppUnit_addTest(pSuite, FPETest, testRound);

	return pSuite;
}
