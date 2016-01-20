//
// RandomStreamTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/RandomStreamTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "RandomStreamTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/RandomStream.h"
#include <vector>
#include <cmath>


using Poco::RandomInputStream;


RandomStreamTest::RandomStreamTest(const std::string& name): CppUnit::TestCase(name)
{
}


RandomStreamTest::~RandomStreamTest()
{
}


void RandomStreamTest::testStream()
{
	RandomInputStream rnd;
	
	const int n = 16;
	std::vector<int> d(n, 0);
	for (int i = 0; i < 1000; ++i)
	{
		unsigned char c;
		rnd >> c;
		d[c & 0x0F]++;
		d[(c >> 4) & 0x0F]++;
	}
	int sum = 0;
	for (int k = 0; k < n; ++k) sum += d[k];
	int avg = sum/n;
	int var = 0;
	for (int k = 0; k < n; ++k) var += (d[k] - avg)*(d[k] - avg);
	var /= n;
	int sd = int(std::sqrt((double) var));
	
	assert (110 < avg && avg < 140);
	assert (sd < 20);
}


void RandomStreamTest::setUp()
{
}


void RandomStreamTest::tearDown()
{
}


CppUnit::Test* RandomStreamTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("RandomStreamTest");

	CppUnit_addTest(pSuite, RandomStreamTest, testStream);

	return pSuite;
}
