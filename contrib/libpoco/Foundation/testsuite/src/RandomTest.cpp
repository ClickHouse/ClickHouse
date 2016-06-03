//
// RandomTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/RandomTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "RandomTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Random.h"
#include <vector>
#include <cmath>


using Poco::UInt32;


RandomTest::RandomTest(const std::string& name): CppUnit::TestCase(name)
{
}


RandomTest::~RandomTest()
{
}


void RandomTest::testSequence1()
{
	Poco::Random rnd1;
	Poco::Random rnd2;
	rnd1.seed(12345);
	rnd2.seed(12345);
	for (int i = 0; i < 100; ++i)
	{
		assert (rnd1.next() == rnd2.next());
	}
}


void RandomTest::testSequence2()
{
	Poco::Random rnd1;
	Poco::Random rnd2;
	rnd1.seed(12345);
	rnd2.seed(54321);
	
	bool equals = true;	
	for (int i = 0; i < 20; ++i)
	{
		if (rnd1.next() != rnd2.next())
		{
			equals = false;
			break;
		}
	}
	assert (!equals);
}


void RandomTest::testDistribution1()
{
	Poco::Random rnd;
	rnd.seed(123456);
	const int n = 11;
	int d[n] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
	for (int i = 0; i < 100; ++i)
	{
		d[rnd.next() % n] = 1;
	}
	int sum = 0;
	for (int k = 0; k < n; ++k) sum += d[k];
	
	assert (sum == n);
}


void RandomTest::testDistribution2()
{
	Poco::Random rnd;
	rnd.seed();
	const int n = 101;
	std::vector<int> d(n, 0);
	for (int i = 0; i < 10000; ++i)
	{
		d[rnd.next(n)]++;
	}
	int sum = 0;
	for (int k = 0; k < n; ++k) sum += d[k];
	int avg = sum/n;
	int var = 0;
	for (int k = 0; k < n; ++k) var += (d[k] - avg)*(d[k] - avg);
	var /= n;
	int sd = int(std::sqrt((double) var));
	
	assert (95 < avg && avg < 105);
	assert (sd < 15);
}


void RandomTest::testDistribution3()
{
	Poco::Random rnd;
	rnd.seed();
	const int n = 101;
	std::vector<int> d(n, 0);
	for (int i = 0; i < 10000; ++i)
	{
		d[int(rnd.nextFloat()*n)]++;
	}
	int sum = 0;
	for (int k = 0; k < n; ++k) sum += d[k];
	int avg = sum/n;
	int var = 0;
	for (int k = 0; k < n; ++k) var += (d[k] - avg)*(d[k] - avg);
	var /= n;
	int sd = int(std::sqrt((double) var));
	
	assert (95 < avg && avg < 105);
	assert (sd < 15);
}


void RandomTest::setUp()
{
}


void RandomTest::tearDown()
{
}


CppUnit::Test* RandomTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("RandomTest");

	CppUnit_addTest(pSuite, RandomTest, testSequence1);
	CppUnit_addTest(pSuite, RandomTest, testSequence2);
	CppUnit_addTest(pSuite, RandomTest, testDistribution1);
	CppUnit_addTest(pSuite, RandomTest, testDistribution2);
	CppUnit_addTest(pSuite, RandomTest, testDistribution3);

	return pSuite;
}
