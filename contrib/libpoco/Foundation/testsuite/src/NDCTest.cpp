//
// NDCTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/NDCTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NDCTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/NestedDiagnosticContext.h"
#include <iostream>


using Poco::NDC;


NDCTest::NDCTest(const std::string& name): CppUnit::TestCase(name)
{
}


NDCTest::~NDCTest()
{
}


void NDCTest::testNDC()
{
	NDC ndc;
	assert (ndc.depth() == 0);
	ndc.push("item1");
	assert (ndc.toString() == "item1");
	assert (ndc.depth() == 1);
	ndc.push("item2");
	assert (ndc.toString() == "item1:item2");
	assert (ndc.depth() == 2);
	ndc.pop();
	assert (ndc.depth() == 1);
	assert (ndc.toString() == "item1");
	ndc.pop();
	assert (ndc.depth() == 0);
}


void NDCTest::testNDCScope()
{
	poco_ndc("item1");
	assert (NDC::current().depth() == 1);
	{
		poco_ndc("item2");
		assert (NDC::current().depth() == 2);
		{
			poco_ndc("item3");
			assert (NDC::current().depth() == 3);
			NDC::current().dump(std::cout);
		}
		assert (NDC::current().depth() == 2);
	}
	assert (NDC::current().depth() == 1);
}


void NDCTest::setUp()
{
}


void NDCTest::tearDown()
{
}


CppUnit::Test* NDCTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NDCTest");

	CppUnit_addTest(pSuite, NDCTest, testNDC);
	CppUnit_addTest(pSuite, NDCTest, testNDCScope);

	return pSuite;
}
