//
// ValidatorTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/ValidatorTest.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ValidatorTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/RegExpValidator.h"
#include "Poco/Util/IntValidator.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionException.h"
#include "Poco/AutoPtr.h"


using Poco::Util::Validator;
using Poco::Util::RegExpValidator;
using Poco::Util::IntValidator;
using Poco::Util::Option;
using Poco::Util::InvalidArgumentException;
using Poco::AutoPtr;


ValidatorTest::ValidatorTest(const std::string& name): CppUnit::TestCase(name)
{
}


ValidatorTest::~ValidatorTest()
{
}


void ValidatorTest::testRegExpValidator()
{
	Option option("option", "o");
	AutoPtr<Validator> pVal(new RegExpValidator("[0-9]+"));
	
	pVal->validate(option, "0");
	pVal->validate(option, "12345");

	try
	{
		pVal->validate(option, " 234");
		fail("does not match - must throw");
	}
	catch (InvalidArgumentException& exc)
	{
		std::string s(exc.message());
		assert (s == "argument for option does not match regular expression [0-9]+");
	}

	try
	{
		pVal->validate(option, "234asdf");
		fail("does not match - must throw");
	}
	catch (InvalidArgumentException& exc)
	{
		std::string s(exc.message());
		assert (s == "argument for option does not match regular expression [0-9]+");
	}

	try
	{
		pVal->validate(option, "abc");
		fail("does not match - must throw");
	}
	catch (InvalidArgumentException& exc)
	{
		std::string s(exc.message());
		assert (s == "argument for option does not match regular expression [0-9]+");
	}

	try
	{
		pVal->validate(option, "");
		fail("does not match - must throw");
	}
	catch (InvalidArgumentException& exc)
	{
		std::string s(exc.message());
		assert (s == "argument for option does not match regular expression [0-9]+");
	}
}


void ValidatorTest::testIntValidator()
{
	Option option("option", "o");
	AutoPtr<Validator> pVal(new IntValidator(0, 100));
	
	pVal->validate(option, "0");
	pVal->validate(option, "100");
	pVal->validate(option, "55");
	
	try
	{
		pVal->validate(option, "-1");
		fail("out of range - must throw");
	}
	catch (InvalidArgumentException& exc)
	{
		std::string s(exc.message());
		assert (s == "argument for option must be in range 0 to 100");
	}

	try
	{
		pVal->validate(option, "101");
		fail("out of range - must throw");
	}
	catch (InvalidArgumentException& exc)
	{
		std::string s(exc.message());
		assert (s == "argument for option must be in range 0 to 100");
	}

	try
	{
		pVal->validate(option, "asdf");
		fail("not a number - must throw");
	}
	catch (InvalidArgumentException& exc)
	{
		std::string s(exc.message());
		assert (s == "argument for option must be an integer");
	}
}


void ValidatorTest::setUp()
{
}


void ValidatorTest::tearDown()
{
}


CppUnit::Test* ValidatorTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ValidatorTest");

	CppUnit_addTest(pSuite, ValidatorTest, testRegExpValidator);
	CppUnit_addTest(pSuite, ValidatorTest, testIntValidator);

	return pSuite;
}
