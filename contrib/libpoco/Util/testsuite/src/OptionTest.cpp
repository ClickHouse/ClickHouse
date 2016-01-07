//
// OptionTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/OptionTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "OptionTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionException.h"


using Poco::Util::Option;


OptionTest::OptionTest(const std::string& name): CppUnit::TestCase(name)
{
}


OptionTest::~OptionTest()
{
}


void OptionTest::testOption()
{
	Option incOpt = Option("include-dir", "I", "specify an include search path")
		.required(false)
		.repeatable(true)
		.argument("path");
		
	Option libOpt = Option("library-dir", "L", "specify a library search path", false)
		.repeatable(true)
		.argument("path");
		
	Option outOpt = Option("output", "o", "specify the output file", true)
		.argument("file", true);

	Option vrbOpt = Option("verbose", "v")
		.description("enable verbose mode")
		.required(false)
		.repeatable(false);
		
	Option optOpt = Option("optimize", "O")
		.description("enable optimization")
		.required(false)
		.repeatable(false)
		.argument("level", false);
		
	assert (incOpt.shortName() == "I");
	assert (incOpt.fullName() == "include-dir");
	assert (incOpt.repeatable());
	assert (!incOpt.required());
	assert (incOpt.argumentName() == "path");
	assert (incOpt.argumentRequired());
	assert (incOpt.takesArgument());
		
	assert (libOpt.shortName() == "L");
	assert (libOpt.fullName() == "library-dir");
	assert (libOpt.repeatable());
	assert (!libOpt.required());
	assert (libOpt.argumentName() == "path");
	assert (libOpt.argumentRequired());
	assert (incOpt.takesArgument());

	assert (outOpt.shortName() == "o");
	assert (outOpt.fullName() == "output");
	assert (!outOpt.repeatable());
	assert (outOpt.required());
	assert (outOpt.argumentName() == "file");
	assert (outOpt.argumentRequired());
	assert (incOpt.takesArgument());

	assert (vrbOpt.shortName() == "v");
	assert (vrbOpt.fullName() == "verbose");
	assert (!vrbOpt.repeatable());
	assert (!vrbOpt.required());
	assert (!vrbOpt.argumentRequired());
	assert (!vrbOpt.takesArgument());

	assert (optOpt.shortName() == "O");
	assert (optOpt.fullName() == "optimize");
	assert (!optOpt.repeatable());
	assert (!optOpt.required());
	assert (optOpt.argumentName() == "level");
	assert (optOpt.takesArgument());
	assert (!optOpt.argumentRequired());
}


void OptionTest::testMatches1()
{
	Option incOpt = Option("include-dir", "I", "specify an include search path")
		.required(false)
		.repeatable(true)
		.argument("path");
		
	assert (incOpt.matchesShort("Iinclude"));
	assert (incOpt.matchesPartial("include:include"));
	assert (incOpt.matchesPartial("include-dir:include"));
	assert (incOpt.matchesPartial("inc=include"));
	assert (incOpt.matchesPartial("INCLUDE=include"));
	assert (incOpt.matchesPartial("include"));
	assert (incOpt.matchesShort("I"));
	assert (incOpt.matchesPartial("i"));
	
	assert (incOpt.matchesFull("include-dir:include"));
	assert (incOpt.matchesFull("INClude-dir:include"));
	assert (!incOpt.matchesFull("include:include"));
	assert (!incOpt.matchesFull("include-dir2:include"));
	
	assert (!incOpt.matchesPartial("include-dir2=include"));
	assert (!incOpt.matchesShort("linclude"));
}


void OptionTest::testMatches2()
{
	Option incOpt = Option("include-dir", "", "specify an include search path")
		.required(false)
		.repeatable(true)
		.argument("path");
		
	assert (!incOpt.matchesShort("Iinclude"));
	assert (incOpt.matchesPartial("include:include"));
	assert (incOpt.matchesPartial("include-dir:include"));
	assert (incOpt.matchesPartial("inc=include"));
	assert (incOpt.matchesPartial("INCLUDE=include"));
	assert (incOpt.matchesPartial("I"));
	assert (incOpt.matchesPartial("i"));
	
	assert (incOpt.matchesFull("include-dir:include"));
	assert (incOpt.matchesFull("INClude-dir:include"));
	assert (!incOpt.matchesFull("include:include"));
	assert (!incOpt.matchesFull("include-dir2:include"));
	
	assert (!incOpt.matchesFull("include-dir2=include"));
	assert (!incOpt.matchesShort("linclude"));
}


void OptionTest::testProcess1()
{
	Option incOpt = Option("include-dir", "I", "specify an include search path")
		.required(false)
		.repeatable(true)
		.argument("path");

	std::string arg;
	incOpt.process("Iinclude", arg);
	assert (arg == "include");
	incOpt.process("I/usr/include", arg);
	assert (arg == "/usr/include");
	incOpt.process("include:/usr/local/include", arg);
	assert (arg == "/usr/local/include");
	incOpt.process("include=/proj/include", arg);
	assert (arg == "/proj/include");
	incOpt.process("include-dir=/usr/include", arg);
	assert (arg == "/usr/include");
	incOpt.process("Include-dir:/proj/include", arg);
	assert (arg == "/proj/include");
	
	try
	{
		incOpt.process("I", arg);
		fail("argument required - must throw");
	}
	catch (Poco::Util::MissingArgumentException&)
	{
	}

	try
	{
		incOpt.process("Include", arg);
		fail("argument required - must throw");
	}
	catch (Poco::Util::MissingArgumentException&)
	{
	}
	
	try
	{
		incOpt.process("Llib", arg);
		fail("wrong option - must throw");
	}
	catch (Poco::Util::UnknownOptionException&)
	{
	}
	
	Option vrbOpt = Option("verbose", "v")
		.description("enable verbose mode")
		.required(false)
		.repeatable(false);
	
	vrbOpt.process("v", arg);
	assert (arg.empty());
	vrbOpt.process("verbose", arg);
	assert (arg.empty());
	
	try
	{
		vrbOpt.process("v2", arg);
		fail("no argument expected - must throw");
	}
	catch (Poco::Util::UnexpectedArgumentException&)
	{
	}

	try
	{
		vrbOpt.process("verbose:2", arg);
		fail("no argument expected - must throw");
	}
	catch (Poco::Util::UnexpectedArgumentException&)
	{
	}
	
	Option optOpt = Option("optimize", "O")
		.description("enable optimization")
		.required(false)
		.repeatable(false)
		.argument("level", false);
		
	optOpt.process("O", arg);
	assert (arg.empty());
	optOpt.process("O2", arg);
	assert (arg == "2");
	optOpt.process("optimize:1", arg);
	assert (arg == "1");
	optOpt.process("opt", arg);
	assert (arg.empty());
	optOpt.process("opt=3", arg);
	assert (arg == "3");
	optOpt.process("opt=", arg);
	assert (arg.empty());
}


void OptionTest::testProcess2()
{
	Option incOpt = Option("include-dir", "", "specify an include search path")
		.required(false)
		.repeatable(true)
		.argument("path");

	std::string arg;
	incOpt.process("include:/usr/local/include", arg);
	assert (arg == "/usr/local/include");
	incOpt.process("include=/proj/include", arg);
	assert (arg == "/proj/include");
	incOpt.process("include-dir=/usr/include", arg);
	assert (arg == "/usr/include");
	incOpt.process("Include-dir:/proj/include", arg);
	assert (arg == "/proj/include");
	
	try
	{
		incOpt.process("Iinclude", arg);
		fail("unknown option - must throw");
	}
	catch (Poco::Util::UnknownOptionException&)
	{
	}
	
	try
	{
		incOpt.process("I", arg);
		fail("argument required - must throw");
	}
	catch (Poco::Util::MissingArgumentException&)
	{
	}

	try
	{
		incOpt.process("Include", arg);
		fail("argument required - must throw");
	}
	catch (Poco::Util::MissingArgumentException&)
	{
	}
	
	try
	{
		incOpt.process("Llib", arg);
		fail("wrong option - must throw");
	}
	catch (Poco::Util::UnknownOptionException&)
	{
	}
	
	Option vrbOpt = Option("verbose", "")
		.description("enable verbose mode")
		.required(false)
		.repeatable(false);
	
	vrbOpt.process("v", arg);
	assert (arg.empty());
	vrbOpt.process("verbose", arg);
	assert (arg.empty());
	
	try
	{
		vrbOpt.process("v2", arg);
		fail("no argument expected - must throw");
	}
	catch (Poco::Util::UnknownOptionException&)
	{
	}

	try
	{
		vrbOpt.process("verbose:2", arg);
		fail("no argument expected - must throw");
	}
	catch (Poco::Util::UnexpectedArgumentException&)
	{
	}
}


void OptionTest::setUp()
{
}


void OptionTest::tearDown()
{
}


CppUnit::Test* OptionTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("OptionTest");

	CppUnit_addTest(pSuite, OptionTest, testOption);
	CppUnit_addTest(pSuite, OptionTest, testMatches1);
	CppUnit_addTest(pSuite, OptionTest, testMatches2);
	CppUnit_addTest(pSuite, OptionTest, testProcess1);
	CppUnit_addTest(pSuite, OptionTest, testProcess2);

	return pSuite;
}
