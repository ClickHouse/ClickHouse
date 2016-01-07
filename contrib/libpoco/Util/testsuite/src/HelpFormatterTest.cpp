//
// HelpFormatterTest.cpp
//
// $Id: //poco/1.4/Util/testsuite/src/HelpFormatterTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "HelpFormatterTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"
#include <iostream>


using Poco::Util::Option;
using Poco::Util::OptionSet;
using Poco::Util::HelpFormatter;


HelpFormatterTest::HelpFormatterTest(const std::string& name): CppUnit::TestCase(name)
{
}


HelpFormatterTest::~HelpFormatterTest()
{
}


void HelpFormatterTest::testHelpFormatter()
{
	OptionSet set;
	set.addOption(
		Option("include-dir", "I", "specify a search path for locating header files")
			.required(false)
			.repeatable(true)
			.argument("path"));
			
	set.addOption(
		Option("library-dir", "L", "specify a search path for locating library files (this option has a very long description)")
			.required(false)
			.repeatable(true)
			.argument("path"));

	set.addOption(
		Option("output", "o", "specify the output file", true)
			.argument("file", true));

	set.addOption(
		Option("verbose", "v")
		.description("enable verbose mode")
		.required(false)
		.repeatable(false));
		
	set.addOption(
		Option("optimize", "O")
		.description("enable optimization")
		.required(false)
		.repeatable(false)
		.argument("level", false));

	HelpFormatter formatter(set);
	formatter.format(std::cout);
	
	formatter.setCommand("cc");
	formatter.format(std::cout);
	
	formatter.setUsage("OPTIONS FILES");
	formatter.setHeader("Lorem ipsum dolor sit amet, consectetuer adipiscing elit. "
		"Vivamus volutpat imperdiet massa. Nulla at ipsum vitae risus facilisis posuere. "
		"Cras convallis, lacus ac vulputate convallis, metus nisl euismod ligula, "
		"ac euismod diam wisi in dolor.\nMauris vitae leo.");
	formatter.setFooter("Cras semper mollis tellus. Mauris eleifend mauris et lorem. "
		"Etiam odio dolor, fermentum quis, mollis nec, sodales sed, tellus. "
		"Quisque consequat orci eu augue. Aliquam ac nibh ac neque hendrerit iaculis.");
	formatter.format(std::cout);
	
	formatter.setUnixStyle(false);
	formatter.format(std::cout);
	
	formatter.setHeader("");
	formatter.setFooter("tab: a\tb\tcde\tf\n\ta\n\t\tb");
	formatter.format(std::cout);
}


void HelpFormatterTest::setUp()
{
}


void HelpFormatterTest::tearDown()
{
}


CppUnit::Test* HelpFormatterTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("HelpFormatterTest");

	CppUnit_addTest(pSuite, HelpFormatterTest, testHelpFormatter);

	return pSuite;
}
