//
// TextTestResult.cpp
//
// $Id: //poco/1.4/CppUnit/src/TextTestResult.cpp#1 $
//


#include "CppUnit/TextTestResult.h"
#include "CppUnit/CppUnitException.h"
#include "CppUnit/Test.h"
#include "CppUnit/estring.h"
#include <iostream>
#include <iomanip>
#include <cstdlib>
#include <cctype>


namespace CppUnit {


TextTestResult::TextTestResult():
	_ostr(std::cout)
{
	setup();
}


TextTestResult::TextTestResult(std::ostream& ostr):
	_ostr(ostr)
{
	setup();
}


void TextTestResult::setup()
{
#if !defined(_WIN32_WCE)
	const char* env = std::getenv("CPPUNIT_IGNORE");
	if (env)
	{
		std::string ignored = env;
		std::string::const_iterator it = ignored.begin();
		std::string::const_iterator end = ignored.end();
		while (it != end)
		{
			while (it != end && std::isspace(*it)) ++it;
			std::string test;
			while (it != end && !std::isspace(*it)) test += *it++;
			if (!test.empty()) _ignored.insert(test);
		}
	}
#endif
}


void TextTestResult::addError(Test* test, CppUnitException* e)
{
	if (_ignored.find(test->toString()) == _ignored.end())
	{
		TestResult::addError(test, e);
		_ostr << "ERROR" << std::flush;
	}
	else
	{
		_ostr << "ERROR (ignored)" << std::flush;
	}
}


void TextTestResult::addFailure(Test* test, CppUnitException* e)
{
	if (_ignored.find(test->toString()) == _ignored.end())
	{
		TestResult::addFailure(test, e);
		_ostr << "FAILURE" << std::flush;
	}
	else
	{
		_ostr << "FAILURE (ignored)" << std::flush;
	}
}


void TextTestResult::startTest(Test* test)
{
	TestResult::startTest(test);
	_ostr << "\n" << shortName(test->toString()) << ": ";
}


void TextTestResult::printErrors(std::ostream& stream)
{
	if (testErrors() != 0) 
	{
		stream << "\n";

		if (testErrors() == 1)
			stream << "There was " << testErrors() << " error: " << std::endl;
		else
			stream << "There were " << testErrors() << " errors: " << std::endl;

		int i = 1;
		for (std::vector<TestFailure*>::iterator it = errors().begin(); it != errors().end(); ++it)
		{
			TestFailure* failure = *it;
			CppUnitException* e = failure->thrownException();

			stream << std::setw(2) << i
			       << ": "
			       << failure->failedTest()->toString() << "\n"
			       << "    \"" << (e ? e->what() : "") << "\"\n"
			       << "    in \"" 
			       << (e ? e->fileName() : std::string())
			       << "\", line ";
			if (e == 0)
			{
				stream << "0";
			}
			else
			{
				stream << e->lineNumber();
				if (e->data2LineNumber() != CppUnitException::CPPUNIT_UNKNOWNLINENUMBER)
				{
					stream << " data lines " << e->data1LineNumber()
                                               << ", " << e->data2LineNumber();
				}
				else if (e->data1LineNumber() != CppUnitException::CPPUNIT_UNKNOWNLINENUMBER)
				{
					stream << " data line " << e->data1LineNumber();
				}
			}
			stream << "\n";
			i++;
		}
	}
}


void TextTestResult::printFailures(std::ostream& stream)
{
	if (testFailures() != 0)
	{
		stream << "\n";
		if (testFailures() == 1)
			stream << "There was " << testFailures() << " failure: " << std::endl;
		else
			stream << "There were " << testFailures() << " failures: " << std::endl;

		int i = 1;

		for (std::vector<TestFailure*>::iterator it = failures().begin(); it != failures().end(); ++it)
		{
			TestFailure* failure = *it;
			CppUnitException* e = failure->thrownException();

			stream << std::setw(2) << i
			       << ": "
			       << failure->failedTest()->toString() << "\n"
			       << "    \"" << (e ? e->what() : "") << "\"\n"
			       << "    in \"" 
			       << (e ? e->fileName() : std::string())
			       << "\", line ";
			if (e == 0)
			{
				stream << "0";
			}
			else
			{
				stream << e->lineNumber();
				if (e->data2LineNumber() != CppUnitException::CPPUNIT_UNKNOWNLINENUMBER)
				{
					stream << " data lines " 
					       << e->data1LineNumber()
                           << ", " << e->data2LineNumber();
				}
				else if (e->data1LineNumber() != CppUnitException::CPPUNIT_UNKNOWNLINENUMBER)
				{
					stream << " data line " << e->data1LineNumber();
				}
			}
			stream << "\n";
			i++;
		}
	}
}


void TextTestResult::print(std::ostream& stream)
{
	printHeader(stream);
	printErrors(stream);
	printFailures(stream);
}


void TextTestResult::printHeader(std::ostream& stream)
{
	stream << "\n\n";
	if (wasSuccessful())
		stream << "OK (" 
		          << runTests() << " tests)" 
		          << std::endl;
	else
		stream << "!!!FAILURES!!!" << std::endl
		          << "Runs: "
		          << runTests ()
		          << "   Failures: "
		          << testFailures ()
		          << "   Errors: "
		          << testErrors ()
		          << std::endl;
}


std::string TextTestResult::shortName(const std::string& testName)
{
	std::string::size_type pos = testName.rfind('.');
	if (pos != std::string::npos)
		return std::string(testName, pos + 1);
	else
		return testName;
}


} // namespace CppUnit
