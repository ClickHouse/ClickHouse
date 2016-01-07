//
// TestRunner.h
//
// $Id: //poco/1.4/CppUnit/include/CppUnit/TestRunner.h#2 $
//


#ifndef CppUnit_TestRunner_INCLUDED
#define CppUnit_TestRunner_INCLUDED


#include "CppUnit/CppUnit.h"
#include <vector>
#include <string>
#include <ostream>
#if defined(POCO_VXWORKS)
#include <cstdarg>
#endif


namespace CppUnit {


class Test;


/*
 * A command line based tool to run tests.
 * TestRunner expects as its only argument the name of a TestCase class.
 * TestRunner prints out a trace as the tests are executed followed by a
 * summary at the end.
 *
 * You can add to the tests that the TestRunner knows about by
 * making additional calls to "addTest (...)" in main.
 *
 * Here is the synopsis:
 *
 * TestRunner [-all] [-print] [-wait] ExampleTestCase
 *
 */
class CppUnit_API TestRunner
{
	typedef std::pair<std::string, Test*> Mapping;
	typedef std::vector<Mapping> Mappings;

public:
	TestRunner();
	TestRunner(std::ostream& ostr);
	~TestRunner();

	bool run(const std::vector<std::string>& args);
	void addTest(const std::string& name, Test* test);

protected:
	bool run(Test* test);
	void printBanner();
	void print(const std::string& name, Test* pTest, int indent);
	Test* find(const std::string& name, Test* pTest, const std::string& testName);

private:
	std::ostream& _ostr;
	Mappings _mappings;
};


} // namespace CppUnit


#if defined(POCO_VXWORKS)
#define CppUnitMain(testCase) \
	int testCase##Runner(const char* arg0, ...) \
	{ \
		std::vector<std::string> args; \
		args.push_back(#testCase "Runner"); \
		args.push_back(std::string(arg0)); \
		va_list vargs; \
		va_start(vargs, arg0); \
		const char* arg = va_arg(vargs, const char*); \
		while (arg) \
		{ \
			args.push_back(std::string(arg)); \
			arg = va_arg(vargs, const char*); \
		} \
		va_end(vargs); \
		CppUnit::TestRunner runner; \
		runner.addTest(#testCase, testCase::suite()); \
		return runner.run(args) ? 0 : 1; \
	}
#else
#define CppUnitMain(testCase) \
	int main(int ac, char **av)							\
	{													\
		std::vector<std::string> args;					\
		for (int i = 0; i < ac; ++i)					\
			args.push_back(std::string(av[i]));			\
		CppUnit::TestRunner runner;						\
		runner.addTest(#testCase, testCase::suite());	\
		return runner.run(args) ? 0 : 1;				\
	}
#endif


#endif // CppUnit_TestRunner_INCLUDED
