//
// TestSuite.cpp
//
// $Id: //poco/1.4/CppUnit/src/TestSuite.cpp#1 $
//


#include "CppUnit/TestSuite.h"
#include "CppUnit/TestResult.h"


namespace CppUnit {


// Deletes all tests in the suite.
void TestSuite::deleteContents()
{
	for (std::vector<Test*>::iterator it = _tests.begin(); it != _tests.end(); ++it)
		delete *it;
}


// Runs the tests and collects their result in a TestResult.
void TestSuite::run(TestResult *result)
{
	for (std::vector<Test*>::iterator it = _tests.begin(); it != _tests.end(); ++it) 
	{
		if (result->shouldStop ())
			break;

		Test *test = *it;
		test->run(result);
	}
}


// Counts the number of test cases that will be run by this test.
int TestSuite::countTestCases()
{
	int count = 0;

	for (std::vector<Test*>::iterator it = _tests.begin (); it != _tests.end (); ++it)
		count += (*it)->countTestCases();

	return count;
}


} // namespace CppUnit
