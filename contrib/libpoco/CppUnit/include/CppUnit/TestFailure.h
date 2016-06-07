//
// TestFailure.h
//
// $Id: //poco/1.4/CppUnit/include/CppUnit/TestFailure.h#1 $
//


#ifndef CppUnit_TestFailure_INCLUDED
#define CppUnit_TestFailure_INCLUDED


#include "CppUnit/CppUnit.h"
#include "CppUnit/CppUnitException.h"
#include "CppUnit/Guards.h"


namespace CppUnit {


class Test;


/*
 * A TestFailure collects a failed test together with
 * the caught exception.
 *
 * TestFailure assumes lifetime control for any exception
 * passed to it.  The lifetime of tests is handled by
 * their TestSuite (if they have been added to one) or
 * whomever creates them.
 *
 * see TestResult
 * see TestSuite
 *
 */
class CppUnit_API TestFailure
{
	REFERENCEOBJECT (TestFailure)

public:
	TestFailure(Test* failedTest, CppUnitException* thrownException);
	~TestFailure();

	Test* failedTest();
	CppUnitException* thrownException();
	std::string toString();

protected:
	Test* _failedTest;
	CppUnitException *_thrownException;
};


// Constructs a TestFailure with the given test and exception.
inline TestFailure::TestFailure(Test* failedTest, CppUnitException* thrownException): _failedTest(failedTest), _thrownException(thrownException)
{
}


// Deletes the owned exception.
inline TestFailure::~TestFailure()
{ 
	delete _thrownException;
}


// Gets the failed test.
inline Test* TestFailure::failedTest()
{
	return _failedTest;
}


// Gets the thrown exception.
inline CppUnitException* TestFailure::thrownException()
{
	return _thrownException;
}


} // namespace CppUnit


#endif // CppUnit_TestFailure_INCLUDED


