//
// TestDecorator.h
//
// $Id: //poco/1.4/CppUnit/include/CppUnit/TestDecorator.h#1 $
//


#ifndef CppUnit_TestDecorator_INCLUDED
#define CppUnit_TestDecorator_INCLUDED


#include "CppUnit/CppUnit.h"
#include "CppUnit/Guards.h"
#include "CppUnit/Test.h"


namespace CppUnit {


class TestResult;


/*
 * A Decorator for Tests
 *
 * Does not assume ownership of the test it decorates
 *
 */
class CppUnit_API TestDecorator: public Test
{
	REFERENCEOBJECT(TestDecorator)

public:
	TestDecorator(Test* test);

	virtual ~TestDecorator();

	int countTestCases();

	void run(TestResult* result);

	std::string toString();

protected:
	Test* _test;
};


} // namespace CppUnit


#endif // CppUnit_TestDecorator_INCLUDED
