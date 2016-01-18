//
// TestDecorator.cpp
//
// $Id: //poco/1.4/CppUnit/src/TestDecorator.cpp#1 $
//


#include "CppUnit/TestDecorator.h"


namespace CppUnit {


TestDecorator::TestDecorator(Test* test)
{
	_test = test;
}


TestDecorator::~TestDecorator()
{
}


int TestDecorator::countTestCases()
{
	return _test->countTestCases();
}


void TestDecorator::run(TestResult* result)
{
	_test->run(result);
} 


std::string TestDecorator::toString()
{
	return _test->toString();
}


} // namespace CppUnit
