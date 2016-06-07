//
// TestResult.cpp
//
// $Id: //poco/1.4/CppUnit/src/TestResult.cpp#1 $
//


#include "CppUnit/TestResult.h"


namespace CppUnit {


// Destroys a test result
TestResult::~TestResult()
{
	std::vector<TestFailure*>::iterator it;

	for (it = _errors.begin(); it != _errors.end(); ++it)
		delete *it;

	for (it = _failures.begin(); it != _failures.end(); ++it)
		delete *it;

	delete _syncObject;
}


} // namespace CppUnit
