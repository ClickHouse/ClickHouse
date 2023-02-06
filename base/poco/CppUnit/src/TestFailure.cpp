//
// TestFailure.cpp
//


#include "CppUnit/TestFailure.h"
#include "CppUnit/Test.h"


namespace CppUnit {


// Returns a short description of the failure.
std::string TestFailure::toString()
{
	return _failedTest->toString () + ": " + _thrownException->what();
}


} // namespace CppUnit
