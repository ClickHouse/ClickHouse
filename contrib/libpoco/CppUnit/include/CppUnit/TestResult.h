//
// TestResult.h
//
// $Id: //poco/1.4/CppUnit/include/CppUnit/TestResult.h#1 $
//


#ifndef CppUnit_TestResult_INCLUDED
#define CppUnit_TestResult_INCLUDED


#include "CppUnit/CppUnit.h"
#include "CppUnit/Guards.h"
#include "CppUnit/TestFailure.h"
#include <vector>


namespace CppUnit {


class CppUnitException;
class Test;


/*
 * A TestResult collects the results of executing a test case. It is an
 * instance of the Collecting Parameter pattern.
 *
 * The test framework distinguishes between failures and errors.
 * A failure is anticipated and checked for with assertions. Errors are
 * unanticipated problems signified by exceptions that are not generated
 * by the framework.
 *
 * TestResult supplies a template method 'setSynchronizationObject ()'
 * so that subclasses can provide mutual exclusion in the face of multiple
 * threads.  This can be useful when tests execute in one thread and
 * they fill a subclass of TestResult which effects change in another
 * thread.  To have mutual exclusion, override setSynchronizationObject ()
 * and make sure that you create an instance of ExclusiveZone at the
 * beginning of each method.
 *
 * see Test
 */
class CppUnit_API TestResult
{
	REFERENCEOBJECT (TestResult)

public:
	TestResult();
	virtual ~TestResult();

	virtual void addError(Test* test, CppUnitException* e);
	virtual void addFailure(Test* test, CppUnitException* e);
	virtual void startTest(Test* test);
	virtual void endTest(Test* test);
	virtual int runTests();
	virtual int testErrors();
	virtual int testFailures();
	virtual bool wasSuccessful();
	virtual bool shouldStop();
	virtual void stop();

	virtual std::vector<TestFailure*>& errors();
	virtual std::vector<TestFailure*>& failures();

	class SynchronizationObject
	{
	public:
		SynchronizationObject()
		{
		}
		
		virtual ~SynchronizationObject()
		{
		}

		virtual void lock()
		{
		}
		
		virtual void unlock()
		{
		}
	};

	class ExclusiveZone
	{
		SynchronizationObject* m_syncObject;

	public:
		ExclusiveZone(SynchronizationObject* syncObject): m_syncObject(syncObject)
		{
			m_syncObject->lock();
		}

		~ExclusiveZone()
		{
			m_syncObject->unlock();
		}
	};

protected:
	virtual void setSynchronizationObject(SynchronizationObject* syncObject);

	std::vector<TestFailure*> _errors;
	std::vector<TestFailure*> _failures;
	int _runTests;
	bool _stop;
	SynchronizationObject* _syncObject;

};


// Construct a TestResult
inline TestResult::TestResult(): _syncObject(new SynchronizationObject())
{
	_runTests = 0; 
	_stop = false;
}


// Adds an error to the list of errors. The passed in exception
// caused the error
inline void TestResult::addError(Test* test, CppUnitException* e)
{
	ExclusiveZone zone(_syncObject); 
	_errors.push_back(new TestFailure(test, e));
}


// Adds a failure to the list of failures. The passed in exception
// caused the failure.
inline void TestResult::addFailure(Test* test, CppUnitException* e)
{
	ExclusiveZone zone(_syncObject); 
	_failures.push_back(new TestFailure(test, e));
}


// Informs the result that a test will be started.
inline void TestResult::startTest(Test* test)
{
	ExclusiveZone zone(_syncObject); 
	_runTests++;
}


// Informs the result that a test was completed.
inline void TestResult::endTest(Test* test)
{
	ExclusiveZone zone(_syncObject);
}


// Gets the number of run tests.
inline int TestResult::runTests()
{
	ExclusiveZone zone(_syncObject); 
	return _runTests;
}


// Gets the number of detected errors.
inline int TestResult::testErrors()
{
	ExclusiveZone zone(_syncObject); 
	return (int) _errors.size();
}


// Gets the number of detected failures.
inline int TestResult::testFailures()
{
	ExclusiveZone zone(_syncObject); 
	return (int) _failures.size();
}


// Returns whether the entire test was successful or not.
inline bool TestResult::wasSuccessful()
{
	ExclusiveZone zone(_syncObject); 
	return _failures.size() == 0 && _errors.size () == 0; 
}


// Returns a std::vector of the errors.
inline std::vector<TestFailure*>& TestResult::errors()
{
	ExclusiveZone zone(_syncObject); 
	return _errors;
}


// Returns a std::vector of the failures.
inline std::vector<TestFailure*>& TestResult::failures()
{
	ExclusiveZone zone(_syncObject); 
	return _failures;
}


// Returns whether testing should be stopped
inline bool TestResult::shouldStop()
{
	ExclusiveZone zone(_syncObject); 
	return _stop;
}


// Stop testing
inline void TestResult::stop()
{
	ExclusiveZone zone(_syncObject); 
	_stop = true;
}


// Accept a new synchronization object for protection of this instance
// TestResult assumes ownership of the object
inline void TestResult::setSynchronizationObject(SynchronizationObject* syncObject)
{
	delete _syncObject; 
	_syncObject = syncObject;
}


} // namespace CppUnit


#endif // CppUnit_TestResult_INCLUDED
