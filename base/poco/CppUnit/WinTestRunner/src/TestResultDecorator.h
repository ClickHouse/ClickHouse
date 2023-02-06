#ifndef CPP_UNIT_TESTRESULTDECORATOR_H
#define CPP_UNIT_TESTRESULTDECORATOR_H

#include "TestResult.h"

class TestResultDecorator
{
public:
								TestResultDecorator (TestResult *result);
	virtual						~TestResultDecorator ();


	virtual bool				shouldStop	();
	virtual void				addError	(Test *test, CppUnitException *e);
	virtual void				addFailure	(Test *test, CppUnitException *e);
	virtual void				startTest	(Test *test);
	virtual void				endTest		(Test *test);
	virtual int					runTests	();
	virtual int					testErrors  ();
	virtual int					testFailures ();
	virtual bool				wasSuccessful ();
	virtual void				stop ();

	vector<TestFailure *>&		errors ();
	vector<TestFailure *>&		failures ();

protected:
	TestResult					*m_result;
};


inline TestResultDecorator::TestResultDecorator (TestResult *result)
: m_result (result) {}

inline TestResultDecorator::~TestResultDecorator ()
{}

// Returns whether the test should stop
inline bool TestResultDecorator::shouldStop ()
{ return m_result->shouldStop (); }


// Adds an error to the list of errors. The passed in exception
// caused the error
inline void TestResultDecorator::addError (Test *test, CppUnitException *e)
{ m_result->addError (test, e); }


// Adds a failure to the list of failures. The passed in exception
// caused the failure.
inline void TestResultDecorator::addFailure (Test *test, CppUnitException *e)
{ m_result->addFailure (test, e); }


// Informs the result that a test will be started.
inline void TestResultDecorator::startTest (Test *test)
{ m_result->startTest (test); }


// Informs the result that a test was completed.
inline void TestResultDecorator::endTest (Test *test)
{ m_result->endTest (test); }


// Gets the number of run tests.
inline int TestResultDecorator::runTests ()
{ return m_result->runTests (); }


// Gets the number of detected errors.
inline int TestResultDecorator::testErrors ()
{ return m_result->testErrors (); }


// Gets the number of detected failures.
inline int TestResultDecorator::testFailures ()
{ return m_result->testFailures (); }


// Returns whether the entire test was successful or not.
inline bool TestResultDecorator::wasSuccessful ()
{ return m_result->wasSuccessful (); }


// Marks that the test run should stop.
inline void TestResultDecorator::stop ()
{ m_result->stop (); }


// Returns a vector of the errors.
inline vector<TestFailure *>& TestResultDecorator::errors ()
{ return m_result->errors (); }


// Returns a vector of the failures.
inline vector<TestFailure *>& TestResultDecorator::failures ()
{ return m_result->failures (); }


#endif


