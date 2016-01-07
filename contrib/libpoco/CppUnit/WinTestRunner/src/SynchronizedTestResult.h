#ifndef SYNCHRONIZEDTESTRESULTDECORATOR_H
#define SYNCHRONIZEDTESTRESULTDECORATOR_H

#include <afxmt.h>
#include "TestResultDecorator.h"

class SynchronizedTestResult : public TestResultDecorator
{
public:
								SynchronizedTestResult (TestResult *result);
								~SynchronizedTestResult ();


	bool						shouldStop	();
	void						addError	(Test *test, CppUnitException *e);
	void						addFailure	(Test *test, CppUnitException *e);
	void						startTest	(Test *test);
	void						endTest		(Test *test);
	int							runTests	();
	int							testErrors  ();
	int							testFailures ();
	bool						wasSuccessful ();
	void						stop ();

	vector<TestFailure *>&		errors ();
	vector<TestFailure *>&		failures ();

private:
	CCriticalSection			m_criticalSection;

};


// Constructor
inline SynchronizedTestResult::SynchronizedTestResult (TestResult *result)
: TestResultDecorator (result) {}

// Destructor
inline SynchronizedTestResult::~SynchronizedTestResult ()
{}

// Returns whether the test should stop
inline bool SynchronizedTestResult::shouldStop ()
{ CSingleLock sync (&m_criticalSection, TRUE); return m_result->shouldStop (); }


// Adds an error to the list of errors. The passed in exception
// caused the error
inline void SynchronizedTestResult::addError (Test *test, CppUnitException *e)
{ CSingleLock sync (&m_criticalSection, TRUE); m_result->addError (test, e); }


// Adds a failure to the list of failures. The passed in exception
// caused the failure.
inline void SynchronizedTestResult::addFailure (Test *test, CppUnitException *e)
{ CSingleLock sync (&m_criticalSection, TRUE); m_result->addFailure (test, e); }


// Informs the result that a test will be started.
inline void SynchronizedTestResult::startTest (Test *test)
{ CSingleLock sync (&m_criticalSection, TRUE); m_result->startTest (test); }


// Informs the result that a test was completed.
inline void SynchronizedTestResult::endTest (Test *test)
{ CSingleLock sync (&m_criticalSection, TRUE); m_result->endTest (test); }


// Gets the number of run tests.
inline int SynchronizedTestResult::runTests ()
{ CSingleLock sync (&m_criticalSection, TRUE); return m_result->runTests (); }


// Gets the number of detected errors.
inline int SynchronizedTestResult::testErrors ()
{ CSingleLock sync (&m_criticalSection, TRUE); return m_result->testErrors (); }


// Gets the number of detected failures.
inline int SynchronizedTestResult::testFailures ()
{ CSingleLock sync (&m_criticalSection, TRUE); return m_result->testFailures (); }


// Returns whether the entire test was successful or not.
inline bool SynchronizedTestResult::wasSuccessful ()
{ CSingleLock sync (&m_criticalSection, TRUE); return m_result->wasSuccessful (); }


// Marks that the test run should stop.
inline void SynchronizedTestResult::stop ()
{ CSingleLock sync (&m_criticalSection, TRUE); m_result->stop (); }


// Returns a vector of the errors.
inline vector<TestFailure *>& SynchronizedTestResult::errors ()
{ CSingleLock sync (&m_criticalSection, TRUE); return m_result->errors (); }


// Returns a vector of the failures.
inline vector<TestFailure *>& SynchronizedTestResult::failures ()
{ CSingleLock sync (&m_criticalSection, TRUE); return m_result->failures (); }


#endif


