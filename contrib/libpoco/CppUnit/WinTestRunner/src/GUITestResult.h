//
// GUITestResult.h
//
// $Id: //poco/1.4/CppUnit/WinTestRunner/src/GUITestResult.h#1 $
//


#ifndef GuiTestResult_INCLUDED
#define GuiTestResult_INCLUDED


#include "CppUnit/CppUnit.h"
#include "CppUnit/TestResult.h"
#include <afxmt.h>


namespace CppUnit {


class TestRunnerDlg;


class GUITestResult: public TestResult
{
public:
	GUITestResult(TestRunnerDlg* runner);
	~GUITestResult();

	void addError(Test* test, CppUnitException* e);
	void addFailure(Test* test, CppUnitException* e);

	void startTest(Test* test);
	void endTest(Test* test);
	void stop();

protected:
	class LightweightSynchronizationObject: public TestResult::SynchronizationObject
	{
	public:
		void lock()
		{
			_syncObject.Lock();
		}
		
		void unlock()
		{
			_syncObject.Unlock();
		}
		
	private:
		CCriticalSection _syncObject;
	};

private:
    TestRunnerDlg *_runner;
};



// Construct with lightweight synchronization
inline GUITestResult::GUITestResult(TestRunnerDlg* runner): _runner(runner) 
{
	setSynchronizationObject(new LightweightSynchronizationObject());
}


// Destructor
inline GUITestResult::~GUITestResult()
{
}


// Override without protection to prevent deadlock
inline void GUITestResult::stop()
{
	_stop = true; 
}


} // namespace CppUnit


#endif // GuiTestResult_INCLUDED
