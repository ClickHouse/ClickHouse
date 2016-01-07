//
// ActiveTest.cpp
//
// $Id: //poco/1.4/CppUnit/WinTestRunner/src/ActiveTest.cpp#1 $
//


#include <afxwin.h>
#include "ActiveTest.h"


namespace CppUnit {


// Spawn a thread to a test
void ActiveTest::run(TestResult* result)
{
	CWinThread* thread;

	setTestResult(result);
	_runCompleted.ResetEvent();

	thread = AfxBeginThread(threadFunction, this, THREAD_PRIORITY_NORMAL, 0, CREATE_SUSPENDED);
	DuplicateHandle(GetCurrentProcess(), thread->m_hThread, GetCurrentProcess(), &_threadHandle, 0, FALSE, DUPLICATE_SAME_ACCESS);

	thread->ResumeThread();
}


// Simple execution thread.  Assuming that an ActiveTest instance
// only creates one of these at a time.
UINT ActiveTest::threadFunction(LPVOID thisInstance)
{
	ActiveTest* test = (ActiveTest*) thisInstance;

	test->run();
	test->_runCompleted.SetEvent();

	return 0;
}


} // namespace CppUnit

