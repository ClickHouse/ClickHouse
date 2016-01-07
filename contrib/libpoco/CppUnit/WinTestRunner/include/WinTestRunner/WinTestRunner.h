//
// WinTestRunner.h
//
// $Id: //poco/1.4/CppUnit/WinTestRunner/include/WinTestRunner/WinTestRunner.h#1 $
//
// Application shell for CppUnit's TestRunner dialog.
//


#ifndef WinTestRunner_H_INCLUDED
#define WinTestRunner_H_INCLUDED


#if !defined(POCO_STATIC)
#if defined(WinTestRunner_EXPORTS)
#define WinTestRunner_API __declspec(dllexport)
#else
#define WinTestRunner_API __declspec(dllimport)
#endif
#else
#define WinTestRunner_API
#endif


#include "CppUnit/CppUnit.h"
#include <vector>
#include <afxwin.h>


namespace CppUnit {


class Test;


class WinTestRunner_API WinTestRunner
{
public:
	WinTestRunner();
	~WinTestRunner();

	void run();
	void addTest(Test* pTest);
	
private:
	std::vector<Test*> _tests;
};


class WinTestRunner_API WinTestRunnerApp: public CWinApp
	/// A simple application class that hosts the TestRunner dialog.
	/// Create a subclass and override the TestMain() method.
	///
	/// WinTestRunnerApp supports a batch mode, which runs the
	/// test using the standard text-based TestRunner from CppUnit.
	/// To enable batch mode, start the application with the "/b"
	/// or "/B" argument. Optionally, a filename may be specified
	/// where the test output will be written to: "/b:<path>" or
	/// "/B:<path>".
	///
	/// When run in batch mode, the exit code of the application
	/// will denote test success (0) or failure (1).
{
public:
	virtual BOOL InitInstance();

	virtual void TestMain() = 0;

	DECLARE_MESSAGE_MAP()
};


} // namespace CppUnit


#endif // WinTestRunner_H_INCLUDED

