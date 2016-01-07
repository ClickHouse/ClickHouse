//
// WinTestRunner.cpp
//
// $Id: //poco/1.4/CppUnit/WinTestRunner/src/WinTestRunner.cpp#1 $
//


#include "WinTestRunner/WinTestRunner.h"
#include "TestRunnerDlg.h"
#include "CppUnit/TestRunner.h"
#include <fstream>


namespace CppUnit {


WinTestRunner::WinTestRunner()
{
}


WinTestRunner::~WinTestRunner()
{
	for (std::vector<Test*>::iterator it = _tests.begin(); it != _tests.end(); ++it)
		delete *it;
}


void WinTestRunner::run()
{
	// Note: The following code is some evil hack to
	// add batch capability to the MFC based WinTestRunner.
	
	std::string cmdLine(AfxGetApp()->m_lpCmdLine);
	if (cmdLine.size() >= 2 && cmdLine[0] == '/' && (cmdLine[1] == 'b' || cmdLine[1] == 'B'))
	{
		TestRunner runner;
		for (std::vector<Test*>::iterator it = _tests.begin(); it != _tests.end(); ++it)
			runner.addTest((*it)->toString(), *it);
		_tests.clear();
		std::vector<std::string> args;
		args.push_back("WinTestRunner");
		args.push_back("-all");
		bool success = runner.run(args);
		ExitProcess(success ? 0 : 1);
	}
	else
	{
		// We're running in interactive mode.
		TestRunnerDlg dlg;
		dlg.setTests(_tests);
		dlg.DoModal();
	}
}


void WinTestRunner::addTest(Test* pTest)
{
	_tests.push_back(pTest);
}


BEGIN_MESSAGE_MAP(WinTestRunnerApp, CWinApp)
END_MESSAGE_MAP()


BOOL WinTestRunnerApp::InitInstance()
{	
	std::string cmdLine(AfxGetApp()->m_lpCmdLine);
	if (cmdLine.size() >= 2 && cmdLine[0] == '/' && (cmdLine[1] == 'b' || cmdLine[1] == 'B'))
	{
		// We're running in batch mode.
		std::string outPath;
		if (cmdLine.size() > 4 && cmdLine[2] == ':')
		{
			outPath = cmdLine.substr(3);
		}
		else
		{
			char buffer[1024];
			GetModuleFileName(NULL, buffer, sizeof(buffer));
			outPath = buffer;
			outPath += ".out";
		}
		freopen(outPath.c_str(), "w", stdout);
		freopen(outPath.c_str(), "w", stderr);
		TestMain();
	}
	else
	{
		AllocConsole();
		SetConsoleTitle("CppUnit WinTestRunner Console");
		freopen("CONOUT$", "w", stdout);
		freopen("CONOUT$", "w", stderr);
		freopen("CONIN$", "r", stdin);
		TestMain();
		FreeConsole();
	}
	return FALSE;
}


void WinTestRunnerApp::TestMain()
{
}


} // namespace CppUnit
