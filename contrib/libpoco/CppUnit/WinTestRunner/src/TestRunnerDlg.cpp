//
// TestRunnerDlg.cpp
//
// $Id: //poco/1.4/CppUnit/WinTestRunner/src/TestRunnerDlg.cpp#1 $
//


#include <afxwin.h>
#include <afxext.h>
#include <afxcmn.h>
#include <mmsystem.h>
#include "TestRunnerDlg.h"
#include "ActiveTest.h"
#include "GUITestResult.h"
#include "ProgressBar.h"
#include "CppUnit/TestSuite.h"
#include "TestRunnerDlg.h"


namespace CppUnit {


TestRunnerDlg::TestRunnerDlg(CWnd* pParent): CDialog(TestRunnerDlg::IDD, pParent)
{
    //{{AFX_DATA_INIT(TestRunnerDlg)
        // NOTE: the ClassWizard will add member initialization here
    //}}AFX_DATA_INIT

    _testsProgress     = 0;
    _selectedTest      = 0;
    _currentTest       = 0;
}


void TestRunnerDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialog::DoDataExchange(pDX);
	//{{AFX_DATA_MAP(TestRunnerDlg)
	// NOTE: the ClassWizard will add DDX and DDV calls here
	//}}AFX_DATA_MAP
}


BEGIN_MESSAGE_MAP(TestRunnerDlg, CDialog)
    //{{AFX_MSG_MAP(TestRunnerDlg)
    ON_BN_CLICKED(ID_RUN, OnRun)
    ON_BN_CLICKED(ID_STOP, OnStop)
    ON_CBN_SELCHANGE(IDC_COMBO_TEST, OnSelchangeComboTest)
	ON_BN_CLICKED(IDC_CHK_AUTORUN, OnBnClickedAutorun)
    ON_WM_PAINT()
    //}}AFX_MSG_MAP
END_MESSAGE_MAP()


BOOL TestRunnerDlg::OnInitDialog()
{
	CDialog::OnInitDialog();

	CListCtrl   *listCtrl = (CListCtrl *)GetDlgItem (IDC_LIST);
	CComboBox   *comboBox = (CComboBox *)GetDlgItem (IDC_COMBO_TEST);

	ASSERT (listCtrl);
	ASSERT (comboBox);

	CString title;
	GetWindowText(title);
#if defined(_DEBUG)
	title.Append(" [debug]");
#else
	title.Append(" [release]");
#endif
	SetWindowText(title);

	listCtrl->InsertColumn (0,"Type", LVCFMT_LEFT, 16 + listCtrl->GetStringWidth ("Type"), 1);
	listCtrl->InsertColumn (1,"Name", LVCFMT_LEFT, 16 * listCtrl->GetStringWidth ("X"), 2);
	listCtrl->InsertColumn (2,"Failed Condition", LVCFMT_LEFT, 24 * listCtrl->GetStringWidth ("M"), 3);
	listCtrl->InsertColumn (3,"Line", LVCFMT_LEFT, 16 + listCtrl->GetStringWidth ("0000"), 4);
	listCtrl->InsertColumn (4,"File Name", LVCFMT_LEFT, 36 * listCtrl->GetStringWidth ("M"), 5);

	int numberOfCases = 0;

	CWinApp* pApp = AfxGetApp();
	CString lastTestCS = pApp->GetProfileString("Tests", "lastTest");
	std::string lastTest((LPCSTR) lastTestCS);
	int sel = -1;
	for (std::vector<TestInfo>::iterator it = _tests.begin (); it != _tests.end (); ++it)
	{
		std::string cbName(it->level*4, ' ');
		cbName.append(it->pTest->toString());
		comboBox->AddString (cbName.c_str ());
		if (sel < 0)
		{
			if (lastTest.empty() || lastTest == it->pTest->toString())
			{
				_selectedTest = it->pTest;
				sel = numberOfCases;
			}
		}
		numberOfCases++;
	}

	if (numberOfCases > 0)
	{
		if (sel < 0)
		{
			_selectedTest = _tests[0].pTest;
			sel = 0;
		}
		comboBox->SetCurSel (sel);
	}
	else
	{
		beRunDisabled ();
	}
	CWnd *pProgress = GetDlgItem(IDC_PROGRESS);
	CRect rect;
	pProgress->GetWindowRect(&rect);
	_testsProgress = new ProgressBar (this, rect);

	CButton* autoRunBtn = (CButton*) GetDlgItem(IDC_CHK_AUTORUN);
	autoRunBtn->SetCheck(pApp->GetProfileInt("Tests", "autoRun", BST_UNCHECKED));

	reset ();

	if (autoRunBtn->GetCheck() == BST_CHECKED)
	{
		OnRun();
	}

	return TRUE;  // return TRUE unless you set the focus to a control
					// EXCEPTION: OCX Property Pages should return FALSE
}


TestRunnerDlg::~TestRunnerDlg ()
{
    freeState ();
    delete _testsProgress;
}


void TestRunnerDlg::OnRun()
{
	if (_selectedTest == 0)
		return;

    freeState       ();
    reset           ();

    beRunning       ();

    int numberOfTests = _selectedTest->countTestCases ();

    _testsProgress->start (numberOfTests);

    _result            = new GUITestResult ((TestRunnerDlg *)this);
    _activeTest        = new ActiveTest (_selectedTest);

    _testStartTime     = timeGetTime ();

    _activeTest->run (_result);

    _testEndTime       = timeGetTime ();

}


void TestRunnerDlg::addListEntry(const std::string& type, TestResult *result, Test *test, CppUnitException *e)
{
    char        stage [80];
    LV_ITEM     lvi;
    CListCtrl   *listCtrl       = (CListCtrl *)GetDlgItem (IDC_LIST);
    int         currentEntry    = result->testErrors () + result->testFailures () -1;

    sprintf (stage, "%s", type.c_str ());

    lvi.mask        = LVIF_TEXT;
    lvi.iItem       = currentEntry;
    lvi.iSubItem    = 0;
    lvi.pszText     = stage;
    lvi.iImage      = 0;
    lvi.stateMask   = 0;
    lvi.state       = 0;

    listCtrl->InsertItem (&lvi);

    // Set class string
    listCtrl->SetItemText (currentEntry, 1, test->toString ().c_str ());

    // Set the asserted text
    listCtrl->SetItemText(currentEntry, 2, e->what ());

    // Set the line number
    if (e->lineNumber () == CppUnitException::CPPUNIT_UNKNOWNLINENUMBER)
        sprintf (stage, "<unknown>");
    else
        sprintf (stage, "%ld", e->lineNumber ());

    listCtrl->SetItemText(currentEntry, 3, stage);

    // Set the file name
    listCtrl->SetItemText(currentEntry, 4, e->fileName ().c_str ());

    listCtrl->RedrawItems (currentEntry, currentEntry);
    listCtrl->UpdateWindow ();

}


void TestRunnerDlg::addError (TestResult *result, Test *test, CppUnitException *e)
{
    addListEntry ("Error", result, test, e);
    _errors++;

	_currentTest = 0;
    updateCountsDisplay ();

}


void TestRunnerDlg::addFailure (TestResult *result, Test *test, CppUnitException *e)
{
    addListEntry ("Failure", result, test, e);
    _failures++;

	_currentTest = 0;
    updateCountsDisplay ();

}


void TestRunnerDlg::startTest(Test* test)
{
	_currentTest = test;
	updateCountsDisplay();
}


void TestRunnerDlg::endTest (TestResult *result, Test *test)
{
	if (_selectedTest == 0)
		return;
	_currentTest = 0;

    _testsRun++;
    updateCountsDisplay ();
    _testsProgress->step (_failures == 0 && _errors == 0);

    _testEndTime   = timeGetTime ();

    updateCountsDisplay ();

    if (_testsRun >= _selectedTest->countTestCases ())
        beIdle ();
}


void TestRunnerDlg::beRunning ()
{
    CButton *runButton = (CButton *)GetDlgItem (ID_RUN);
    CButton *closeButton = (CButton *)GetDlgItem (IDOK);

    runButton->EnableWindow (FALSE);
    closeButton->EnableWindow (FALSE);

}


void TestRunnerDlg::beIdle ()
{
    CButton *runButton = (CButton *)GetDlgItem (ID_RUN);
    CButton *closeButton = (CButton *)GetDlgItem (IDOK);

    runButton->EnableWindow (TRUE);
    closeButton->EnableWindow (TRUE);

}


void TestRunnerDlg::beRunDisabled ()
{
    CButton *runButton = (CButton *)GetDlgItem (ID_RUN);
    CButton *closeButton = (CButton *)GetDlgItem (IDOK);
    CButton *stopButton = (CButton *)GetDlgItem (ID_STOP);

    runButton->EnableWindow (FALSE);
    stopButton->EnableWindow (FALSE);
    closeButton->EnableWindow (TRUE);

}


void TestRunnerDlg::freeState ()
{
    delete _activeTest;
    delete _result;

}


void TestRunnerDlg::reset ()
{
    _testsRun      = 0;
    _errors        = 0;
    _failures      = 0;
    _testEndTime   = _testStartTime;

    updateCountsDisplay ();

    _activeTest    = 0;
    _result        = 0;

    CListCtrl *listCtrl = (CListCtrl *)GetDlgItem (IDC_LIST);

    listCtrl->DeleteAllItems ();
    _testsProgress->reset ();

}


void TestRunnerDlg::updateCountsDisplay ()
{
    CStatic *statTestsRun   = (CStatic *)GetDlgItem (IDC_STATIC_RUNS);
    CStatic *statErrors     = (CStatic *)GetDlgItem (IDC_STATIC_ERRORS);
    CStatic *statFailures   = (CStatic *)GetDlgItem (IDC_STATIC_FAILURES);
    CEdit *editTime         = (CEdit *)GetDlgItem (IDC_EDIT_TIME);

    CString argumentString;

    argumentString.Format ("%d", _testsRun);
    statTestsRun    ->SetWindowText (argumentString);

    argumentString.Format ("%d", _errors);
    statErrors      ->SetWindowText (argumentString);

    argumentString.Format ("%d", _failures);
    statFailures    ->SetWindowText (argumentString);

	if (_currentTest)
		argumentString.Format ("Execution Time: %3.3lf seconds, Current Test: %s", (_testEndTime - _testStartTime) / 1000.0, _currentTest->toString().c_str());
	else
		argumentString.Format ("Execution Time: %3.3lf seconds", (_testEndTime - _testStartTime) / 1000.0);
		
    editTime        ->SetWindowText (argumentString);


}


void TestRunnerDlg::OnStop()
{
    if (_result)
        _result->stop ();

    beIdle ();

}


void TestRunnerDlg::OnOK()
{
    if (_result)
        _result->stop ();

    CDialog::OnOK ();
}


void TestRunnerDlg::OnSelchangeComboTest()
{
    CComboBox   *testsSelection = (CComboBox *)GetDlgItem (IDC_COMBO_TEST);

    int currentSelection = testsSelection->GetCurSel ();

    if (currentSelection >= 0 && currentSelection < _tests.size ())
    {
        _selectedTest = (_tests.begin () + currentSelection)->pTest;
        beIdle ();
		CWinApp* pApp = AfxGetApp();
		pApp->WriteProfileString("Tests", "lastTest", _selectedTest->toString().c_str());
    }
    else
    {
        _selectedTest = 0;
        beRunDisabled ();

    }

    freeState ();
    reset ();

}


void TestRunnerDlg::OnBnClickedAutorun()
{
    CButton   *autoRunBtn = (CButton *)GetDlgItem (IDC_CHK_AUTORUN);
	CWinApp* pApp = AfxGetApp();
	pApp->WriteProfileInt("Tests", "autoRun", autoRunBtn->GetCheck());
}


void TestRunnerDlg::OnPaint()
{
    CPaintDC dc (this);

    _testsProgress->paint (dc);
}


void TestRunnerDlg::setTests(const std::vector<Test*>& tests)
{
	_tests.clear();
	for (std::vector<Test*>::const_iterator it = tests.begin(); it != tests.end(); ++it)
	{
		addTest(*it, 0);
	}
}


void TestRunnerDlg::addTest(Test* pTest, int level)
{
	TestInfo ti;
	ti.pTest = pTest;
	ti.level = level;
	_tests.push_back(ti);
	TestSuite* pSuite = dynamic_cast<TestSuite*>(pTest);
	if (pSuite)
	{
		const std::vector<Test*>& tests = pSuite->tests();
		for (std::vector<Test*>::const_iterator it = tests.begin(); it != tests.end(); ++it)
		{
			addTest(*it, level + 1);
		}
	}
}


} // namespace CppUnit
