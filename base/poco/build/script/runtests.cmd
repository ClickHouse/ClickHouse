@echo off
rem
rem A script for running the POCO testsuites.
rem
rem usage: runtests [64]
rem
rem If the environment variable EXCLUDE_TESTS is set, containing
rem a space-separated list of project names (as found in the
rem components file), these tests will be skipped.
rem

setlocal EnableDelayedExpansion

set TESTRUNNER=TestSuite.exe
set TESTRUNNERARGS=/B:TestSuite.out
set BINDIR=bin

if "%1"=="64" (
  set BINDIR=bin64
)

set runs=0
set failures=0
set failedTests=
set status=0
set excluded=0

for /f %%C in ('findstr /R "." components') do (
  set excluded=0
  for %%X in (%EXCLUDE_TESTS%) do (
    if "%%X"=="%%C" (
      set excluded=1
    )
  )
  if !excluded!==0 (
    if exist %%C (
      if exist %%C\testsuite (
        if exist %%C\testsuite\%BINDIR%\%TESTRUNNER% (
          echo.
          echo.
          echo ****************************************
          echo *** %%C
          echo ****************************************
          echo.

		  set /a runs=!runs! + 1
		  set dir=%CD%
		  cd %%C\testsuite
		  %BINDIR%\%TESTRUNNER% %TESTRUNNERARGS%
		  if !ERRORLEVEL! neq 0 (
		    set /a failures=!failures! + 1
		    set failedTests=!failedTests! %%C
		    set status=1
		  )
		  if exist TestSuite.out (
		    type TestSuite.out
		  )
		  cd !dir!
        )
      )
    )
  )
)

echo.
echo.
echo !runs! runs, !failures! failed.
echo.
for %%F in (!failedTests!) do (
	echo Failed: %%F
)
echo.

exit /b !status!
