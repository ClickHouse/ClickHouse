@echo off
setlocal enableextensions enabledelayedexpansion

rem
rem buildwin.cmd
rem
rem POCO C++ Libraries command-line build script
rem for MS Visual Studio 2008 to 2013
rem
rem Copyright (c) 2006-2017 by Applied Informatics Software Engineering GmbH
rem and Contributors.
rem
rem Original version by Aleksandar Fabijanic.
rem Modified by Guenter Obiltschnig.
rem
rem Usage:
rem ------
rem buildwin VS_VERSION [ACTION] [LINKMODE] [CONFIGURATION] [PLATFORM] [SAMPLES] [TESTS] [TOOL] [ENV] [VERBOSITY [LOGGER] ]
rem VS_VERSION:    90|100|110|120|140|150|160
rem ACTION:        build|rebuild|clean
rem LINKMODE:      static_mt|static_md|shared|all
rem CONFIGURATION: release|debug|both
rem PLATFORM:      Win32|x64|WinCE|WEC2013
rem SAMPLES:       samples|nosamples
rem TESTS:         tests|notests
rem TOOL:          devenv|vcexpress|wdexpress|msbuild
rem ENV:           env|noenv (active only with msbuild, defaulted to env)
rem VERBOSITY      quiet|minimal|normal|detailed|diagnostic
rem LOGGER         <logger path> see msbuild /?
rem
rem VS_VERSION is required argument. Default is build all.

set POCO_BASE=%CD%
set PATH=%POCO_BASE%\bin64;%POCO_BASE%\bin;%PATH%

rem VS_VERSION {90 | 100 | 110 | 120 | 140 | 150 | 160}
if "%1"=="" goto usage

rem -version ^^[16.0^^,17.0^^)
set VS_VERSION=vs%1
if %VS_VERSION%==vs160 (
rem  should be set "VSWHERE='%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe  -property installationPath -version ^[16.0^,17.0^)'"
  set "VSWHERE='C:\PROGRA~2\MICROS~1\Installer\vswhere.exe  -latest -property installationPath -version ^[16.0^,17.0^)'"
) else (
if %VS_VERSION%==vs150 (
  set "VSWHERE='C:\PROGRA~2\MICROS~1\Installer\vswhere.exe  -latest -property installationPath -version ^[15.0^,16.0^)'"
)
)
for /f " delims=" %%a in (%VSWHERE%) do @set "VSCOMNTOOLS=%%a"

echo ============= %VSCOMNTOOLS% =============

if %VS_VERSION%==vs160 (
  set VS_VARSALL=..\..\VC\Auxiliary\Build\vcvarsall.bat
  set "VS160COMNTOOLS=%VSCOMNTOOLS%\Common7\Tools\"
) else (
  if %VS_VERSION%==vs150 (
    set VS_VARSALL=..\..\VC\Auxiliary\Build\vcvarsall.bat
    set "VS150COMNTOOLS=%VSCOMNTOOLS%\Common7\Tools\"
  ) else (
    set VS_VARSALL=..\..\VC\vcvarsall.bat
  )
)

shift /1
rem ACTION [build|rebuild|clean]
set ACTION=%1
if "%ACTION%"=="" (set ACTION=build)
if not "%ACTION%"=="build" (
if not "%ACTION%"=="rebuild" (
if not "%ACTION%"=="clean" goto usage))

shift /1
rem LINKMODE [static_mt|static_md|shared|all]
set LINK_MODE=%1
if "%LINK_MODE%"=="" (set LINK_MODE=all)
if not "%LINK_MODE%"=="static_mt" (
if not "%LINK_MODE%"=="static_md" (
if not "%LINK_MODE%"=="shared" (
if not "%LINK_MODE%"=="all" goto usage)))

rem CONFIGURATION [release|debug|both]
set CONFIGURATION=%2
if "%CONFIGURATION%"=="" (set CONFIGURATION=both)
if not "%CONFIGURATION%"=="release" (
if not "%CONFIGURATION%"=="debug" (
if not "%CONFIGURATION%"=="both" goto usage))

rem PLATFORM [Win32|x64|WinCE|WEC2013]
set PLATFORM=%3
if "%PLATFORM%"=="" (set PLATFORM=Win32)
if not "%PLATFORM%"=="Win32" (
if not "%PLATFORM%"=="x64" (
if not "%PLATFORM%"=="WinCE" (
if not "%PLATFORM%"=="WEC2013" goto usage)))

if "%PLATFORM%"=="Win32" (set PLATFORM_SUFFIX=) else (
if "%PLATFORM%"=="x64" (set PLATFORM_SUFFIX=_x64) else (
if "%PLATFORM%"=="WinCE" (set PLATFORM_SUFFIX=_CE) else (
if "%PLATFORM%"=="WEC2013" (set PLATFORM_SUFFIX=_WEC2013))))


rem SAMPLES [samples|nosamples]
set SAMPLES=%4
if "%SAMPLES%"=="" (set SAMPLES=samples)

rem TESTS [tests|notests]
set TESTS=%5
if "%TESTS%"=="" (set TESTS=notests)

if not defined VCINSTALLDIR (
  if %VS_VERSION%==vs90 (
    if %PLATFORM%==x64 (
      call "%VS90COMNTOOLS%%VS_VARSALL%" x86_amd64 8.1
    ) else (
      call "%VS90COMNTOOLS%%VS_VARSALL%" x86 8.1
    )
  ) else (
    if %VS_VERSION%==vs100 (
      if %PLATFORM%==x64 (
        call "%VS100COMNTOOLS%%VS_VARSALL%" x86_amd64 8.1
      ) else (
        call "%VS100COMNTOOLS%%VS_VARSALL%" x86 8.1
      )
    ) else (
      if %VS_VERSION%==vs110 (
        if %PLATFORM%==x64 (
          call "%VS110COMNTOOLS%%VS_VARSALL%" x86_amd64 8.1
        ) else (
          call "%VS110COMNTOOLS%%VS_VARSALL%" x86 8.1
        )
      ) else (
        if %VS_VERSION%==vs120 (
          if %PLATFORM%==x64 (
            call "%VS120COMNTOOLS%%VS_VARSALL%" x86_amd64 8.1
          ) else (
            call "%VS120COMNTOOLS%%VS_VARSALL%" x86 8.1
          )
        ) else (
          if %VS_VERSION%==vs140 (
            if %PLATFORM%==x64 (
              call "%VS140COMNTOOLS%%VS_VARSALL%" x86_amd64 8.1
            ) else (
              call "%VS140COMNTOOLS%%VS_VARSALL%" x86 8.1
            )
          ) else (
            if %VS_VERSION%==vs150 (
              if %PLATFORM%==x64 (
                call "%VS150COMNTOOLS%%VS_VARSALL%" x86_amd64 8.1
              ) else (
                call "%VS150COMNTOOLS%%VS_VARSALL%" x86 8.1
              )
            ) else (
              if %VS_VERSION%==vs160 (
                if %PLATFORM%==x64 (
                  call "%VS160COMNTOOLS%%VS_VARSALL%" x86_amd64 8.1
                ) else (
                  call "%VS160COMNTOOLS%%VS_VARSALL%" x86 8.1
                )
              )
            )
          )
        )
      )
    )
  )
)

if not defined VSINSTALLDIR (
  echo Error: No Visual C++ environment found.
  echo Please run this script from a Visual Studio Command Prompt
  echo or run "%%VSnnCOMNTOOLS%%\vsvars32.bat" first.
  goto :buildfailed
)

set VCPROJ_EXT=vcproj
if %VS_VERSION%==vs100 (set VCPROJ_EXT=vcxproj)
if %VS_VERSION%==vs110 (set VCPROJ_EXT=vcxproj)
if %VS_VERSION%==vs120 (set VCPROJ_EXT=vcxproj)
if %VS_VERSION%==vs140 (set VCPROJ_EXT=vcxproj)
if %VS_VERSION%==vs150 (set VCPROJ_EXT=vcxproj)
if %VS_VERSION%==vs160 (set VCPROJ_EXT=vcxproj)


rem ENV      	env|noenv
set USEENV=%7
if "%USEENV%"=="" (set USEENV=env)
if not "%USEENV%"=="env" (
if not "%USEENV%"=="noenv"  goto usage)

rem VERBOSITY      quiet|minimal|normal|detailed
set VERBOSITY=%8
if "%VERBOSITY%"=="" (set VERBOSITY=minimal)
if not "%VERBOSITY%"=="quiet" (
if not "%VERBOSITY%"=="minimal" (
if not "%VERBOSITY%"=="normal" (
if not "%VERBOSITY%"=="detailed" (
if not "%VERBOSITY%"=="diagnostic" goto usage))))

if "%6"=="" goto use_devenv
set BUILD_TOOL=%6
goto use_custom
:use_devenv
set BUILD_TOOL=devenv
if "%VS_VERSION%"=="vs100" (set BUILD_TOOL=msbuild)
if "%VS_VERSION%"=="vs110" (set BUILD_TOOL=msbuild)
if "%VS_VERSION%"=="vs120" (set BUILD_TOOL=msbuild)
if "%VS_VERSION%"=="vs140" (set BUILD_TOOL=msbuild)
if "%VS_VERSION%"=="vs150" (set BUILD_TOOL=msbuild)
if "%VS_VERSION%"=="vs160" (set BUILD_TOOL=msbuild)
:use_custom
if "%BUILD_TOOL%"=="msbuild" (
  if "%PLATFORM%"=="Win32" (set PLATFORMSW=/p:Platform=Win32) else (
  if "%PLATFORM%"=="x86"   (set PLATFORMSW=/p:Platform=Win32) else (
  if "%PLATFORM%"=="x64"   (set PLATFORMSW=/p:Platform=x64)))

  set ACTIONSW=/t:
  set CONFIGSW=/p:Configuration=
  set EXTRASW=/m
  if "%USEENV%"=="env"   set USEENVP=/p:UseEnv=true
  if "%USEENV%"=="noenv" set USEENVP=/p:UseEnv=false

  set BUILD_TOOL_FLAGS=/clp:NoSummary
  set BUILD_TOOL_FLAGS=!BUILD_TOOL_FLAGS! /nologo /v:%VERBOSITY%
)
if not "%BUILD_TOOL%"=="msbuild" (
  set USEENVP=/useenv
  set ACTIONSW=/
)


rem LOGGER         <logger path> see msbuild /?
set LOGGER=%9
if not "%LOGGER%"=="" (
  if "%BUILD_TOOL%"=="msbuild" (
    if not %LOGGER%X==X (
       set BUILD_TOOL_FLAGS=!BUILD_TOOL_FLAGS! /logger:%LOGGER%
    )
  )
)

if "%VS_VERSION%"=="vs100" (goto msbuildok)
if "%VS_VERSION%"=="vs110" (goto msbuildok)
if "%VS_VERSION%"=="vs120" (goto msbuildok)
if "%VS_VERSION%"=="vs140" (goto msbuildok)
if "%VS_VERSION%"=="vs150" (goto msbuildok)
if "%VS_VERSION%"=="vs160" (goto msbuildok)
if "%BUILD_TOOL%"=="msbuild" (
  echo "Cannot use msbuild with Visual Studio 2008 or earlier."
  exit /b 2
)
:msbuildok


if "%PLATFORM%"=="WEC2013" (
if "%WEC2013_PLATFORM%"=="" (
echo WEC2013_PLATFORM not set. Exiting.
exit /b 1
)
set PLATFORMSW=/p:Platform=%WEC2013_PLATFORM%
set USEENV=
if %VS_VERSION%==vs110 (set EXTRASW=/m /p:VisualStudioVersion=11.0)
if %VS_VERSION%==vs120 (set EXTRASW=/m /p:VisualStudioVersion=12.0)
if %VS_VERSION%==vs140 (set EXTRASW=/m /p:VisualStudioVersion=14.0)
if %VS_VERSION%==vs150 (set EXTRASW=/m /p:VisualStudioVersion=15.0)
if %VS_VERSION%==vs160 (set EXTRASW=/m /p:VisualStudioVersion=16.0)
)

set DEBUG_SHARED=0
set RELEASE_SHARED=0
set DEBUG_STATIC_MT=0
set RELEASE_STATIC_MT=0
set DEBUG_STATIC_MD=0
set RELEASE_STATIC_MD=0

if %LINK_MODE%==shared (
if %CONFIGURATION%==release (set RELEASE_SHARED=1) else (
if %CONFIGURATION%==both (set RELEASE_SHARED=1) else (
if "%CONFIGURATION%"=="" (set RELEASE_SHARED=1))))

if %LINK_MODE%==shared (
if %CONFIGURATION%==debug (set DEBUG_SHARED=1) else (
if %CONFIGURATION%==both (set DEBUG_SHARED=1) else (
if "%CONFIGURATION%"=="" (set DEBUG_SHARED=1))))

if %LINK_MODE%==static_mt (
if %CONFIGURATION%==release (set RELEASE_STATIC_MT=1) else (
if %CONFIGURATION%==both (set RELEASE_STATIC_MT=1) else (
if "%CONFIGURATION%"=="" (set RELEASE_STATIC_MT=1))))

if %LINK_MODE%==static_md (
if %CONFIGURATION%==release (set RELEASE_STATIC_MD=1) else (
if %CONFIGURATION%==both (set RELEASE_STATIC_MD=1) else (
if "%CONFIGURATION%"=="" (set RELEASE_STATIC_MD=1))))

if %LINK_MODE%==static_mt (
if %CONFIGURATION%==debug (set DEBUG_STATIC_MT=1) else (
if %CONFIGURATION%==both (set DEBUG_STATIC_MT=1) else (
if "%CONFIGURATION%"=="" (set DEBUG_STATIC_MT=1))))

if %LINK_MODE%==static_md (
if %CONFIGURATION%==debug (set DEBUG_STATIC_MD=1) else (
if %CONFIGURATION%==both (set DEBUG_STATIC_MD=1) else (
if "%CONFIGURATION%"=="" (set DEBUG_STATIC_MD=1))))

if %LINK_MODE%==all (
if %CONFIGURATION%==debug (
set DEBUG_STATIC_MT=1
set DEBUG_STATIC_MD=1
set DEBUG_SHARED=1) else (
if %CONFIGURATION%==release (
set RELEASE_STATIC_MT=1
set RELEASE_STATIC_MD=1
set RELEASE_SHARED=1) else (
if %CONFIGURATION%==both (
set DEBUG_STATIC_MT=1
set DEBUG_STATIC_MD=1
set DEBUG_SHARED=1
set RELEASE_STATIC_MT=1
set RELEASE_STATIC_MD=1
set RELEASE_SHARED=1) else (
if "%CONFIGURATION%"=="" (
set DEBUG_STATIC_MT=1
set DEBUG_STATIC_MD=1
set DEBUG_SHARED=1
set RELEASE_STATIC_MT=1
set RELEASE_STATIC_MD=1
set RELEASE_SHARED=1)))))

if "%LINK_MODE%"=="" (
if %CONFIGURATION%==debug (
set DEBUG_STATIC_MT=1
set DEBUG_STATIC_MD=1
set DEBUG_SHARED=1) else (
if %CONFIGURATION%==release (
set RELEASE_STATIC_MT=1
set RELEASE_STATIC_MD=1
set RELEASE_SHARED=1) else (
if %CONFIGURATION%==both (
set DEBUG_STATIC_MT=1
set DEBUG_STATIC_MD=1
set DEBUG_SHARED=1
set RELEASE_STATIC_MT=1
set RELEASE_STATIC_MD=1
set RELEASE_SHARED=1) else (
if "%CONFIGURATION%"=="" (
set DEBUG_STATIC_MT=1
set DEBUG_STATIC_MD=1
set DEBUG_SHARED=1
set RELEASE_STATIC_MT=1
set RELEASE_STATIC_MD=1
set RELEASE_SHARED=1)))))

echo.
echo.
echo ########################################################################
echo ####
echo #### STARTING VISUAL STUDIO BUILD (%VS_VERSION%, %PLATFORM%)
echo ####
echo ########################################################################
echo.
echo.
echo buildwin %VS_VERSION% %ACTION% %LINK_MODE% %CONFIGURATION% %PLATFORM% %SAMPLES% %TESTS% !BUILD_TOOL! %USEENV% %VERBOSITY%
echo.
echo The following configurations will be built:

if %DEBUG_SHARED%==1      (echo debug_shared)
if %RELEASE_SHARED%==1    (echo release_shared)
if %DEBUG_STATIC_MT%==1   (echo debug_static_mt)
if %DEBUG_STATIC_MD%==1   (echo debug_static_md)
if %RELEASE_STATIC_MT%==1 (echo release_static_mt)
if %RELEASE_STATIC_MD%==1 (echo release_static_md)

rem build for up to 4 levels deep
for /f %%G in ('findstr /R "." components') do (
  if exist %%G (
    cd %%G
    for /f "tokens=1,2,3,4 delims=/" %%Q in ("%%G") do (
      set PROJECT_FILE=%%Q%PLATFORM_SUFFIX%_%VS_VERSION%.%VCPROJ_EXT%
      set TEST_PROJECT_FILE=testsuite/TestSuite%PLATFORM_SUFFIX%_%VS_VERSION%.%VCPROJ_EXT%
      set TEST_APP_PROJECT_FILE=testsuite/TestApp%PLATFORM_SUFFIX%_%VS_VERSION%.%VCPROJ_EXT%
      set TEST_LIB_PROJECT_FILE=testsuite/TestLibrary%PLATFORM_SUFFIX%_%VS_VERSION%.%VCPROJ_EXT%
      if exist !PROJECT_FILE! (
        call :build %%G
        if ERRORLEVEL 1 goto buildfailed
      )
      set PROJECT_FILE=%%R%PLATFORM_SUFFIX%_%VS_VERSION%.%VCPROJ_EXT%
      if exist !PROJECT_FILE! (
        call :build %%G
        if ERRORLEVEL 1 goto buildfailed
      )
      set PROJECT_FILE=%%S%PLATFORM_SUFFIX%_%VS_VERSION%.%VCPROJ_EXT%
      if exist !PROJECT_FILE! (
        call :build %%G
        if ERRORLEVEL 1 goto buildfailed
      )
      set PROJECT_FILE=%%T%PLATFORM_SUFFIX%_%VS_VERSION%.%VCPROJ_EXT%
      if exist !PROJECT_FILE! (
        call :build %%G
        if ERRORLEVEL 1 goto buildfailed
      )
    )
  )
  cd "%POCO_BASE%"
)

goto samples

rem ////////////////////
rem / build subroutine /
rem ////////////////////
:build

echo.
echo ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
echo ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
echo ++++ Building [!PROJECT_FILE!]
echo ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
echo ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
echo.

if %DEBUG_SHARED%==1 (
  echo !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_shared %PLATFORMSW% !PROJECT_FILE!
  !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_shared %PLATFORMSW% !PROJECT_FILE!
  if ERRORLEVEL 1 exit /b 1
  echo. && echo.
  if %TESTS%==tests (
    if exist !TEST_PROJECT_FILE! (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_shared %PLATFORMSW% !TEST_PROJECT_FILE!
      if ERRORLEVEL 1 exit /b 1
      if %1==Foundation (
        echo.
        !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_shared %PLATFORMSW% !TEST_APP_PROJECT_FILE!
        echo.
        !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_shared %PLATFORMSW% !TEST_LIB_PROJECT_FILE!
        if ERRORLEVEL 1 exit /b 1
      )
      echo. && echo.
    )
  )
)
if %RELEASE_SHARED%==1 (
  echo   !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_shared %PLATFORMSW% !PROJECT_FILE!

  !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_shared %PLATFORMSW% !PROJECT_FILE!
  if ERRORLEVEL 1 exit /b 1
  echo. && echo.
  if %TESTS%==tests (
    if exist !TEST_PROJECT_FILE! (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_shared %PLATFORMSW% !TEST_PROJECT_FILE!
      if ERRORLEVEL 1 exit /b 1
      if %1==Foundation (
        echo.
        !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_shared %PLATFORMSW% !TEST_APP_PROJECT_FILE!
        echo.
        !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_shared %PLATFORMSW% !TEST_LIB_PROJECT_FILE!
        if ERRORLEVEL 1 exit /b 1
      )
      echo. && echo.
    )
  )
)
if %DEBUG_STATIC_MT%==1 (
  echo !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_static_mt %PLATFORMSW% !PROJECT_FILE!
  !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_static_mt %PLATFORMSW% !PROJECT_FILE!
  if ERRORLEVEL 1 exit /b 1
  echo. && echo.
  if %TESTS%==tests (
    if exist !TEST_PROJECT_FILE! (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_static_mt %PLATFORMSW% !TEST_PROJECT_FILE!
      if ERRORLEVEL 1 exit /b 1
      if %1==Foundation (
        echo.
        !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_static_mt %PLATFORMSW% !TEST_APP_PROJECT_FILE!
        if ERRORLEVEL 1 exit /b 1
      )
      echo. && echo.
    )
  )
)
if %RELEASE_STATIC_MT%==1 (
  echo !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_static_mt %PLATFORMSW% !PROJECT_FILE!
  !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_static_mt %PLATFORMSW% !PROJECT_FILE!
  if ERRORLEVEL 1 exit /b 1
  echo. && echo.
  if %TESTS%==tests (
    if exist !TEST_PROJECT_FILE! (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_static_mt %PLATFORMSW% !TEST_PROJECT_FILE!
      if ERRORLEVEL 1 exit /b 1
      if %1==Foundation (
        echo.
        !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_static_mt %PLATFORMSW% !TEST_APP_PROJECT_FILE!
        if ERRORLEVEL 1 exit /b 1
      )
      echo. && echo.
    )
  )
)
if %DEBUG_STATIC_MD%==1 (
  echo !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_static_md %PLATFORMSW% !PROJECT_FILE!
  !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_static_md %PLATFORMSW% !PROJECT_FILE!
  if ERRORLEVEL 1 exit /b 1
  echo. && echo.
  if %TESTS%==tests (
    if exist !TEST_PROJECT_FILE! (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_static_md %PLATFORMSW% !TEST_PROJECT_FILE!
      if ERRORLEVEL 1 exit /b 1
      if %1==Foundation (
        echo.
        !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_static_md %PLATFORMSW% !TEST_APP_PROJECT_FILE!
        if ERRORLEVEL 1 exit /b 1
      )
      echo. && echo.
    )
  )
)
if %RELEASE_STATIC_MD%==1 (
  echo !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_static_md %PLATFORMSW% !PROJECT_FILE!
  !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_static_md %PLATFORMSW% !PROJECT_FILE!
  if ERRORLEVEL 1 exit /b 1
  echo. && echo.
  if %TESTS%==tests (
    if exist !TEST_PROJECT_FILE! (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_static_md %PLATFORMSW% !TEST_PROJECT_FILE!
      if ERRORLEVEL 1 exit /b 1
      if %1==Foundation (
        echo.
        !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_static_md %PLATFORMSW% !TEST_APP_PROJECT_FILE!
        if ERRORLEVEL 1 exit /b 1
      )
      echo. && echo.
    )
  )
)

echo.
echo ------------------------------------------------------------------------
echo ------------------------------------------------------------------------
echo ---- Done building [!PROJECT_FILE!]
echo ------------------------------------------------------------------------
echo ------------------------------------------------------------------------
echo.

exit /b

rem ////////////////////
rem / build samples /
rem ////////////////////

:samples

if %SAMPLES%==nosamples goto :EOF

rem root level component samples
for /f %%G in ('findstr /R "." components') do (
  if exist %%G\samples\samples%PLATFORM_SUFFIX%_%VS_VERSION%.sln (
    cd %%G\samples
    echo.
    echo.
    echo ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    echo ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    echo ++++ Building [%%G/samples]
    echo ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    echo ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    echo.
    set SOLUTION_FILE=samples%PLATFORM_SUFFIX%_%VS_VERSION%.sln

    if %DEBUG_SHARED%==1 (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_shared %PLATFORMSW% !SOLUTION_FILE!
      if ERRORLEVEL 1 goto buildfailed
      echo. && echo.
    )
    if %RELEASE_SHARED%==1 (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_shared %PLATFORMSW% !SOLUTION_FILE!
      if ERRORLEVEL 1 goto buildfailed
      echo. && echo.
    )
    if %DEBUG_STATIC_MT%==1 (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_static_mt %PLATFORMSW% !SOLUTION_FILE!
      if ERRORLEVEL 1 goto buildfailed
      echo. && echo.
    )
    if %RELEASE_STATIC_MT%==1 (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_static_mt %PLATFORMSW% !SOLUTION_FILE!
      if ERRORLEVEL 1 goto buildfailed
      echo. && echo.
    )
    if %DEBUG_STATIC_MD%==1 (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%debug_static_md %PLATFORMSW% !SOLUTION_FILE!
      if ERRORLEVEL 1 goto buildfailed
      echo. && echo.
    )
    if %RELEASE_STATIC_MD%==1 (
      !BUILD_TOOL! !BUILD_TOOL_FLAGS! !USEENVP! %EXTRASW% %ACTIONSW%%ACTION% %CONFIGSW%release_static_md %PLATFORMSW% !SOLUTION_FILE!
      if ERRORLEVEL 1 goto buildfailed
      echo. && echo.
    )

    cd "%POCO_BASE%"

    echo.
    echo ------------------------------------------------------------------------
    echo ------------------------------------------------------------------------
    echo ---- Done building [%%G/samples]
    echo ------------------------------------------------------------------------
    echo ------------------------------------------------------------------------
    echo.
  )
)

echo.
echo ------------------------------------------------------------------------
echo ------------------------------------------------------------------------
echo ---- Build completed.
echo ------------------------------------------------------------------------
echo ------------------------------------------------------------------------
echo.

goto :EOF

rem ////////////////
rem / build failed /
rem ////////////////

:buildfailed

echo.
echo.
echo XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
echo XXX  BUILD FAILED. EXITING. XXX
echo XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
echo.
echo.
exit /b 1

:usage
echo Usage:
echo ------
echo buildwin VS_VERSION [ACTION] [LINKMODE] [CONFIGURATION] [PLATFORM] [SAMPLES] [TESTS] [TOOL] [ENV] [VERBOSITY]
echo VS_VERSION:    "90|100|110|120|140|150"
echo ACTION:        "build|rebuild|clean"
echo LINKMODE:      "static_mt|static_md|shared|all"
echo CONFIGURATION: "release|debug|both"
echo PLATFORM:      "Win32|x64|WinCE|WEC2013"
echo SAMPLES:       "samples|nosamples"
echo TESTS:         "tests|notests"
echo TOOL:          "devenv|vcexpress|wdexpress|msbuild"
echo ENV:           "env|noenv" (active only with msbuild, defaulted to env)
echo VERBOSITY:     "quiet|minimal|normal|detailed|diagnostic" only for msbuild
echo.
echo Default is build all.
endlocal
