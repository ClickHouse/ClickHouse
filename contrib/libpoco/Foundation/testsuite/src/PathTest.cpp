//
// PathTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/PathTest.cpp#2 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "PathTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Path.h"
#include "Poco/Exception.h"
#include "Poco/Random.h"
#include "Poco/Environment.h"
#include <iostream>

#if defined(POCO_OS_FAMILY_WINDOWS) && defined(POCO_WIN32_UTF8)
#if defined(_WIN32_WCE)
#include "Poco/Path_WINCE.h"
#else
#include "Poco/Path_WIN32U.h"
#endif
#elif defined(POCO_OS_FAMILY_WINDOWS)
#include "Poco/Path_WIN32.h"
#endif

using Poco::Path;
using Poco::PathSyntaxException;
using Poco::Environment;


PathTest::PathTest(const std::string& name): CppUnit::TestCase(name)
{
}


PathTest::~PathTest()
{
}


void PathTest::testParseUnix1()
{
	Path p;
	p.parse("", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "");

	p.parse("/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/");

	p.parse("/usr", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr");

	p.parse("/usr/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/");

	p.parse("usr/", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "usr/");

	p.parse("usr", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 0);
	assert (p[0] == "usr");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "usr");
	
	p.parse("/usr/local", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local");
}


void PathTest::testParseUnix2()
{
	Path p;
	p.parse("/usr/local/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/");

	p.parse("usr/local/", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "usr/local/");

	p.parse("usr/local", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "usr/local");

	p.parse("/usr/local/bin", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/bin");

	p.parse("/usr/local/bin/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/bin/");
}


void PathTest::testParseUnix3()
{
	Path p;
	p.parse("//usr/local/bin/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/bin/");

	p.parse("/usr//local/bin/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/bin/");

	p.parse("/usr/local//bin/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/bin/");

	p.parse("/usr/local/bin//", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/bin/");
	
	p.parse("/usr/local/./bin/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/bin/");

	p.parse("./usr/local/bin/", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "usr/local/bin/");

	p.parse("./usr/local/bin/./", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "usr/local/bin/");

	p.parse("./usr/local/bin/.", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "usr/local/bin/.");
}


void PathTest::testParseUnix4()
{
	Path p;
	p.parse("/usr/local/lib/../bin/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/bin/");

	p.parse("/usr/local/lib/../bin/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/bin/");

	p.parse("/usr/local/lib/../../", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/");

	p.parse("/usr/local/lib/..", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "lib");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/lib/..");

	p.parse("../usr/local/lib/", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 4);
	assert (p[0] == "..");
	assert (p[1] == "usr");
	assert (p[2] == "local");
	assert (p[3] == "lib");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "../usr/local/lib/");

	p.parse("/usr/../lib/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "lib");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/lib/");

	p.parse("/usr/../../lib/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "lib");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/lib/");

	p.parse("local/../../lib/", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "..");
	assert (p[1] == "lib");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "../lib/");
	
	p.parse("a/b/c/d", Path::PATH_UNIX);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "a");
	assert (p[1] == "b");
	assert (p[2] == "c");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "a/b/c/d");
}


void PathTest::testParseUnix5()
{
	Path p;
	p.parse("/c:/windows/system32/", Path::PATH_UNIX);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.getDevice() == "c");
	assert (p.depth() == 2);
	assert (p[0] == "windows");
	assert (p[1] == "system32");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/c:/windows/system32/");	
}


void PathTest::testParseWindows1()
{
	Path p;
	p.parse("", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "");

	p.parse("/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\");

	p.parse("\\", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\");

	p.parse("/usr", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr");

	p.parse("\\usr", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr");

	p.parse("/usr/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\");

	p.parse("\\usr\\", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\");
}


void PathTest::testParseWindows2()
{
	Path p;
	p.parse("usr/", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "usr\\");

	p.parse("usr", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 0);
	assert (p[0] == "usr");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "usr");

	p.parse("usr\\", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "usr\\");
	
	p.parse("/usr/local", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local");

	p.parse("\\usr\\local", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local");

	p.parse("/usr/local/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\");

	p.parse("usr/local/", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "usr\\local\\");

	p.parse("usr/local", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "usr\\local");

	p.parse("/usr/local/bin", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\bin");

	p.parse("/usr/local/bin/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\bin\\");

	p.parse("/usr//local/bin/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\bin\\");

	p.parse("/usr/local//bin/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\bin\\");

	p.parse("/usr/local/bin//", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\bin\\");
}

	
void PathTest::testParseWindows3()
{
	Path p;
	p.parse("/usr/local/./bin/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\bin\\");

	p.parse("./usr/local/bin/", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "usr\\local\\bin\\");

	p.parse("./usr/local/bin/./", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "usr\\local\\bin\\");

	p.parse("./usr/local/bin/.", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "usr\\local\\bin\\.");

	p.parse("/usr/local/lib/../bin/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\bin\\");

	p.parse("/usr/local/lib/../bin/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\bin\\");

	p.parse("\\usr\\local\\lib\\..\\bin\\", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\bin\\");

	p.parse("/usr/local/lib/../../", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "usr");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\");

	p.parse("/usr/local/lib/..", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "lib");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\usr\\local\\lib\\..");

	p.parse("../usr/local/lib/", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 4);
	assert (p[0] == "..");
	assert (p[1] == "usr");
	assert (p[2] == "local");
	assert (p[3] == "lib");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "..\\usr\\local\\lib\\");

	p.parse("/usr/../lib/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "lib");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\lib\\");

	p.parse("/usr/../../lib/", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "lib");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\lib\\");

	p.parse("local/../../lib/", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "..");
	assert (p[1] == "lib");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "..\\lib\\");
}


void PathTest::testParseWindows4()
{
	Path p;
	p.parse("\\\\server\\files", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "files");
	assert (p.getNode() == "server");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\\\server\\files\\");

	p.parse("\\\\server\\files\\", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "files");
	assert (p.getNode() == "server");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\\\server\\files\\");
	
	p.parse("\\\\server\\files\\file", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "files");
	assert (p.getNode() == "server");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\\\server\\files\\file");

	p.parse("\\\\server\\files\\dir\\file", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "files");
	assert (p[1] == "dir");
	assert (p.getNode() == "server");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\\\server\\files\\dir\\file");

	p.parse("\\\\server\\files\\dir\\file", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "files");
	assert (p[1] == "dir");
	assert (p.getNode() == "server");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\\\server\\files\\dir\\file");	

	p.parse("\\\\server", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.getNode() == "server");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\\\server\\");
	
	p.parse("c:\\", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.getDevice() == "c");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "c:\\");	

	p.parse("c:\\WinNT", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.getDevice() == "c");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "c:\\WinNT");	

	p.parse("c:\\WinNT\\", Path::PATH_WINDOWS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "WinNT");
	assert (p.getDevice() == "c");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "c:\\WinNT\\");
	
	try
	{
		p.parse("~:\\", Path::PATH_WINDOWS);
		fail("bad path - must throw exception");
	}
	catch (PathSyntaxException&)
	{
	}
	
	try
	{
		p.parse("c:file.txt", Path::PATH_WINDOWS);
		fail("bad path - must throw exception");
	}
	catch (PathSyntaxException&)
	{
	}
	
	p.parse("a\\b\\c\\d", Path::PATH_WINDOWS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "a");
	assert (p[1] == "b");
	assert (p[2] == "c");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "a\\b\\c\\d");
}


void PathTest::testParseVMS1()
{
	Path p;
	p.parse("", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "");
	
	p.parse("[]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "");
	
	p.parse("[foo]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]");

	p.parse("[.foo]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[.foo]");

	p.parse("[foo.bar]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "foo");
	assert (p[1] == "bar");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo.bar]");

	p.parse("[.foo.bar]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "foo");
	assert (p[1] == "bar");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[.foo.bar]");
	
	p.parse("[foo.bar.foobar]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "foo");
	assert (p[1] == "bar");
	assert (p[2] == "foobar");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo.bar.foobar]");

	p.parse("[.foo.bar.foobar]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "foo");
	assert (p[1] == "bar");
	assert (p[2] == "foobar");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[.foo.bar.foobar]");
}


void PathTest::testParseVMS2()
{
	Path p;
	p.parse("[foo][bar]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "foo");
	assert (p[1] == "bar");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo.bar]");
	
	p.parse("[foo.][bar]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "foo");
	assert (p[1] == "bar");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo.bar]");
	
	p.parse("[foo.bar][foo]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "foo");
	assert (p[1] == "bar");
	assert (p[2] == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo.bar.foo]");
	
	try
	{
		p.parse("[foo.bar][.foo]", Path::PATH_VMS);
		failmsg("bad path - must throw exception");
	}
	catch (PathSyntaxException&)
	{
	}

	try
	{
		p.parse("[.foo.bar][foo]", Path::PATH_VMS);
		failmsg("bad path - must throw exception");
	}
	catch (PathSyntaxException&)
	{
	}
	
	p.parse("[-]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "..");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[-]");

	p.parse("[--]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "..");
	assert (p[1] == "..");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[--]");

	p.parse("[---]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "..");
	assert (p[1] == "..");
	assert (p[2] == "..");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[---]");

	p.parse("[-.-]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "..");
	assert (p[1] == "..");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[--]");

	p.parse("[.-.-]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "..");
	assert (p[1] == "..");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[--]");

	p.parse("[-.-.-]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "..");
	assert (p[1] == "..");
	assert (p[2] == "..");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[---]");

	p.parse("[.-.-.-]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "..");
	assert (p[1] == "..");
	assert (p[2] == "..");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[---]");

	p.parse("[.--.-]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "..");
	assert (p[1] == "..");
	assert (p[2] == "..");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[---]");

	p.parse("[--.-]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "..");
	assert (p[1] == "..");
	assert (p[2] == "..");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[---]");
}


void PathTest::testParseVMS3()
{
	Path p;
	p.parse("[foo][-]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]");

	p.parse("[foo][--]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]");

	p.parse("[foo][-.-]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]");

	p.parse("[foo][bar.-]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]");

	p.parse("[foo][bar.foo.-]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "foo");
	assert (p[1] == "bar");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo.bar]");

	p.parse("[foo][bar.foo.--]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]");

	p.parse("[foo][bar.foo.---]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]");

	p.parse("[foo][bar.foo.-.-.-]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]");
}


void PathTest::testParseVMS4()
{
	Path p;
	p.parse("device:[foo]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.getDevice() == "device");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "device:[foo]");

	p.parse("device:[.foo]", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.getDevice() == "device");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "device:[.foo]");

	p.parse("node::device:[foo]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (p.getNode() == "node");
	assert (p.getDevice() == "device");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "node::device:[foo]");

	p.parse("node::device:[foo.bar]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "foo");
	assert (p[1] == "bar");
	assert (p.getNode() == "node");
	assert (p.getDevice() == "device");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "node::device:[foo.bar]");
	
	p.parse("node::device:[foo.bar.][goo]", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 3);
	assert (p[0] == "foo");
	assert (p[1] == "bar");
	assert (p[2] == "goo");
	assert (p.getNode() == "node");
	assert (p.getDevice() == "device");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "node::device:[foo.bar.goo]");
	
	p.parse("[]foo.txt", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 0);
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "foo.txt");

	p.parse("[foo]bar.txt", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]bar.txt");

	p.parse("[foo]bar.txt;", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]bar.txt");

	p.parse("[foo]bar.txt;5", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]bar.txt;5");
	assert (p.version() == "5");

	p.parse("foo:bar.txt", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.getDevice() == "foo");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "foo:bar.txt");

	p.parse("foo:bar.txt;5", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.getDevice() == "foo");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "foo:bar.txt;5");
	assert (p.version() == "5");

	p.parse("foo:", Path::PATH_VMS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.getDevice() == "foo");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_VMS) == "foo:");

	p.parse("bar.txt", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 0);
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "bar.txt");

	p.parse("bar.txt;5", Path::PATH_VMS);
	assert (p.isRelative());
	assert (!p.isAbsolute());
	assert (p.depth() == 0);
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "bar.txt;5");
	assert (p.version() == "5");
}


void PathTest::testParseGuess()
{
	Path p;

	p.parse("foo:bar.txt;5", Path::PATH_GUESS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.getDevice() == "foo");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "foo:bar.txt;5");
	assert (p.version() == "5");

	p.parse("/usr/local/bin", Path::PATH_GUESS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 2);
	assert (p[0] == "usr");
	assert (p[1] == "local");
	assert (p[2] == "bin");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/bin");

	p.parse("\\\\server\\files", Path::PATH_GUESS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "files");
	assert (p.getNode() == "server");
	assert (p.isDirectory());
	assert (!p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "\\\\server\\files\\");

	p.parse("c:\\WinNT", Path::PATH_GUESS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.getDevice() == "c");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_WINDOWS) == "c:\\WinNT");	

	p.parse("foo:bar.txt;5", Path::PATH_GUESS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 0);
	assert (p.getDevice() == "foo");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "foo:bar.txt;5");
	assert (p.version() == "5");

	p.parse("[foo]bar.txt", Path::PATH_GUESS);
	assert (!p.isRelative());
	assert (p.isAbsolute());
	assert (p.depth() == 1);
	assert (p[0] == "foo");
	assert (!p.isDirectory());
	assert (p.isFile());
	assert (p.toString(Path::PATH_VMS) == "[foo]bar.txt");
}


void PathTest::testTryParse()
{
	Path p;
#if defined(POCO_OS_FAMILY_UNIX)
	assert (p.tryParse("/etc/passwd"));
	assert (p.toString() == "/etc/passwd");
#elif defined(POCO_OS_FAMILY_WINDOWS)
	assert (p.tryParse("c:\\windows\\system32"));
	assert (p.toString() == "c:\\windows\\system32");
	assert (!p.tryParse("c:foo.bar"));
	assert (p.toString() == "c:\\windows\\system32");
#endif

	assert (p.tryParse("c:\\windows\\system", Path::PATH_WINDOWS));
	assert (p.toString(Path::PATH_WINDOWS) == "c:\\windows\\system");
	assert (!p.tryParse("c:foo.bar", Path::PATH_WINDOWS));
	assert (p.toString(Path::PATH_WINDOWS) == "c:\\windows\\system");
}


void PathTest::testStatics()
{
	std::string s = Path::current();
	assert (!s.empty());
	Path p(s);
	assert (p.isDirectory() && p.isAbsolute());
	
	s = Path::home();
	assert (!s.empty());
	p = s;
	assert (p.isDirectory() && p.isAbsolute());
	
	s = Path::temp();
	assert (!s.empty());
	p = s;
	assert (p.isDirectory() && p.isAbsolute());

	s = Path::null();
	assert (!s.empty());
	p = s;
}


void PathTest::testBaseNameExt()
{
	Path p("foo.bar");
	assert (p.getFileName() == "foo.bar");
	assert (p.getBaseName() == "foo");
	assert (p.getExtension() == "bar");
	
	p.setBaseName("readme");
	assert (p.getFileName() == "readme.bar");
	assert (p.getBaseName() == "readme");
	assert (p.getExtension() == "bar");
	
	p.setExtension("txt");
	assert (p.getFileName() == "readme.txt");
	assert (p.getBaseName() == "readme");
	assert (p.getExtension() == "txt");
	
	p.setExtension("html");
	assert (p.getFileName() == "readme.html");
	assert (p.getBaseName() == "readme");
	assert (p.getExtension() == "html");

	p.setBaseName("index");
	assert (p.getFileName() == "index.html");
	assert (p.getBaseName() == "index");
	assert (p.getExtension() == "html");
}


void PathTest::testAbsolute()
{
	Path base("C:\\Program Files\\", Path::PATH_WINDOWS);
	Path rel("Poco");
	Path abs = rel.absolute(base);
	assert (abs.toString(Path::PATH_WINDOWS) == "C:\\Program Files\\Poco");
	
	base.parse("/usr/local", Path::PATH_UNIX);
	rel.parse("Poco/include", Path::PATH_UNIX);
	abs = rel.absolute(base);
	assert (abs.toString(Path::PATH_UNIX) == "/usr/local/Poco/include");

	base.parse("/usr/local/bin", Path::PATH_UNIX);
	rel.parse("../Poco/include", Path::PATH_UNIX);
	abs = rel.absolute(base);
	assert (abs.toString(Path::PATH_UNIX) == "/usr/local/Poco/include");
}


void PathTest::testRobustness()
{
	Poco::Random r;
	for (int i = 0; i < 256; ++i)
	{
		int len = r.next(1024);
		std::string s;
		for (int i = 0; i < len; ++i) s += r.nextChar();
		try
		{
			Path p(s, Path::PATH_WINDOWS);
		}
		catch (PathSyntaxException&)
		{
		}
		try
		{
			Path p(s, Path::PATH_UNIX);
		}
		catch (PathSyntaxException&)
		{
		}
		try
		{
			Path p(s, Path::PATH_VMS);
		}
		catch (PathSyntaxException&)
		{
		}
		try
		{
			Path p(s, Path::PATH_GUESS);
		}
		catch (PathSyntaxException&)
		{
		}
	}
}


void PathTest::testParent()
{
	Path p("/usr/local/include", Path::PATH_UNIX);
	p.makeParent();
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/");
	p.makeParent();
	assert (p.toString(Path::PATH_UNIX) == "/usr/");
	p.makeParent();
	assert (p.toString(Path::PATH_UNIX) == "/");
	p.makeParent();
	assert (p.toString(Path::PATH_UNIX) == "/");
}


void PathTest::testForDirectory()
{
	Path p = Path::forDirectory("/usr/local/include", Path::PATH_UNIX);
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/include/");

	p = Path::forDirectory("/usr/local/include/", Path::PATH_UNIX);
	assert (p.toString(Path::PATH_UNIX) == "/usr/local/include/");
}


void PathTest::testExpand()
{
#if defined(POCO_OS_FAMILY_UNIX)
	std::string s = Path::expand("~/.bashrc");
	assert (s == Path::expand("$HOME/.bashrc"));
	assert (s == Environment::get("HOME") + "/.bashrc" || 
	        s == Environment::get("HOME") + "//.bashrc");
	Path p(s);
	s = Path::expand("$HOME/.bashrc");
	assert (s == Path::expand("~/.bashrc"));
	s = Path::expand("${HOME}/.bashrc");
	assert (s == Path::expand("~/.bashrc"));
#elif defined(POCO_OS_FAMILY_WINDOWS)
	std::string s = Path::expand("%TMP%\\foo");
	assert (s == Environment::get("TMP") + "\\foo");
	Path p(s);
#else
	std::string s = Path::expand("SYS$LOGIN:[projects]");
	assert (s.find(":[projects]") != std::string::npos);
	Path p(s);
#endif
}


void PathTest::testListRoots()
{
	std::vector<std::string> devs;
	Path::listRoots(devs);
	assert (devs.size() > 0);
	for (std::vector<std::string>::iterator it = devs.begin(); it != devs.end(); ++it)
	{
		std::cout << *it << std::endl;
	}
}


void PathTest::testFind()
{
	Path p;
#if defined(POCO_OS_FAMILY_UNIX)
	bool found = Path::find(Environment::get("PATH"), "ls", p);
	bool notfound = Path::find(Environment::get("PATH"), "xxxyyy123", p);
#elif defined(POCO_OS_FAMILY_WINDOWS)
#if defined(_WIN32_WCE)
	return;
#endif
	bool found = Path::find(Environment::get("PATH"), "cmd.exe", p);
	bool notfound = Path::find(Environment::get("PATH"), "xxxyyy123.zzz", p);
#else
	bool found = true;
	bool notfound = false;
#endif
	assert (found);
	assert (!notfound);
	
	std::string fn = p.toString();
	assert (fn.size() > 0);
}


void PathTest::testSwap()
{
	Path p1("c:\\temp\\foo.bar");
	Path p2("\\\\server\\files\\foo.bar");
	p1.swap(p2);
	assert (p1.toString() == "\\\\server\\files\\foo.bar");
	assert (p2.toString() == "c:\\temp\\foo.bar");
}


void PathTest::testResolve()
{
	Path p("c:\\foo\\", Path::PATH_WINDOWS);
	p.resolve("test.dat");
	assert (p.toString(Path::PATH_WINDOWS) == "c:\\foo\\test.dat");
	
	p.assign("c:\\foo\\", Path::PATH_WINDOWS);
	p.resolve(Path("d:\\bar.txt", Path::PATH_WINDOWS));
	assert (p.toString(Path::PATH_WINDOWS) == "d:\\bar.txt");
	
	p.assign("c:\\foo\\bar.txt", Path::PATH_WINDOWS);
	p.resolve("foo.txt");
	assert (p.toString(Path::PATH_WINDOWS) == "c:\\foo\\foo.txt");
	
	p.assign("c:\\foo\\bar\\", Path::PATH_WINDOWS);
	p.resolve(Path("..\\baz\\test.dat", Path::PATH_WINDOWS));
	assert (p.toString(Path::PATH_WINDOWS) == "c:\\foo\\baz\\test.dat");
}


void PathTest::testPushPop()
{
	Path p;
	p.pushDirectory("a");
	p.pushDirectory("b");
	p.pushDirectory("c");
	assert (p.toString(Path::PATH_UNIX) == "a/b/c/");

	p.popDirectory();
	assert (p.toString(Path::PATH_UNIX) == "a/b/");

	p.popFrontDirectory();
	assert (p.toString(Path::PATH_UNIX) == "b/");
}


void PathTest::testWindowsSystem()
{
#if defined(POCO_OS_FAMILY_WINDOWS)
	std::cout << Poco::PathImpl::systemImpl() << std::endl;
#endif
}


void PathTest::setUp()
{
}


void PathTest::tearDown()
{
}


CppUnit::Test* PathTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("PathTest");

	CppUnit_addTest(pSuite, PathTest, testParseUnix1);
	CppUnit_addTest(pSuite, PathTest, testParseUnix2);
	CppUnit_addTest(pSuite, PathTest, testParseUnix3);
	CppUnit_addTest(pSuite, PathTest, testParseUnix4);
	CppUnit_addTest(pSuite, PathTest, testParseUnix5);
	CppUnit_addTest(pSuite, PathTest, testParseWindows1);
	CppUnit_addTest(pSuite, PathTest, testParseWindows2);
	CppUnit_addTest(pSuite, PathTest, testParseWindows3);
	CppUnit_addTest(pSuite, PathTest, testParseWindows4);
	CppUnit_addTest(pSuite, PathTest, testParseVMS1);
	CppUnit_addTest(pSuite, PathTest, testParseVMS2);
	CppUnit_addTest(pSuite, PathTest, testParseVMS3);
	CppUnit_addTest(pSuite, PathTest, testParseVMS4);
	CppUnit_addTest(pSuite, PathTest, testParseGuess);
	CppUnit_addTest(pSuite, PathTest, testTryParse);
	CppUnit_addTest(pSuite, PathTest, testStatics);
	CppUnit_addTest(pSuite, PathTest, testBaseNameExt);
	CppUnit_addTest(pSuite, PathTest, testAbsolute);
	CppUnit_addTest(pSuite, PathTest, testRobustness);
	CppUnit_addTest(pSuite, PathTest, testParent);
	CppUnit_addTest(pSuite, PathTest, testForDirectory);
	CppUnit_addTest(pSuite, PathTest, testExpand);
	CppUnit_addTest(pSuite, PathTest, testListRoots);
	CppUnit_addTest(pSuite, PathTest, testFind);
	CppUnit_addTest(pSuite, PathTest, testSwap);
	CppUnit_addTest(pSuite, PathTest, testResolve);
	CppUnit_addTest(pSuite, PathTest, testPushPop);
	CppUnit_addTest(pSuite, PathTest, testWindowsSystem);

	return pSuite;
}
