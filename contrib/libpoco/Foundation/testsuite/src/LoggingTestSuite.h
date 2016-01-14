//
// LoggingTestSuite.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/LoggingTestSuite.h#1 $
//
// Definition of the LoggingTestSuite class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef LoggingTestSuite_INCLUDED
#define LoggingTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class LoggingTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // LoggingTestSuite_INCLUDED
