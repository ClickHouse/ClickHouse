//
// ThreadingTestSuite.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ThreadingTestSuite.h#1 $
//
// Definition of the ThreadingTestSuite class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ThreadingTestSuite_INCLUDED
#define ThreadingTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class ThreadingTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // ThreadingTestSuite_INCLUDED
