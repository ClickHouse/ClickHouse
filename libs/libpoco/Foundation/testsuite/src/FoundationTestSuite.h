//
// FoundationTestSuite.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/FoundationTestSuite.h#1 $
//
// Definition of the FoundationTestSuite class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef FoundationTestSuite_INCLUDED
#define FoundationTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class FoundationTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // FoundationTestSuite_INCLUDED
