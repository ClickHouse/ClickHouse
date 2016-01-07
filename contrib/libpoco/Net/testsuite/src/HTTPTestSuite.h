//
// HTTPTestSuite.h
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPTestSuite.h#1 $
//
// Definition of the HTTPTestSuite class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPTestSuite_INCLUDED
#define HTTPTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class HTTPTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // HTTPTestSuite_INCLUDED
