//
// NetCoreTestSuite.h
//
// $Id: //poco/1.4/Net/testsuite/src/NetCoreTestSuite.h#1 $
//
// Definition of the NetCoreTestSuite class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetCoreTestSuite_INCLUDED
#define NetCoreTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class NetCoreTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // NetCoreTestSuite_INCLUDED
