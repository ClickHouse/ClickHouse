//
// NetTestSuite.h
//
// $Id: //poco/1.4/Net/testsuite/src/NetTestSuite.h#1 $
//
// Definition of the NetTestSuite class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetTestSuite_INCLUDED
#define NetTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class NetTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // NetTestSuite_INCLUDED
