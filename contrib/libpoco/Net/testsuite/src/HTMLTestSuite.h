//
// HTMLTestSuite.h
//
// $Id: //poco/1.4/Net/testsuite/src/HTMLTestSuite.h#1 $
//
// Definition of the HTMLTestSuite class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTMLTestSuite_INCLUDED
#define HTMLTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class HTMLTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // HTMLTestSuite_INCLUDED
