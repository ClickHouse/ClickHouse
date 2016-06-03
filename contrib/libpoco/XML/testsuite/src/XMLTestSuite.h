//
// XMLTestSuite.h
//
// $Id: //poco/1.4/XML/testsuite/src/XMLTestSuite.h#1 $
//
// Definition of the XMLTestSuite class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XMLTestSuite_INCLUDED
#define XMLTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class XMLTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // XMLTestSuite_INCLUDED
