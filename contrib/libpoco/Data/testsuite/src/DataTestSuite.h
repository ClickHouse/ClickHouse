//
// DataTestSuite.h
//
// $Id: //poco/Main/Data/testsuite/src/DataTestSuite.h#1 $
//
// Definition of the DataTestSuite class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DataTestSuite_INCLUDED
#define DataTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class DataTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // DataTestSuite_INCLUDED
