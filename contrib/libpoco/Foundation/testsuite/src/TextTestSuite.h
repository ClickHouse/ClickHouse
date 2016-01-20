//
// TextTestSuite.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TextTestSuite.h#1 $
//
// Definition of the TextTestSuite class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TextTestSuite_INCLUDED
#define TextTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class TextTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // TextTestSuite_INCLUDED
