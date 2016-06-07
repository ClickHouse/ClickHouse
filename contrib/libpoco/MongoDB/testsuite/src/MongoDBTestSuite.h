//
// MongoDBTestSuite.h
//
// $Id$
//
// Definition of the MongoDBTestSuite class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDBTestSuite_INCLUDED
#define MongoDBTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class MongoDBTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // MongoDBTestSuite_INCLUDED
