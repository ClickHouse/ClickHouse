//
// MongoDBTestSuite.cpp
//
// $Id$
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MongoDBTestSuite.h"
#include "MongoDBTest.h"


CppUnit::Test* MongoDBTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("MongoDBTestSuite");

	pSuite->addTest(MongoDBTest::suite());

	return pSuite;
}
