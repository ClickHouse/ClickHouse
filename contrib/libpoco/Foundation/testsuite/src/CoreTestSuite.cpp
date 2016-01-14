//
// CoreTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/CoreTestSuite.cpp#2 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CoreTestSuite.h"
#include "CoreTest.h"
#include "ArrayTest.h"
#include "AutoPtrTest.h"
#include "SharedPtrTest.h"
#include "AutoReleasePoolTest.h"
#include "ByteOrderTest.h"
#include "StringTest.h"
#include "StringTokenizerTest.h"
#ifndef POCO_VXWORKS
#include "FPETest.h"
#endif
#include "RegularExpressionTest.h"
#include "NDCTest.h"
#include "NumberFormatterTest.h"
#include "NumberParserTest.h"
#include "DynamicFactoryTest.h"
#include "MemoryPoolTest.h"
#include "AnyTest.h"
#include "VarTest.h"
#include "FormatTest.h"
#include "TuplesTest.h"
#ifndef POCO_VXWORKS
#include "NamedTuplesTest.h"
#endif
#include "TypeListTest.h"
#include "ObjectPoolTest.h"
#include "ListMapTest.h"


CppUnit::Test* CoreTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("CoreTestSuite");

	pSuite->addTest(CoreTest::suite());
	pSuite->addTest(ArrayTest::suite());
	pSuite->addTest(AutoPtrTest::suite());
	pSuite->addTest(SharedPtrTest::suite());
	pSuite->addTest(AutoReleasePoolTest::suite());
	pSuite->addTest(ByteOrderTest::suite());
	pSuite->addTest(StringTest::suite());
	pSuite->addTest(StringTokenizerTest::suite());
#ifndef POCO_VXWORKS
	pSuite->addTest(FPETest::suite());
#endif
	pSuite->addTest(RegularExpressionTest::suite());
	pSuite->addTest(NDCTest::suite());
	pSuite->addTest(NumberFormatterTest::suite());
	pSuite->addTest(NumberParserTest::suite());
	pSuite->addTest(DynamicFactoryTest::suite());
	pSuite->addTest(MemoryPoolTest::suite());
	pSuite->addTest(AnyTest::suite());
	pSuite->addTest(VarTest::suite());
	pSuite->addTest(FormatTest::suite());
	pSuite->addTest(TuplesTest::suite());
#ifndef POCO_VXWORKS
	pSuite->addTest(NamedTuplesTest::suite());
#endif
	pSuite->addTest(TypeListTest::suite());
	pSuite->addTest(ObjectPoolTest::suite());
	pSuite->addTest(ListMapTest::suite());

	return pSuite;
}
