//
// OpenSSLTestSuite.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/NetSSLTestSuite.cpp#1 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NetSSLTestSuite.h"

#include "HTTPSClientTestSuite.h"
#include "TCPServerTestSuite.h"
#include "HTTPSServerTestSuite.h"


CppUnit::Test* NetSSLTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("OpenSSLTestSuite");

	
	pSuite->addTest(HTTPSClientTestSuite::suite());
	pSuite->addTest(TCPServerTestSuite::suite());
	pSuite->addTest(HTTPSServerTestSuite::suite());

	return pSuite;
}
