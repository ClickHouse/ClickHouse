//
// ODBCTestSuite.cpp
//
// $Id: //poco/Main/Data/ODBC/testsuite/src/ODBCTestSuite.cpp#4 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ODBCTestSuite.h"
#include "ODBCDB2Test.h"
#include "ODBCMySQLTest.h"
#include "ODBCOracleTest.h"
#include "ODBCPostgreSQLTest.h"
#include "ODBCSQLiteTest.h"
#if defined(POCO_OS_FAMILY_WINDOWS)
#include "ODBCAccessTest.h"
#endif
#include "ODBCSQLServerTest.h"


CppUnit::Test* ODBCTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ODBCTestSuite");

	// WARNING!
	// On Win XP Pro, the PostgreSQL connection fails if attempted after DB2 w/ following error:
	// 
	// sqlState="IM003" 
	// message="Specified driver could not be loaded due to system error  127 (PostgreSQL ANSI)." 
	// nativeError=160 
	// System error 127 is "The specified procedure could not be found."
	// This problem does not manifest with Mammoth ODBCng PostgreSQL driver.
	//
	// Oracle tests do not exit cleanly if Oracle driver is loaded after DB2.
	// 
	// For the time being, the workaround is to connect to DB2 after connecting to PostgreSQL and Oracle.

	addTest(pSuite, ODBCMySQLTest::suite());
	addTest(pSuite, ODBCOracleTest::suite());
	addTest(pSuite, ODBCPostgreSQLTest::suite());
	addTest(pSuite, ODBCSQLiteTest::suite());
	addTest(pSuite, ODBCSQLServerTest::suite());
	addTest(pSuite, ODBCDB2Test::suite());
// MS Access driver does not support connection status detection
// disabled for the time being
#if 0 //defined(POCO_OS_FAMILY_WINDOWS)
	addTest(pSuite, ODBCAccessTest::suite());
#endif

	return pSuite;
}


void ODBCTestSuite::addTest(CppUnit::TestSuite* pSuite, CppUnit::Test* pT)
{
	if (pSuite && pT) pSuite->addTest(pT);
}
