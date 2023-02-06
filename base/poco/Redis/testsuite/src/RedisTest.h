//
// RedisTest.h
//
// Definition of the RedisTest class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef RedisTest_INCLUDED
#define RedisTest_INCLUDED


#include "Poco/Redis/Redis.h"
#include "Poco/Redis/Client.h"
#include "CppUnit/TestCase.h"


class RedisTest: public CppUnit::TestCase
{
public:
	RedisTest(const std::string& name);


	virtual ~RedisTest();

	void testAPPEND();
	void testBLPOP();
	void testBRPOP();
	void testDECR();
	void testECHO();
	void testError();
	void testEVAL();
	void testHDEL();
	void testHEXISTS();
	void testHGETALL();
	void testHINCRBY();
	void testHKEYS();
	void testHMGET();
	void testHMSET();
	void testHSET();
	void testHSTRLEN();
	void testHVALS();
	void testINCR();
	void testINCRBY();
	void testLINDEX();
	void testLINSERT();
	void testLPOP();
	void testLREM();
	void testLSET();
	void testLTRIM();
	void testMULTI();
	void testMSET();
	void testMSETWithMap();
	void testPING();
	void testPipeliningWithSendCommands();
	void testPipeliningWithWriteCommand();
	void testPubSub();
	void testSADD();
	void testSCARD();
	void testSDIFF();
	void testSDIFFSTORE();
	void testSET();
	void testSINTER();
	void testSINTERSTORE();
	void testSISMEMBER();
	void testSMEMBERS();
	void testSMOVE();
	void testSPOP();
	void testSRANDMEMBER();
	void testSREM();
	void testSUNION();
	void testSUNIONSTORE();
	void testSTRLEN();
	void testRENAME();
	void testRENAMENX();
	void testRPOP();
	void testRPOPLPUSH();
	void testRPUSH();

	void testPool();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:

	void delKey(const std::string& key);

	std::string _host;
	unsigned    _port;
	static bool _connected;
	static Poco::Redis::Client _redis;

};


#endif // RedisTest_INCLUDED
