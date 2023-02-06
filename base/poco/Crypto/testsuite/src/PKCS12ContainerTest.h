//
// PKCS12ContainerTest.h
//
// Definition of the PKCS12ContainerTest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PKCS12ContainerTest_INCLUDED
#define PKCS12ContainerTest_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "CppUnit/TestCase.h"
#include "Poco/Crypto/PKCS12Container.h"
#include "Poco/Crypto/X509Certificate.h"


class PKCS12ContainerTest: public CppUnit::TestCase
{
public:
	PKCS12ContainerTest(const std::string& name);
	~PKCS12ContainerTest();

	void testFullPKCS12();
	void testCertsOnlyPKCS12();
	void testPEMReadWrite();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	std::string getTestFilesPath(const std::string& name,
		const std::string& ext = "p12");
	void certsOnly(const Poco::Crypto::PKCS12Container& pkcs12);
	void certsOnlyList(const Poco::Crypto::PKCS12Container::CAList& caList,
		const Poco::Crypto::PKCS12Container::CANameList& caNamesList,
		const std::vector<int>& certOrder);
	void full(const Poco::Crypto::PKCS12Container& pkcs12);
	void fullCert(const Poco::Crypto::X509Certificate& x509);
	void fullList(Poco::Crypto::PKCS12Container::CAList caList,
		const Poco::Crypto::PKCS12Container::CANameList& caNamesList,
		const std::vector<int>& certOrder);
};


#endif // PKCS12ContainerTest_INCLUDED
