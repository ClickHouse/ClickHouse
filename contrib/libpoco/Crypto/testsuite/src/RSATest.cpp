//
// RSATest.cpp
//
// $Id: //poco/1.4/Crypto/testsuite/src/RSATest.cpp#1 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "RSATest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Crypto/RSADigestEngine.h"
#include "Poco/Crypto/CipherFactory.h"
#include "Poco/Crypto/Cipher.h"
#include "Poco/Crypto/X509Certificate.h"
#include <sstream>


using namespace Poco::Crypto;


static const std::string anyPem(
	"-----BEGIN CERTIFICATE-----\r\n"
	"MIICaDCCAdECCQCzfxSsk7yaLjANBgkqhkiG9w0BAQUFADBzMQswCQYDVQQGEwJB\r\n"
	"VDESMBAGA1UECBMJQ2FyaW50aGlhMRIwEAYDVQQHEwlTdC4gSmFrb2IxDzANBgNV\r\n"
	"BAoTBkFwcEluZjEPMA0GA1UEAxMGQXBwSW5mMRowGAYJKoZIhvcNAQkBFgthcHBA\r\n"
	"aW5mLmNvbTAeFw0wNjAzMDExMzA3MzFaFw0wNjAzMzExMzA3MzFaMH4xCzAJBgNV\r\n"
	"BAYTAkFUMRIwEAYDVQQIEwlDYXJpbnRoaWExETAPBgNVBAcTCFN0IEpha29iMRww\r\n"
	"GgYDVQQKExNBcHBsaWVkIEluZm9ybWF0aWNzMQowCAYDVQQDFAEqMR4wHAYJKoZI\r\n"
	"hvcNAQkBFg9pbmZvQGFwcGluZi5jb20wgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJ\r\n"
	"AoGBAJHGyXDHyCYoWz+65ltNwwZbhwOGnxr9P1WMATuFJh0bPBZxKbZRdbTm9KhZ\r\n"
	"OlvsEIsfgiYdsxURYIqXfEgISYLZcZY0pQwGEOmB+0NeC/+ENSfOlNSthx6zSVlc\r\n"
	"zhJ7+dJOGwepHAiLr1fRuc5jogYLraE+lKTnqAAFfzwvti77AgMBAAEwDQYJKoZI\r\n"
	"hvcNAQEFBQADgYEAY/ZoeY1ukkEJX7259NeoVM0oahlulWV0rlCqyaeosOiDORPT\r\n"
	"m6X1w/5MTCf9VyaD1zukoSZ4QqNVjHFXcXidbB7Tgt3yRuZ5PC5LIFCDPv9mgPne\r\n"
	"mUA70yfctNfza2z3ZiQ6NDkW3mZX+1tmxYIrJQIrkVeYeqf1Gh2nyZrUMcE=\r\n"
	"-----END CERTIFICATE-----\r\n"
	"-----BEGIN RSA PRIVATE KEY-----\r\n"
	"Proc-Type: 4,ENCRYPTED\r\n"
	"DEK-Info: DES-EDE3-CBC,E7AE93C9E49184EA\r\n"
	"\r\n"
	"A2IqzNcWs+I5vzV+i+woDk56+yr58eU0Onw8eEvXkLjnSc58JU4327IF7yUbKWdW\r\n"
	"Q7BYGGOkVFiZ7ANOwviDg5SUhxRDWCcW8dS6/p1vfdQ1C3qj2OwJjkpg0aDBIzJn\r\n"
	"FzgguT3MF3ama77vxv0S3kOfmCj62MLqPGpj5pQ0/1hefRFbL8oAX8bXUN7/rmGM\r\n"
	"Zc0QyzFZv2iQ04dY/6TNclwKPB4H0On4K+8BMs3PRkWA0clCaQaFO2+iwnk3XZfe\r\n"
	"+MsKUEbLCpAQeYspYv1cw38dCdWq1KTP5aJk+oXgwjfX5cAaPTz74NTqTIsCcaTD\r\n"
	"3vy7ukJYFlDR9Kyo7z8rMazYrKJslhnuRH0BhK9st9McwL957j5tZmrKyraCcmCx\r\n"
	"dMAGcsis1va3ayYZpIpFqA4EhYrTM+6N8ZRfUap20+b5IQwHfTQDejUhL6rBwy7j\r\n"
	"Ti5yD83/itoOMyXq2sV/XWfVD5zk/P5iv22O1EAQMhhnPB9K/I/JhuSGQJfn3cNh\r\n"
	"ykOUYT0+vDeSeEVa+FVEP1W35G0alTbKbNs5Tb8KxJ3iDJUxokM//SvPXZy9hOVX\r\n"
	"Y05imB04J15DaGbAHlNzunhuJi7121WV/JRXZRW9diE6hwpD8rwqi3FMuRUmy7U9\r\n"
	"aFA5poKRAYlo9YtZ3YpFyjGKB6MfCQcB2opuSnQ/gbugV41m67uQ4CDwWLaNRkTb\r\n"
	"GlsMBNcHnidg15Bsat5HaB7l250ukrI13Uw1MYdDUzaS3gPfw9aC4F2w0p3U+DPH\r\n"
	"80/zePxtroR7T4/+rI136Rl+aMXDMOEGCX1TVP8rjuZzuRyUSUKC8Q==\r\n"
	"-----END RSA PRIVATE KEY-----\r\n"
	"-----BEGIN CERTIFICATE-----\r\n"
	"MIICXTCCAcYCCQC1Vk/N8qR4AjANBgkqhkiG9w0BAQUFADBzMQswCQYDVQQGEwJB\r\n"
	"VDESMBAGA1UECBMJQ2FyaW50aGlhMRIwEAYDVQQHEwlTdC4gSmFrb2IxDzANBgNV\r\n"
	"BAoTBkFwcEluZjEPMA0GA1UEAxMGQXBwSW5mMRowGAYJKoZIhvcNAQkBFgthcHBA\r\n"
	"aW5mLmNvbTAeFw0wNjAyMjcxMzI3MThaFw0wNjAzMjkxMzI3MThaMHMxCzAJBgNV\r\n"
	"BAYTAkFUMRIwEAYDVQQIEwlDYXJpbnRoaWExEjAQBgNVBAcTCVN0LiBKYWtvYjEP\r\n"
	"MA0GA1UEChMGQXBwSW5mMQ8wDQYDVQQDEwZBcHBJbmYxGjAYBgkqhkiG9w0BCQEW\r\n"
	"C2FwcEBpbmYuY29tMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCsFXiPuicN\r\n"
	"Im4oJwF8NuaFN+lgYwcZ6dAO3ILIR3kLA2PxF8HSQLfF8J8a4odZhLhctIMAKTxm\r\n"
	"k0w8TW5qhL8QLdGzY9vzvkgdKOkan2t3sMeXJAfrM1AphTsmgntAQazGZjOj5p4W\r\n"
	"jDnxQ+VXAylqwjHh49eSBxM3wgoscF4iLQIDAQABMA0GCSqGSIb3DQEBBQUAA4GB\r\n"
	"AIpfLdXiKchPvFMhQS8xTtXvrw5dVL3yImUMYs4GQi8RrjGmfGB3yMAR7B/b8v4a\r\n"
	"+ztfusgWAWiUKuSGTk4S8YB0fsFlmOv0WDr+PyZ4Lui/a8opbyzGE7rqpnF/s0GO\r\n"
	"M7uLCNNwIN7WhmxcWV0KZU1wTppoSWPJda1yTbBzF9XP\r\n"
	"-----END CERTIFICATE-----\r\n"
);


RSATest::RSATest(const std::string& name): CppUnit::TestCase(name)
{
}


RSATest::~RSATest()
{
}


void RSATest::testNewKeys()
{
	RSAKey key(RSAKey::KL_1024, RSAKey::EXP_SMALL);
	std::ostringstream strPub;
	std::ostringstream strPriv;
	key.save(&strPub, &strPriv, "testpwd");
	std::string pubKey = strPub.str();
	std::string privKey = strPriv.str();

	// now do the round trip
	std::istringstream iPub(pubKey);
	std::istringstream iPriv(privKey);
	RSAKey key2(&iPub, &iPriv, "testpwd");

	std::istringstream iPriv2(privKey);
	RSAKey key3(0, &iPriv2,  "testpwd");
	std::ostringstream strPub3;
	key3.save(&strPub3);
	std::string pubFromPrivate = strPub3.str();
	assert (pubFromPrivate == pubKey);
}


void RSATest::testNewKeysNoPassphrase()
{
	RSAKey key(RSAKey::KL_1024, RSAKey::EXP_SMALL);
	std::ostringstream strPub;
	std::ostringstream strPriv;
	key.save(&strPub, &strPriv);
	std::string pubKey = strPub.str();
	std::string privKey = strPriv.str();

	// now do the round trip
	std::istringstream iPub(pubKey);
	std::istringstream iPriv(privKey);
	RSAKey key2(&iPub, &iPriv);

	std::istringstream iPriv2(privKey);
	RSAKey key3(0, &iPriv2);
	std::ostringstream strPub3;
	key3.save(&strPub3);
	std::string pubFromPrivate = strPub3.str();
	assert (pubFromPrivate == pubKey);
}


void RSATest::testSign()
{
	std::string msg("Test this sign message");
	RSAKey key(RSAKey::KL_2048, RSAKey::EXP_LARGE);
	RSADigestEngine eng(key);
	eng.update(msg.c_str(), static_cast<unsigned>(msg.length()));
	const Poco::DigestEngine::Digest& sig = eng.signature();
	std::string hexDig = Poco::DigestEngine::digestToHex(sig);

	// verify
	std::ostringstream strPub;
	key.save(&strPub);
	std::string pubKey = strPub.str();
	std::istringstream iPub(pubKey);
	RSAKey keyPub(&iPub);
	RSADigestEngine eng2(keyPub);
	eng2.update(msg.c_str(), static_cast<unsigned>(msg.length()));
	assert (eng2.verify(sig));
}


void RSATest::testSignSha256()
{
	std::string msg("Test this sign message");
	RSAKey key(RSAKey::KL_2048, RSAKey::EXP_LARGE);
	RSADigestEngine eng(key, "SHA256");
	eng.update(msg.c_str(), static_cast<unsigned>(msg.length()));
	const Poco::DigestEngine::Digest& sig = eng.signature();
	std::string hexDig = Poco::DigestEngine::digestToHex(sig);

	// verify
	std::ostringstream strPub;
	key.save(&strPub);
	std::string pubKey = strPub.str();
	std::istringstream iPub(pubKey);
	RSAKey keyPub(&iPub);
	RSADigestEngine eng2(keyPub, "SHA256");
	eng2.update(msg.c_str(), static_cast<unsigned>(msg.length()));
	assert (eng2.verify(sig));
}


void RSATest::testSignManipulated()
{
	std::string msg("Test this sign message");
	std::string msgManip("Test that sign message");
	RSAKey key(RSAKey::KL_2048, RSAKey::EXP_LARGE);
	RSADigestEngine eng(key);
	eng.update(msg.c_str(), static_cast<unsigned>(msg.length()));
	const Poco::DigestEngine::Digest& sig = eng.signature();
	std::string hexDig = Poco::DigestEngine::digestToHex(sig);

	// verify
	std::ostringstream strPub;
	key.save(&strPub);
	std::string pubKey = strPub.str();
	std::istringstream iPub(pubKey);
	RSAKey keyPub(&iPub);
	RSADigestEngine eng2(keyPub);
	eng2.update(msgManip.c_str(), static_cast<unsigned>(msgManip.length()));
	assert (!eng2.verify(sig));
}


void RSATest::testRSACipher()
{
	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(RSAKey(RSAKey::KL_1024, RSAKey::EXP_SMALL));
	for (std::size_t n = 1; n <= 1200; n++)
	{
		std::string val(n, 'x');
		std::string enc = pCipher->encryptString(val);
		std::string dec = pCipher->decryptString(enc);
		assert (dec == val);
	}
}


void RSATest::testRSACipherLarge()
{
	std::vector<std::size_t> sizes;
	sizes.push_back (2047);
	sizes.push_back (2048);
	sizes.push_back (2049);
	sizes.push_back (4095);
	sizes.push_back (4096);
	sizes.push_back (4097);
	sizes.push_back (8191);
	sizes.push_back (8192);
	sizes.push_back (8193);
	sizes.push_back (16383);
	sizes.push_back (16384);
	sizes.push_back (16385);
	
	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(RSAKey(RSAKey::KL_1024, RSAKey::EXP_SMALL));
	for (std::vector<std::size_t>::const_iterator it = sizes.begin(); it != sizes.end(); ++it)
	{
		std::string val(*it, 'x');
		std::string enc = pCipher->encryptString(val);
		std::string dec = pCipher->decryptString(enc);
		assert (dec == val);
	}
}


void RSATest::testCertificate()
{
	std::istringstream str(anyPem);
	X509Certificate cert(str);
	RSAKey publicKey(cert);
	std::istringstream str2(anyPem);
	RSAKey privateKey(0, &str2, "test");
	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(publicKey);
	Cipher::Ptr pCipher2 = CipherFactory::defaultFactory().createCipher(privateKey);
	std::string val("lets do some encryption");
	
	std::string enc = pCipher->encryptString(val);
	std::string dec = pCipher2->decryptString(enc);
	assert (dec == val);
}


void RSATest::setUp()
{
}


void RSATest::tearDown()
{
}


CppUnit::Test* RSATest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("RSATest");

	CppUnit_addTest(pSuite, RSATest, testNewKeys);
	CppUnit_addTest(pSuite, RSATest, testNewKeysNoPassphrase);
	CppUnit_addTest(pSuite, RSATest, testSign);
	CppUnit_addTest(pSuite, RSATest, testSignSha256);
	CppUnit_addTest(pSuite, RSATest, testSignManipulated);
	CppUnit_addTest(pSuite, RSATest, testRSACipher);
	CppUnit_addTest(pSuite, RSATest, testRSACipherLarge);
	CppUnit_addTest(pSuite, RSATest, testCertificate);

	return pSuite;
}
