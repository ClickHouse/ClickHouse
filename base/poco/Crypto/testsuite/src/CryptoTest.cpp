//
// CryptoTest.cpp
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CryptoTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Crypto/CipherFactory.h"
#include "Poco/Crypto/Cipher.h"
#include "Poco/Crypto/CipherKey.h"
#include "Poco/Crypto/X509Certificate.h"
#include "Poco/Crypto/CryptoStream.h"
#include "Poco/Crypto/CryptoTransform.h"
#include "Poco/StreamCopier.h"
#include "Poco/Base64Encoder.h"
#include "Poco/HexBinaryEncoder.h"
#include <sstream>


using namespace Poco::Crypto;


static const std::string APPINF_PEM(
	"-----BEGIN CERTIFICATE-----\n"
	"MIIESzCCAzOgAwIBAgIBATALBgkqhkiG9w0BAQUwgdMxEzARBgNVBAMMCmFwcGlu\n"
	"Zi5jb20xNjA0BgNVBAoMLUFwcGxpZWQgSW5mb3JtYXRpY3MgU29mdHdhcmUgRW5n\n"
	"aW5lZXJpbmcgR21iSDEUMBIGA1UECwwLRGV2ZWxvcG1lbnQxEjAQBgNVBAgMCUNh\n"
	"cmludGhpYTELMAkGA1UEBhMCQVQxHjAcBgNVBAcMFVN0LiBKYWtvYiBpbSBSb3Nl\n"
	"bnRhbDEtMCsGCSqGSIb3DQEJARYeZ3VlbnRlci5vYmlsdHNjaG5pZ0BhcHBpbmYu\n"
	"Y29tMB4XDTA5MDUwNzE0NTY1NloXDTI5MDUwMjE0NTY1NlowgdMxEzARBgNVBAMM\n"
	"CmFwcGluZi5jb20xNjA0BgNVBAoMLUFwcGxpZWQgSW5mb3JtYXRpY3MgU29mdHdh\n"
	"cmUgRW5naW5lZXJpbmcgR21iSDEUMBIGA1UECwwLRGV2ZWxvcG1lbnQxEjAQBgNV\n"
	"BAgMCUNhcmludGhpYTELMAkGA1UEBhMCQVQxHjAcBgNVBAcMFVN0LiBKYWtvYiBp\n"
	"bSBSb3NlbnRhbDEtMCsGCSqGSIb3DQEJARYeZ3VlbnRlci5vYmlsdHNjaG5pZ0Bh\n"
	"cHBpbmYuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA89GolWCR\n"
	"KtLQclJ2M2QtpFqzNC54hUQdR6n8+DAeruH9WFwLSdWW2fEi+jrtd/WEWCdt4PxX\n"
	"F2/eBYeURus7Hg2ZtJGDd3je0+Ygsv7+we4cMN/knaBY7rATqhmnZWk+yBpkf5F2\n"
	"IHp9gBxUaJWmt/bq3XrvTtzrDXpCd4zg4zPXZ8IC8ket5o3K2vnkAOsIsgN+Ffqd\n"
	"4GjF4dsblG6u6E3VarGRLwGtgB8BAZOA/33mV4FHSMkc4OXpAChaK3tM8YhrLw+m\n"
	"XtsfqDiv1825S6OWFCKGj/iX8X2QAkrdB63vXCSpb3de/ByIUfp31PpMlMh6dKo1\n"
	"vf7yj0nb2w0utQIDAQABoyowKDAOBgNVHQ8BAf8EBAMCB4AwFgYDVR0lAQH/BAww\n"
	"CgYIKwYBBQUHAwMwDQYJKoZIhvcNAQEFBQADggEBAM0cpfb4BgiU/rkYe121P581\n"
	"ftg5Ck1PYYda1Fy/FgzbgJh2AwVo/6sn6GF79/QkEcWEgtCMNNO3LMTTddUUApuP\n"
	"jnEimyfmUhIThyud/vryzTMNa/eZMwaAqUQWqLf+AwgqjUsBSMenbSHavzJOpsvR\n"
	"LI0PQ1VvqB+3UGz0JUnBJiKvHs83Fdm4ewPAf3M5fGcIa+Fl2nU5Plzwzskj84f6\n"
	"73ZlEEi3aW9JieNy7RWsMM+1E8Sj2CGRZC4BM9V1Fgnsh4+VHX8Eu7eHucvfeIYx\n"
	"3mmLMoK4sCayL/FGhrUDw5AkWb8tKNpRXY+W60Et281yxQSeWLPIbatVzIWI0/M=\n"
	"-----END CERTIFICATE-----\n"
);


CryptoTest::CryptoTest(const std::string& name): CppUnit::TestCase(name)
{
}


CryptoTest::~CryptoTest()
{
}


void CryptoTest::testEncryptDecrypt()
{
	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(CipherKey("aes256"));

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_NONE);
		std::string result = pCipher->decryptString(out, Cipher::ENC_NONE);
		assert (in == result);
	}

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_BASE64);
		std::string result = pCipher->decryptString(out, Cipher::ENC_BASE64);
		assert (in == result);
	}

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_BINHEX);
		std::string result = pCipher->decryptString(out, Cipher::ENC_BINHEX);
		assert (in == result);
	}
}


void CryptoTest::testEncryptDecryptWithSalt()
{
	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(CipherKey("aes256", "simplepwd", "Too much salt"));
	Cipher::Ptr pCipher2 = CipherFactory::defaultFactory().createCipher(CipherKey("aes256", "simplepwd", "Too much salt"));
	
	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_NONE);
		std::string result = pCipher2->decryptString(out, Cipher::ENC_NONE);
		assert (in == result);
	}

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_BASE64);
		std::string result = pCipher2->decryptString(out, Cipher::ENC_BASE64);
		assert (in == result);
	}

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_BINHEX);
		std::string result = pCipher2->decryptString(out, Cipher::ENC_BINHEX);
		assert (in == result);
	}
}


void CryptoTest::testEncryptDecryptWithSaltSha1()
{
	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(
		CipherKey("aes256", "simplepwd", "Too much salt", 2000, "sha1"));
	Cipher::Ptr pCipher2 = CipherFactory::defaultFactory().createCipher(
		CipherKey("aes256", "simplepwd", "Too much salt", 2000, "sha1"));

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_NONE);
		std::string result = pCipher2->decryptString(out, Cipher::ENC_NONE);
		assert (in == result);
	}

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_BASE64);
		std::string result = pCipher2->decryptString(out, Cipher::ENC_BASE64);
		assert (in == result);
	}

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_BINHEX);
		std::string result = pCipher2->decryptString(out, Cipher::ENC_BINHEX);
		assert (in == result);
	}
}


void CryptoTest::testEncryptDecryptDESECB()
{
	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(CipherKey("des-ecb", "password"));

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_NONE);
		std::string result = pCipher->decryptString(out, Cipher::ENC_NONE);
		assert (in == result);
	}

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_BASE64);
		std::string result = pCipher->decryptString(out, Cipher::ENC_BASE64);
		assert (in == result);
	}

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::string in(n, 'x');
		std::string out = pCipher->encryptString(in, Cipher::ENC_BINHEX);
		std::string result = pCipher->decryptString(out, Cipher::ENC_BINHEX);
		assert (in == result);
	}
}


void CryptoTest::testEncryptDecryptGCM()
{
	CipherKey key("aes-256-gcm");
	
	CipherKey::ByteVec iv(20, 213);
	key.setIV(iv);

	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(key);

	for (std::size_t n = 1; n < MAX_DATA_SIZE; n++)
	{
		std::stringstream str;
		CryptoTransform* pEncryptor = pCipher->createEncryptor();
		CryptoOutputStream encryptorStream(str, pEncryptor);
		std::string in(n, 'x');
		encryptorStream << in;
		encryptorStream.close();
		assert (encryptorStream.good());

		std::string tag = pEncryptor->getTag();

		CryptoTransform* pDecryptor = pCipher->createDecryptor();
		pDecryptor->setTag(tag);
		CryptoInputStream decryptorStream(str, pDecryptor);
		std::string out;
		decryptorStream >> out;

		assert (in == out);
	}
}


void CryptoTest::testPassword()
{
	CipherKey key("aes256", "password", "salt");
	
	std::ostringstream keyStream;
	Poco::Base64Encoder base64KeyEnc(keyStream);
	base64KeyEnc.write(reinterpret_cast<const char*>(&key.getKey()[0]), key.keySize());
	base64KeyEnc.close();
	std::string base64Key = keyStream.str();
	assert (base64Key == "hIzxBt58GDd7/6mRp88bewKk42lM4QwaF78ek0FkVoA=");
}


void CryptoTest::testPasswordSha1()
{
	// the test uses 1 iteration, as the openssl executable does not allow to set a custom number
	// of iterations
	CipherKey key("aes256", "password", "saltsalt", 1, "sha1");

	std::ostringstream keyStream;
	Poco::HexBinaryEncoder hexKeyEnc(keyStream);
	hexKeyEnc.write(reinterpret_cast<const char*>(&key.getKey()[0]), key.keySize());
	hexKeyEnc.close();
	std::string hexKey = keyStream.str();

	std::ostringstream ivStream;
	Poco::HexBinaryEncoder hexIvEnc(ivStream);
	hexIvEnc.write(reinterpret_cast<const char*>(&key.getIV()[0]), key.ivSize());
	hexIvEnc.close();
	std::string hexIv = ivStream.str();

	// got Hex value for key and iv using:
	// openssl enc -e -a -md sha1 -aes256 -k password -S 73616c7473616c74 -P
	// (where "salt" == 73616c74 in Hex, doubled for an 8 bytes salt, openssl padds the salt with 0
	// whereas Poco's implementation padds with the existing bytes using a modulo operation)
	assert (hexIv == "c96049b0edc0b67af61ecc43d3de8898");
	assert (hexKey == "cab86dd6261710891e8cb56ee3625691a75df344f0bff4c12cf3596fc00b39c7");
}


void CryptoTest::testEncryptInterop()
{
	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(CipherKey("aes256", "password", "salt"));

	const std::string plainText  = "This is a secret message.";
	const std::string expectedCipherText = "9HITTPaU3A/LaZzldbdnRZ109DKlshouKren/n8BsHc=";
	std::string cipherText = pCipher->encryptString(plainText, Cipher::ENC_BASE64);
	assert (cipherText == expectedCipherText);
}


void CryptoTest::testDecryptInterop()
{
	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(CipherKey("aes256", "password", "salt"));

	const std::string expectedPlainText  = "This is a secret message.";
	const std::string cipherText = "9HITTPaU3A/LaZzldbdnRZ109DKlshouKren/n8BsHc=";
	std::string plainText = pCipher->decryptString(cipherText, Cipher::ENC_BASE64);
	assert (plainText == expectedPlainText);
}


void CryptoTest::testStreams()
{
	Cipher::Ptr pCipher = CipherFactory::defaultFactory().createCipher(CipherKey("aes256"));

	static const std::string SECRET_MESSAGE = "This is a secret message. Don't tell anyone.";

	std::stringstream sstr;
	EncryptingOutputStream encryptor(sstr, *pCipher);
	encryptor << SECRET_MESSAGE;
	encryptor.close();
	
	DecryptingInputStream decryptor(sstr, *pCipher);
	std::string result;
	Poco::StreamCopier::copyToString(decryptor, result);
	
	assert (result == SECRET_MESSAGE);
	assert (decryptor.eof());
	assert (!decryptor.bad());


	std::istringstream emptyStream;
	DecryptingInputStream badDecryptor(emptyStream, *pCipher);
	Poco::StreamCopier::copyToString(badDecryptor, result);

	assert (badDecryptor.fail());
	assert (badDecryptor.bad());
	assert (!badDecryptor.eof());
}


void CryptoTest::testCertificate()
{
	std::istringstream certStream(APPINF_PEM);
	X509Certificate cert(certStream);
	
	std::string subjectName(cert.subjectName());
	std::string issuerName(cert.issuerName());
	std::string commonName(cert.commonName());
	std::string country(cert.subjectName(X509Certificate::NID_COUNTRY));
	std::string localityName(cert.subjectName(X509Certificate::NID_LOCALITY_NAME));
	std::string stateOrProvince(cert.subjectName(X509Certificate::NID_STATE_OR_PROVINCE));
	std::string organizationName(cert.subjectName(X509Certificate::NID_ORGANIZATION_NAME));
	std::string organizationUnitName(cert.subjectName(X509Certificate::NID_ORGANIZATION_UNIT_NAME));
	
	assert (subjectName == "/CN=appinf.com/O=Applied Informatics Software Engineering GmbH/OU=Development/ST=Carinthia/C=AT/L=St. Jakob im Rosental/emailAddress=guenter.obiltschnig@appinf.com");
	assert (issuerName == subjectName);
	assert (commonName == "appinf.com");
	assert (country == "AT");
	assert (localityName == "St. Jakob im Rosental");
	assert (stateOrProvince == "Carinthia");
	assert (organizationName == "Applied Informatics Software Engineering GmbH");
	assert (organizationUnitName == "Development");
	
	// fails with recent OpenSSL versions:
	// assert (cert.issuedBy(cert));
	
	std::istringstream otherCertStream(APPINF_PEM);
	X509Certificate otherCert(otherCertStream);
	
	assert (cert.equals(otherCert));
}


void CryptoTest::setUp()
{
}


void CryptoTest::tearDown()
{
}


CppUnit::Test* CryptoTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("CryptoTest");

	CppUnit_addTest(pSuite, CryptoTest, testEncryptDecrypt);
	CppUnit_addTest(pSuite, CryptoTest, testEncryptDecryptWithSalt);
	CppUnit_addTest(pSuite, CryptoTest, testEncryptDecryptWithSaltSha1);
	CppUnit_addTest(pSuite, CryptoTest, testEncryptDecryptDESECB);
	CppUnit_addTest(pSuite, CryptoTest, testEncryptDecryptGCM);
	CppUnit_addTest(pSuite, CryptoTest, testPassword);
	CppUnit_addTest(pSuite, CryptoTest, testPasswordSha1);
	CppUnit_addTest(pSuite, CryptoTest, testEncryptInterop);
	CppUnit_addTest(pSuite, CryptoTest, testDecryptInterop);
	CppUnit_addTest(pSuite, CryptoTest, testStreams);
	CppUnit_addTest(pSuite, CryptoTest, testCertificate);

	return pSuite;
}
