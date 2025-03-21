//
// RSAKeyImpl.cpp
//
// Library: Crypto
// Package: RSA
// Module:  RSAKeyImpl
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/RSAKeyImpl.h"
#include "Poco/Crypto/X509Certificate.h"
#include "Poco/Crypto/PKCS12Container.h"
#include "Poco/FileStream.h"
#include "Poco/StreamCopier.h"
#include <sstream>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/evp.h>
#if OPENSSL_VERSION_NUMBER >= 0x00908000L
#include <openssl/bn.h>
#endif


namespace Poco {
namespace Crypto {


RSAKeyImpl::RSAKeyImpl(const EVPPKey& key):
	KeyPairImpl("rsa", KT_RSA_IMPL),
	_pRSA(EVP_PKEY_get1_RSA(const_cast<EVP_PKEY*>((const EVP_PKEY*)key)))
{
	if (!_pRSA) throw OpenSSLException();
}


RSAKeyImpl::RSAKeyImpl(const X509Certificate& cert):
	KeyPairImpl("rsa", KT_RSA_IMPL),
	_pRSA(0)
{
	const X509* pCert = cert.certificate();
	EVP_PKEY* pKey = X509_get_pubkey(const_cast<X509*>(pCert));
	if (pKey)
	{
		_pRSA = EVP_PKEY_get1_RSA(pKey);
		EVP_PKEY_free(pKey);
	}
	else
		throw OpenSSLException("RSAKeyImpl(const X509Certificate&)");
}


RSAKeyImpl::RSAKeyImpl(const PKCS12Container& cont):
	KeyPairImpl("rsa", KT_RSA_IMPL),
	_pRSA(0)
{
	EVPPKey key = cont.getKey();
	_pRSA = EVP_PKEY_get1_RSA(key);
}


RSAKeyImpl::RSAKeyImpl(int keyLength, unsigned long exponent): KeyPairImpl("rsa", KT_RSA_IMPL),
	_pRSA(0)
{
#if OPENSSL_VERSION_NUMBER >= 0x00908000L
	_pRSA = RSA_new();
	int ret = 0;
	BIGNUM* bn = 0;
	try
	{
		bn = BN_new();
		BN_set_word(bn, exponent);
		ret = RSA_generate_key_ex(_pRSA, keyLength, bn, 0);
		BN_free(bn);
	}
	catch (...)
	{
		BN_free(bn);
		throw;
	}
	if (!ret) throw Poco::InvalidArgumentException("Failed to create RSA context");
#else
	_pRSA = RSA_generate_key(keyLength, exponent, 0, 0);
	if (!_pRSA) throw Poco::InvalidArgumentException("Failed to create RSA context");
#endif
}


RSAKeyImpl::RSAKeyImpl(const std::string& publicKeyFile,
	const std::string& privateKeyFile,
	const std::string& privateKeyPassphrase): KeyPairImpl("rsa", KT_RSA_IMPL),
		_pRSA(0)
{
	poco_assert_dbg(_pRSA == 0);

	_pRSA = RSA_new();
	if (!publicKeyFile.empty())
	{
		BIO* bio = BIO_new(BIO_s_file());
		if (!bio) throw Poco::IOException("Cannot create BIO for reading public key", publicKeyFile);
		int rc = BIO_read_filename(bio, const_cast<char *>(publicKeyFile.c_str()));
		if (rc)
		{
			RSA* pubKey = PEM_read_bio_RSAPublicKey(bio, &_pRSA, 0, 0);
			if (!pubKey)
			{
				int rc = BIO_reset(bio);
				// BIO_reset() normally returns 1 for success and 0 or -1 for failure.
				// File BIOs are an exception, they return 0 for success and -1 for failure.
				if (rc != 0) throw Poco::FileException("Failed to load public key", publicKeyFile);
				pubKey = PEM_read_bio_RSA_PUBKEY(bio, &_pRSA, 0, 0);
			}
			BIO_free(bio);
			if (!pubKey)
			{
				freeRSA();
				throw Poco::FileException("Failed to load public key", publicKeyFile);
			}
		}
		else
		{
			freeRSA();
			throw Poco::FileNotFoundException("Public key file", publicKeyFile);
		}
	}

	if (!privateKeyFile.empty())
	{
		BIO* bio = BIO_new(BIO_s_file());
		if (!bio) throw Poco::IOException("Cannot create BIO for reading private key", privateKeyFile);
		int rc = BIO_read_filename(bio, const_cast<char *>(privateKeyFile.c_str()));
		if (rc)
		{
			RSA* privKey = 0;
			if (privateKeyPassphrase.empty())
				privKey = PEM_read_bio_RSAPrivateKey(bio, &_pRSA, 0, 0);
			else
				privKey = PEM_read_bio_RSAPrivateKey(bio, &_pRSA, 0, const_cast<char*>(privateKeyPassphrase.c_str()));
			BIO_free(bio);
			if (!privKey)
			{
				freeRSA();
				throw Poco::FileException("Failed to load private key", privateKeyFile);
			}
		}
		else
		{
			freeRSA();
			throw Poco::FileNotFoundException("Private key file", privateKeyFile);
		}
	}
}


RSAKeyImpl::RSAKeyImpl(std::istream* pPublicKeyStream,
	std::istream* pPrivateKeyStream,
	const std::string& privateKeyPassphrase): KeyPairImpl("rsa", KT_RSA_IMPL),
		_pRSA(0)
{
	poco_assert_dbg(_pRSA == 0);

	_pRSA = RSA_new();
	if (pPublicKeyStream)
	{
		std::string publicKeyData;
		Poco::StreamCopier::copyToString(*pPublicKeyStream, publicKeyData);
		BIO* bio = BIO_new_mem_buf(const_cast<char*>(publicKeyData.data()), static_cast<int>(publicKeyData.size()));
		if (!bio) throw Poco::IOException("Cannot create BIO for reading public key");
		RSA* publicKey = PEM_read_bio_RSAPublicKey(bio, &_pRSA, 0, 0);
		if (!publicKey)
		{
			int rc = BIO_reset(bio);
			// BIO_reset() normally returns 1 for success and 0 or -1 for failure.
			// File BIOs are an exception, they return 0 for success and -1 for failure.
			if (rc != 1) throw Poco::FileException("Failed to load public key");
			publicKey = PEM_read_bio_RSA_PUBKEY(bio, &_pRSA, 0, 0);
		}
		BIO_free(bio);
		if (!publicKey)
		{
			freeRSA();
			throw Poco::FileException("Failed to load public key");
		}
	}

	if (pPrivateKeyStream)
	{
		std::string privateKeyData;
		Poco::StreamCopier::copyToString(*pPrivateKeyStream, privateKeyData);
		BIO* bio = BIO_new_mem_buf(const_cast<char*>(privateKeyData.data()), static_cast<int>(privateKeyData.size()));
		if (!bio) throw Poco::IOException("Cannot create BIO for reading private key");
		RSA* privateKey = 0;
		if (privateKeyPassphrase.empty())
			privateKey = PEM_read_bio_RSAPrivateKey(bio, &_pRSA, 0, 0);
		else
			privateKey = PEM_read_bio_RSAPrivateKey(bio, &_pRSA, 0, const_cast<char*>(privateKeyPassphrase.c_str()));
		BIO_free(bio);
		if (!privateKey)
		{
			freeRSA();
			throw Poco::FileException("Failed to load private key");
		}
	}
}

std::string RSAKeyImpl::getPrivateInPEM() const
{
    EVP_PKEY *evp_key = EVP_PKEY_new();
    if (EVP_PKEY_assign_RSA(evp_key, _pRSA) != 1)
    {
        EVP_PKEY_free(evp_key);
        throw OpenSSLException("Error converting RSA key to an EVP_PKEY structure");
    }

    BIO * key_bio(BIO_new(BIO_s_mem()));
    if (PEM_write_bio_PrivateKey(key_bio, evp_key, nullptr, nullptr, 0, nullptr, nullptr) != 1)
    {
        BIO_free(key_bio);
        EVP_PKEY_free(evp_key);
        throw OpenSSLException("Error writing private key to BIO");
    }

    char * data;
    size_t data_len = BIO_get_mem_data(key_bio, &data);
    std::string private_key(data, data_len);

    BIO_free(key_bio);

    return private_key;
}


RSAKeyImpl::~RSAKeyImpl()
{
	freeRSA();
}


void RSAKeyImpl::freeRSA()
{
	if (_pRSA) RSA_free(_pRSA);
	_pRSA = 0;
}


int RSAKeyImpl::size() const
{
	return RSA_size(_pRSA);
}


RSAKeyImpl::ByteVec RSAKeyImpl::modulus() const
{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L && !defined(LIBRESSL_VERSION_NUMBER)
	const BIGNUM* n = 0;
	const BIGNUM* e = 0;
	const BIGNUM* d = 0;
	RSA_get0_key(_pRSA, &n, &e, &d);
	return convertToByteVec(n);
#else
	return convertToByteVec(_pRSA->n);
#endif
}


RSAKeyImpl::ByteVec RSAKeyImpl::encryptionExponent() const
{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L && !defined(LIBRESSL_VERSION_NUMBER)
	const BIGNUM* n = 0;
	const BIGNUM* e = 0;
	const BIGNUM* d = 0;
	RSA_get0_key(_pRSA, &n, &e, &d);
	return convertToByteVec(e);
#else
	return convertToByteVec(_pRSA->e);
#endif
}


RSAKeyImpl::ByteVec RSAKeyImpl::decryptionExponent() const
{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L && !defined(LIBRESSL_VERSION_NUMBER)
	const BIGNUM* n = 0;
	const BIGNUM* e = 0;
	const BIGNUM* d = 0;
	RSA_get0_key(_pRSA, &n, &e, &d);
	return convertToByteVec(d);
#else
	return convertToByteVec(_pRSA->d);
#endif
}


RSAKeyImpl::ByteVec RSAKeyImpl::convertToByteVec(const BIGNUM* bn)
{
	int numBytes = BN_num_bytes(bn);
	ByteVec byteVector(numBytes);

	ByteVec::value_type* buffer = new ByteVec::value_type[numBytes];
	BN_bn2bin(bn, buffer);

	for (int i = 0; i < numBytes; ++i)
		byteVector[i] = buffer[i];

	delete [] buffer;

	return byteVector;
}


} } // namespace Poco::Crypto
