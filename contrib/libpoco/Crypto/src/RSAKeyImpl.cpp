//
// RSAKeyImpl.cpp
//
// $Id: //poco/1.4/Crypto/src/RSAKeyImpl.cpp#3 $
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


RSAKeyImpl::RSAKeyImpl(const X509Certificate& cert):
	_pRSA(0)
{
	const X509* pCert = cert.certificate();
	EVP_PKEY* pKey = X509_get_pubkey(const_cast<X509*>(pCert));
	_pRSA = EVP_PKEY_get1_RSA(pKey);
	EVP_PKEY_free(pKey);
}


RSAKeyImpl::RSAKeyImpl(int keyLength, unsigned long exponent):
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


RSAKeyImpl::RSAKeyImpl(
		const std::string& publicKeyFile, 
		const std::string& privateKeyFile, 
		const std::string& privateKeyPassphrase):
	_pRSA(0)
{
	poco_assert_dbg(_pRSA == 0);
	
	_pRSA = RSA_new();
	if (!publicKeyFile.empty())
	{
		BIO* bio = BIO_new(BIO_s_file());
		if (!bio) throw Poco::IOException("Cannot create BIO for reading public key", publicKeyFile);
		int rc = BIO_read_filename(bio, publicKeyFile.c_str());
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
		int rc = BIO_read_filename(bio, privateKeyFile.c_str());
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


RSAKeyImpl::RSAKeyImpl(std::istream* pPublicKeyStream, std::istream* pPrivateKeyStream, const std::string& privateKeyPassphrase):
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


RSAKeyImpl::~RSAKeyImpl()
{
	freeRSA();
}


void RSAKeyImpl::freeRSA()
{
	if (_pRSA)
		RSA_free(_pRSA);
	_pRSA = 0;
}


int RSAKeyImpl::size() const
{
	return RSA_size(_pRSA);
}


RSAKeyImpl::ByteVec RSAKeyImpl::modulus() const
{
	return convertToByteVec(_pRSA->n);
}


RSAKeyImpl::ByteVec RSAKeyImpl::encryptionExponent() const
{
	return convertToByteVec(_pRSA->e);
}


RSAKeyImpl::ByteVec RSAKeyImpl::decryptionExponent() const
{
	return convertToByteVec(_pRSA->d);
}


void RSAKeyImpl::save(const std::string& publicKeyFile, const std::string& privateKeyFile, const std::string& privateKeyPassphrase)
{
	if (!publicKeyFile.empty())
	{
		BIO* bio = BIO_new(BIO_s_file());
		if (!bio) throw Poco::IOException("Cannot create BIO for writing public key file", publicKeyFile);
		try
		{
			if (BIO_write_filename(bio, const_cast<char*>(publicKeyFile.c_str())))
			{
				if (!PEM_write_bio_RSAPublicKey(bio, _pRSA))
					throw Poco::WriteFileException("Failed to write public key to file", publicKeyFile);
			}
			else throw Poco::CreateFileException("Cannot create public key file");
		}
		catch (...)
		{
			BIO_free(bio);
			throw;
		}
		BIO_free(bio);
	}
	
	if (!privateKeyFile.empty())
	{
		BIO* bio = BIO_new(BIO_s_file());
		if (!bio) throw Poco::IOException("Cannot create BIO for writing private key file", privateKeyFile);
		try
		{
			if (BIO_write_filename(bio, const_cast<char*>(privateKeyFile.c_str())))
			{
				int rc = 0;
				if (privateKeyPassphrase.empty())
					rc = PEM_write_bio_RSAPrivateKey(bio, _pRSA, 0, 0, 0, 0, 0);
				else
					rc = PEM_write_bio_RSAPrivateKey(bio, _pRSA, EVP_des_ede3_cbc(), 
						reinterpret_cast<unsigned char*>(const_cast<char*>(privateKeyPassphrase.c_str())), 
						static_cast<int>(privateKeyPassphrase.length()), 0, 0);
				if (!rc) throw Poco::FileException("Failed to write private key to file", privateKeyFile);
			}
			else throw Poco::CreateFileException("Cannot create private key file", privateKeyFile);
		}
		catch (...)
		{
			BIO_free(bio);
			throw;
		}
		BIO_free(bio);
	}
}


void RSAKeyImpl::save(std::ostream* pPublicKeyStream, std::ostream* pPrivateKeyStream, const std::string& privateKeyPassphrase)
{
	if (pPublicKeyStream)
	{
		BIO* bio = BIO_new(BIO_s_mem());
		if (!bio) throw Poco::IOException("Cannot create BIO for writing public key");
		if (!PEM_write_bio_RSAPublicKey(bio, _pRSA))
		{
			BIO_free(bio);
			throw Poco::WriteFileException("Failed to write public key to stream");
		}
		char* pData;
		long size = BIO_get_mem_data(bio, &pData);
		pPublicKeyStream->write(pData, static_cast<std::streamsize>(size));
		BIO_free(bio);
	}

	if (pPrivateKeyStream)
	{
		BIO* bio = BIO_new(BIO_s_mem());
		if (!bio) throw Poco::IOException("Cannot create BIO for writing public key");
		int rc = 0;
		if (privateKeyPassphrase.empty())
			rc = PEM_write_bio_RSAPrivateKey(bio, _pRSA, 0, 0, 0, 0, 0);
		else
			rc = PEM_write_bio_RSAPrivateKey(bio, _pRSA, EVP_des_ede3_cbc(), 
				reinterpret_cast<unsigned char*>(const_cast<char*>(privateKeyPassphrase.c_str())), 
				static_cast<int>(privateKeyPassphrase.length()), 0, 0);
		if (!rc) 
		{
			BIO_free(bio);
			throw Poco::FileException("Failed to write private key to stream");
		}
		char* pData;
		long size = BIO_get_mem_data(bio, &pData);
		pPrivateKeyStream->write(pData, static_cast<std::streamsize>(size));
		BIO_free(bio);
	}
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
