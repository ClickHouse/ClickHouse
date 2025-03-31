//
// CipherImpl.cpp
//
// Library: Crypto
// Package: Cipher
// Module:  CipherImpl
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/CipherImpl.h"
#include "Poco/Crypto/CryptoTransform.h"
#include "Poco/Exception.h"
#include "Poco/Buffer.h"
#include <openssl/err.h>


namespace Poco {
namespace Crypto {


namespace
{
	void throwError()
	{
		unsigned long err;
		std::string msg;

		while ((err = ERR_get_error()))
		{
			if (!msg.empty())
				msg.append("; ");
			msg.append(ERR_error_string(err, 0));
		}

		throw Poco::IOException(msg);
	}


	class CryptoTransformImpl: public CryptoTransform
	{
	public:
		typedef Cipher::ByteVec ByteVec;

		enum Direction
		{
			DIR_ENCRYPT,
			DIR_DECRYPT
		};

		CryptoTransformImpl(
			const EVP_CIPHER* pCipher,
			const ByteVec&    key,
			const ByteVec&    iv,
			Direction         dir);

		~CryptoTransformImpl();

		std::size_t blockSize() const;
		int setPadding(int padding);
		std::string getTag(std::size_t tagSize);
		void setTag(const std::string& tag);

		std::streamsize transform(
			const unsigned char* input,
			std::streamsize      inputLength,
			unsigned char*       output,
			std::streamsize      outputLength);

		std::streamsize finalize(
			unsigned char*  output,
			std::streamsize length);

	private:
		const EVP_CIPHER* _pCipher;
		EVP_CIPHER_CTX*   _pContext;
		ByteVec           _key;
		ByteVec           _iv;
	};


	CryptoTransformImpl::CryptoTransformImpl(
		const EVP_CIPHER* pCipher,
		const ByteVec&    key,
		const ByteVec&    iv,
		Direction         dir):
		_pCipher(pCipher),
		_key(key),
		_iv(iv)
	{
		_pContext = EVP_CIPHER_CTX_new();
		EVP_CipherInit(
			_pContext,
			_pCipher,
			&_key[0],
			_iv.empty() ? 0 : &_iv[0],
			(dir == DIR_ENCRYPT) ? 1 : 0);

		if (_iv.size() != EVP_CIPHER_iv_length(_pCipher) && EVP_CIPHER_mode(_pCipher) == EVP_CIPH_GCM_MODE)
		{
			int rc = EVP_CIPHER_CTX_ctrl(_pContext, EVP_CTRL_GCM_SET_IVLEN, _iv.size(), NULL);
			if (rc == 0) throwError();
		}
	}


	CryptoTransformImpl::~CryptoTransformImpl()
	{
		EVP_CIPHER_CTX_cleanup(_pContext);
		EVP_CIPHER_CTX_free(_pContext);
	}


	std::size_t CryptoTransformImpl::blockSize() const
	{
		return EVP_CIPHER_CTX_block_size(_pContext);
	}


	int CryptoTransformImpl::setPadding(int padding)
	{
		return EVP_CIPHER_CTX_block_size(_pContext);
	}


	std::string CryptoTransformImpl::getTag(std::size_t tagSize)
	{
		std::string tag;

		Poco::Buffer<char> buffer(tagSize);
		int rc = EVP_CIPHER_CTX_ctrl(_pContext, EVP_CTRL_GCM_GET_TAG, tagSize, buffer.begin());
		if (rc == 0) throwError();
		tag.assign(buffer.begin(), tagSize);

		return tag;
	}


	void CryptoTransformImpl::setTag(const std::string& tag)
	{
		int rc = EVP_CIPHER_CTX_ctrl(_pContext, EVP_CTRL_GCM_SET_TAG, tag.size(), const_cast<char*>(tag.data()));
		if (rc == 0) throwError();
	}


	std::streamsize CryptoTransformImpl::transform(
		const unsigned char* input,
		std::streamsize      inputLength,
		unsigned char*       output,
		std::streamsize      outputLength)
	{
		poco_assert (outputLength >= (inputLength + blockSize() - 1));

		int outLen = static_cast<int>(outputLength);
		int rc = EVP_CipherUpdate(
			_pContext,
			output,
			&outLen,
			input,
			static_cast<int>(inputLength));
		if (rc == 0)
			throwError();

		return static_cast<std::streamsize>(outLen);
	}


	std::streamsize CryptoTransformImpl::finalize(
		unsigned char*	output,
		std::streamsize length)
	{
		poco_assert (length >= blockSize());

		int len = static_cast<int>(length);

		// Use the '_ex' version that does not perform implicit cleanup since we
		// will call EVP_CIPHER_CTX_cleanup() from the dtor as there is no
		// guarantee that finalize() will be called if an error occurred.
		int rc = EVP_CipherFinal_ex(_pContext, output, &len);

		if (rc == 0)
			throwError();

		return static_cast<std::streamsize>(len);
	}
}


CipherImpl::CipherImpl(const CipherKey& key):
	_key(key)
{
}


CipherImpl::~CipherImpl()
{
}


CryptoTransform* CipherImpl::createEncryptor()
{
	CipherKeyImpl::Ptr p = _key.impl();
	return new CryptoTransformImpl(p->cipher(), p->getKey(), p->getIV(), CryptoTransformImpl::DIR_ENCRYPT);
}


CryptoTransform* CipherImpl::createDecryptor()
{
	CipherKeyImpl::Ptr p = _key.impl();
	return new CryptoTransformImpl(p->cipher(), p->getKey(), p->getIV(), CryptoTransformImpl::DIR_DECRYPT);
}


} } // namespace Poco::Crypto
