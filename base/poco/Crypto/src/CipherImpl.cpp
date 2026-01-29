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
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		EVP_CIPHER_CTX*   _pContext;
#else
		EVP_CIPHER_CTX    _context;
#endif
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
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		_pContext = EVP_CIPHER_CTX_new();
		EVP_CipherInit(
			_pContext,
			_pCipher,
			&_key[0],
			_iv.empty() ? 0 : &_iv[0],
			(dir == DIR_ENCRYPT) ? 1 : 0);
#else
		EVP_CipherInit(
			&_context,
			_pCipher,
			&_key[0],
			_iv.empty() ? 0 : &_iv[0],
			(dir == DIR_ENCRYPT) ? 1 : 0);
#endif

#if OPENSSL_VERSION_NUMBER >= 0x10001000L
		if (_iv.size() != EVP_CIPHER_iv_length(_pCipher) && EVP_CIPHER_mode(_pCipher) == EVP_CIPH_GCM_MODE)
		{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
			int rc = EVP_CIPHER_CTX_ctrl(_pContext, EVP_CTRL_GCM_SET_IVLEN, _iv.size(), NULL);
#else
			int rc = EVP_CIPHER_CTX_ctrl(&_context, EVP_CTRL_GCM_SET_IVLEN, _iv.size(), NULL);
#endif
			if (rc == 0) throwError();
		}
#endif
	}


	CryptoTransformImpl::~CryptoTransformImpl()
	{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		EVP_CIPHER_CTX_cleanup(_pContext);
		EVP_CIPHER_CTX_free(_pContext);
#else
		EVP_CIPHER_CTX_cleanup(&_context);
#endif
	}


	std::size_t CryptoTransformImpl::blockSize() const
	{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		return EVP_CIPHER_CTX_block_size(_pContext);
#else
		return EVP_CIPHER_CTX_block_size(&_context);
#endif
	}


	int CryptoTransformImpl::setPadding(int padding)
	{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		return EVP_CIPHER_CTX_block_size(_pContext);
#else
		return EVP_CIPHER_CTX_set_padding(&_context, padding);
#endif
	}


	std::string CryptoTransformImpl::getTag(std::size_t tagSize)
	{
		std::string tag;
#if OPENSSL_VERSION_NUMBER >= 0x10001000L
		Poco::Buffer<char> buffer(tagSize);
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		int rc = EVP_CIPHER_CTX_ctrl(_pContext, EVP_CTRL_GCM_GET_TAG, tagSize, buffer.begin());
#else
		int rc = EVP_CIPHER_CTX_ctrl(&_context, EVP_CTRL_GCM_GET_TAG, tagSize, buffer.begin());
#endif
		if (rc == 0) throwError();
		tag.assign(buffer.begin(), tagSize);
#endif
		return tag;
	}


	void CryptoTransformImpl::setTag(const std::string& tag)
	{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		int rc = EVP_CIPHER_CTX_ctrl(_pContext, EVP_CTRL_GCM_SET_TAG, tag.size(), const_cast<char*>(tag.data()));
#elif OPENSSL_VERSION_NUMBER >= 0x10001000L
		int rc = EVP_CIPHER_CTX_ctrl(&_context, EVP_CTRL_GCM_SET_TAG, tag.size(), const_cast<char*>(tag.data()));
#else
		int rc = 0;
#endif
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
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		int rc = EVP_CipherUpdate(
			_pContext,
			output,
			&outLen,
			input,
			static_cast<int>(inputLength));
#else
		int rc = EVP_CipherUpdate(
			&_context,
			output,
			&outLen,
			input,
			static_cast<int>(inputLength));
#endif
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
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		int rc = EVP_CipherFinal_ex(_pContext, output, &len);
#else
		int rc = EVP_CipherFinal_ex(&_context, output, &len);
#endif

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
