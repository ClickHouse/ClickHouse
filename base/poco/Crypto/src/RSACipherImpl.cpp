//
// RSACipherImpl.cpp
//
// Library: Crypto
// Package: RSA
// Module:  RSACipherImpl
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/RSACipherImpl.h"
#include "Poco/Crypto/CryptoTransform.h"
#include "Poco/Exception.h"
#include <openssl/err.h>
#include <openssl/rsa.h>
#include <cstring>


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


	int mapPaddingMode(RSAPaddingMode paddingMode)
	{
		switch (paddingMode)
		{
		case RSA_PADDING_PKCS1:
			return RSA_PKCS1_PADDING;
		case RSA_PADDING_PKCS1_OAEP:
			return RSA_PKCS1_OAEP_PADDING;
		case RSA_PADDING_NONE:
			return RSA_NO_PADDING;
		default:
			poco_bugcheck();
			return RSA_NO_PADDING;
		}
	}


	class RSAEncryptImpl: public CryptoTransform
	{
	public:
		RSAEncryptImpl(const RSA* pRSA, RSAPaddingMode paddingMode);
		~RSAEncryptImpl();

		std::size_t blockSize() const;
		std::size_t maxDataSize() const;
		std::string getTag(std::size_t);
		void setTag(const std::string&);

		std::streamsize transform(
			const unsigned char* input,
			std::streamsize		 inputLength,
			unsigned char*		 output,
			std::streamsize		 outputLength);

		std::streamsize finalize(unsigned char*	output, std::streamsize length);

	private:
		const RSA*      _pRSA;
		RSAPaddingMode  _paddingMode;
		std::streamsize _pos;
		unsigned char*  _pBuf;
	};


	RSAEncryptImpl::RSAEncryptImpl(const RSA* pRSA, RSAPaddingMode paddingMode):
			_pRSA(pRSA),
			_paddingMode(paddingMode),
			_pos(0),
			_pBuf(0)
	{
		_pBuf = new unsigned char[blockSize()];
	}


	RSAEncryptImpl::~RSAEncryptImpl()
	{
		delete [] _pBuf;
	}


	std::size_t RSAEncryptImpl::blockSize() const
	{
		return RSA_size(_pRSA);
	}


	std::size_t RSAEncryptImpl::maxDataSize() const
	{
		std::size_t size = blockSize();
		switch (_paddingMode)
		{
		case RSA_PADDING_PKCS1:
		case RSA_PADDING_SSLV23:
			size -= 11;
			break;
		case RSA_PADDING_PKCS1_OAEP:
			size -= 41;
			break;
		default:
			break;
		}
		return size;
	}


	std::string RSAEncryptImpl::getTag(std::size_t)
	{
		return std::string();
	}


	void RSAEncryptImpl::setTag(const std::string&)
	{
	}


	std::streamsize RSAEncryptImpl::transform(
		const unsigned char* input,
		std::streamsize		 inputLength,
		unsigned char*		 output,
		std::streamsize		 outputLength)
	{
		// always fill up the buffer before writing!
		std::streamsize maxSize = static_cast<std::streamsize>(maxDataSize());
		std::streamsize rsaSize = static_cast<std::streamsize>(blockSize());
		poco_assert_dbg(_pos <= maxSize);
		poco_assert (outputLength >= rsaSize);
		int rc = 0;
		while (inputLength > 0)
		{
			// check how many data bytes we are missing to get the buffer full
			poco_assert_dbg (maxSize >= _pos);
			std::streamsize missing = maxSize - _pos;
			if (missing == 0)
			{
				poco_assert (outputLength >= rsaSize);
				int n = RSA_public_encrypt(static_cast<int>(maxSize), _pBuf, output, const_cast<RSA*>(_pRSA), mapPaddingMode(_paddingMode));
				if (n == -1)
					throwError();
				rc += n;
				output += n;
				outputLength -= n;
				_pos = 0;

			}
			else
			{
				if (missing > inputLength)
					missing = inputLength;

				std::memcpy(_pBuf + _pos, input, static_cast<std::size_t>(missing));
				input += missing;
				_pos += missing;
				inputLength -= missing;
			}
		}
		return rc;
	}


	std::streamsize RSAEncryptImpl::finalize(unsigned char*	output, std::streamsize length)
	{
		poco_assert (length >= blockSize());
		poco_assert (_pos <= maxDataSize());
		int rc = 0;
		if (_pos > 0)
		{
			rc = RSA_public_encrypt(static_cast<int>(_pos), _pBuf, output, const_cast<RSA*>(_pRSA), mapPaddingMode(_paddingMode));
			if (rc == -1) throwError();
		}
		return rc;
	}


	class RSADecryptImpl: public CryptoTransform
	{
	public:
		RSADecryptImpl(const RSA* pRSA, RSAPaddingMode paddingMode);
		~RSADecryptImpl();

		std::size_t blockSize() const;
		std::string getTag(std::size_t);
		void setTag(const std::string&);

		std::streamsize transform(
			const unsigned char* input,
			std::streamsize		 inputLength,
			unsigned char*		 output,
			std::streamsize		 outputLength);

		std::streamsize finalize(
			unsigned char*	output,
			std::streamsize length);

	private:
		const RSA*      _pRSA;
		RSAPaddingMode  _paddingMode;
		std::streamsize _pos;
		unsigned char*  _pBuf;
	};


	RSADecryptImpl::RSADecryptImpl(const RSA* pRSA, RSAPaddingMode paddingMode):
			_pRSA(pRSA),
			_paddingMode(paddingMode),
			_pos(0),
			_pBuf(0)
	{
		_pBuf = new unsigned char[blockSize()];
	}


	RSADecryptImpl::~RSADecryptImpl()
	{
		delete [] _pBuf;
	}


	std::size_t RSADecryptImpl::blockSize() const
	{
		return RSA_size(_pRSA);
	}


	std::string RSADecryptImpl::getTag(std::size_t)
	{
		return std::string();
	}


	void RSADecryptImpl::setTag(const std::string&)
	{
	}


	std::streamsize RSADecryptImpl::transform(
		const unsigned char* input,
		std::streamsize		 inputLength,
		unsigned char*		 output,
		std::streamsize		 outputLength)
	{

		// always fill up the buffer before decrypting!
		std::streamsize rsaSize = static_cast<std::streamsize>(blockSize());
		poco_assert_dbg(_pos <= rsaSize);
		poco_assert (outputLength >= rsaSize);
		int rc = 0;
		while (inputLength > 0)
		{
			// check how many data bytes we are missing to get the buffer full
			poco_assert_dbg (rsaSize >= _pos);
			std::streamsize missing = rsaSize - _pos;
			if (missing == 0)
			{
				int tmp = RSA_private_decrypt(static_cast<int>(rsaSize), _pBuf, output, const_cast<RSA*>(_pRSA), mapPaddingMode(_paddingMode));
				if (tmp == -1)
					throwError();
				rc += tmp;
				output += tmp;
				outputLength -= tmp;
				_pos = 0;

			}
			else
			{
				if (missing > inputLength)
					missing = inputLength;

				std::memcpy(_pBuf + _pos, input, static_cast<std::size_t>(missing));
				input += missing;
				_pos += missing;
				inputLength -= missing;
			}
		}
		return rc;
	}


	std::streamsize RSADecryptImpl::finalize(unsigned char*	output, std::streamsize length)
	{
		poco_assert (length >= blockSize());
		int rc = 0;
		if (_pos > 0)
		{
			rc = RSA_private_decrypt(static_cast<int>(_pos), _pBuf, output, const_cast<RSA*>(_pRSA), mapPaddingMode(_paddingMode));
			if (rc == -1)
				throwError();
		}
		return rc;
	}
}


RSACipherImpl::RSACipherImpl(const RSAKey& key, RSAPaddingMode paddingMode):
	_key(key),
	_paddingMode(paddingMode)
{
}


RSACipherImpl::~RSACipherImpl()
{
}


CryptoTransform* RSACipherImpl::createEncryptor()
{
	return new RSAEncryptImpl(_key.impl()->getRSA(), _paddingMode);
}


CryptoTransform* RSACipherImpl::createDecryptor()
{
	return new RSADecryptImpl(_key.impl()->getRSA(), _paddingMode);
}


} } // namespace Poco::Crypto
