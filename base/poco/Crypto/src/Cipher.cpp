//
// Cipher.cpp
//
// Library: Crypto
// Package: Cipher
// Module:  Cipher
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/Cipher.h"
#include "Poco/Crypto/CryptoStream.h"
#include "Poco/Crypto/CryptoTransform.h"
#include "Poco/Base64Encoder.h"
#include "Poco/Base64Decoder.h"
#include "Poco/HexBinaryEncoder.h"
#include "Poco/HexBinaryDecoder.h"
#include "Poco/StreamCopier.h"
#include "Poco/Exception.h"
#include <sstream>
#include <memory>


namespace Poco {
namespace Crypto {


Cipher::Cipher()
{
}


Cipher::~Cipher()
{
}


std::string Cipher::encryptString(const std::string& str, Encoding encoding)
{
	std::istringstream source(str);
	std::ostringstream sink;

	encrypt(source, sink, encoding);

	return sink.str();
}


std::string Cipher::decryptString(const std::string& str, Encoding encoding)
{
	std::istringstream source(str);
	std::ostringstream sink;

	decrypt(source, sink, encoding);
	return sink.str();
}


void Cipher::encrypt(std::istream& source, std::ostream& sink, Encoding encoding)
{
	CryptoInputStream encryptor(source, createEncryptor());

	switch (encoding)
	{
	case ENC_NONE:
		StreamCopier::copyStream(encryptor, sink);
		break;

	case ENC_BASE64:
	case ENC_BASE64_NO_LF:
		{
			Poco::Base64Encoder encoder(sink);
			if (encoding == ENC_BASE64_NO_LF)
			{
				encoder.rdbuf()->setLineLength(0);
			}
			StreamCopier::copyStream(encryptor, encoder);
			encoder.close();
		}
		break;

	case ENC_BINHEX:
	case ENC_BINHEX_NO_LF:
		{
			Poco::HexBinaryEncoder encoder(sink);
			if (encoding == ENC_BINHEX_NO_LF)
			{
				encoder.rdbuf()->setLineLength(0);
			}
			StreamCopier::copyStream(encryptor, encoder);
			encoder.close();
		}
		break;

	default:
		throw Poco::InvalidArgumentException("Invalid argument", "encoding");
	}
}


void Cipher::decrypt(std::istream& source, std::ostream& sink, Encoding encoding)
{
	CryptoOutputStream decryptor(sink, createDecryptor());

	switch (encoding)
	{
	case ENC_NONE:
		StreamCopier::copyStream(source, decryptor);
		decryptor.close();
		break;

	case ENC_BASE64:
	case ENC_BASE64_NO_LF:
		{
			Poco::Base64Decoder decoder(source);
			StreamCopier::copyStream(decoder, decryptor);
			decryptor.close();
		}
		break;

	case ENC_BINHEX:
	case ENC_BINHEX_NO_LF:
		{
			Poco::HexBinaryDecoder decoder(source);
			StreamCopier::copyStream(decoder, decryptor);
			decryptor.close();
		}
		break;

	default:
		throw Poco::InvalidArgumentException("Invalid argument", "encoding");
	}
}


} } // namespace Poco::Crypto
