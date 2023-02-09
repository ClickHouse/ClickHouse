//
// CryptoException.h
//
//
// Library: Crypto
// Package: Crypto
// Module:  CryptoException
//
// Definition of the CryptoException class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_CryptoException_INCLUDED
#define Crypto_CryptoException_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Crypto {


POCO_DECLARE_EXCEPTION(Crypto_API, CryptoException, Poco::Exception)


class Crypto_API OpenSSLException : public CryptoException
{
public:
	OpenSSLException(int code = 0);
	OpenSSLException(const std::string& msg, int code = 0);
	OpenSSLException(const std::string& msg, const std::string& arg, int code = 0);
	OpenSSLException(const std::string& msg, const Poco::Exception& exc, int code = 0);
	OpenSSLException(const OpenSSLException& exc);
	~OpenSSLException() throw();
	OpenSSLException& operator = (const OpenSSLException& exc);
	const char* name() const throw();
	const char* className() const throw();
	Poco::Exception* clone() const;
	void rethrow() const;

private:
	void setExtMessage();
};


} } // namespace Poco::Crypto


#endif // Crypto_CryptoException_INCLUDED
