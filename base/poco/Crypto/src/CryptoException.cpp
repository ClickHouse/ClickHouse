//
// CryptoException.cpp
//
//
// Library: Crypto
// Package: Crypto
// Module:  CryptoException
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/CryptoException.h"
#include "Poco/NumberFormatter.h"
#include <typeinfo>
#include <openssl/err.h>


namespace Poco {
namespace Crypto {


POCO_IMPLEMENT_EXCEPTION(CryptoException, Exception, "Crypto Exception")


OpenSSLException::OpenSSLException(int otherCode): CryptoException(otherCode)
{
	setExtMessage();
}


OpenSSLException::OpenSSLException(const std::string& msg, int otherCode): CryptoException(msg, otherCode)
{
	setExtMessage();
}


OpenSSLException::OpenSSLException(const std::string& msg, const std::string& arg, int otherCode): CryptoException(msg, arg, otherCode)
{
	setExtMessage();
}


OpenSSLException::OpenSSLException(const std::string& msg, const Poco::Exception& exc, int otherCode): CryptoException(msg, exc, otherCode)
{
	setExtMessage();
}


OpenSSLException::OpenSSLException(const OpenSSLException& exc): CryptoException(exc)
{
	setExtMessage();
}


OpenSSLException::~OpenSSLException() throw()
{
}


OpenSSLException& OpenSSLException::operator = (const OpenSSLException& exc)
{
	CryptoException::operator = (exc);
	return *this;
}


const char* OpenSSLException::name() const throw()
{
	return "OpenSSLException";
}


const char* OpenSSLException::className() const throw()
{
	return typeid(*this).name();
}


Poco::Exception* OpenSSLException::clone() const
{
	return new OpenSSLException(*this);
}


void OpenSSLException::setExtMessage()
{
	Poco::UInt64 e = static_cast<Poco::UInt64>(ERR_get_error());
	char buf[128] = { 0 };
	char* pErr = ERR_error_string(static_cast<unsigned long>(e), buf);
	std::string err;
	if (pErr) err = pErr;
	else err = NumberFormatter::format(e);

	extendedMessage(err);
}


void OpenSSLException::rethrow() const
{
	throw *this;
}


} } // namespace Poco::Crypto
