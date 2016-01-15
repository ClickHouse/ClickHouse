//
// Context.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/src/Context.cpp#2 $
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  Context
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/Context.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Net/SSLException.h"
#include "Poco/Net/Utility.h"
#include "Poco/Crypto/OpenSSLInitializer.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/Timestamp.h"
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>


namespace Poco {
namespace Net {


Context::Context(
	Usage usage,
	const std::string& privateKeyFile, 
	const std::string& certificateFile,
	const std::string& caLocation, 
	VerificationMode verificationMode,
	int verificationDepth,
	bool loadDefaultCAs,
	const std::string& cipherList):
	_usage(usage),
	_mode(verificationMode),
	_pSSLContext(0),
	_extendedCertificateVerification(true)
{
	Poco::Crypto::OpenSSLInitializer::initialize();
	
	createSSLContext();

	try
	{
		int errCode = 0;
		if (!caLocation.empty())
		{
			Poco::File aFile(caLocation);
			if (aFile.isDirectory())
				errCode = SSL_CTX_load_verify_locations(_pSSLContext, 0, Poco::Path::transcode(caLocation).c_str());
			else
				errCode = SSL_CTX_load_verify_locations(_pSSLContext, Poco::Path::transcode(caLocation).c_str(), 0);
			if (errCode != 1)
			{
				std::string msg = Utility::getLastError();
				throw SSLContextException(std::string("Cannot load CA file/directory at ") + caLocation, msg);
			}
		}

		if (loadDefaultCAs)
		{
			errCode = SSL_CTX_set_default_verify_paths(_pSSLContext);
			if (errCode != 1)
			{
				std::string msg = Utility::getLastError();
				throw SSLContextException("Cannot load default CA certificates", msg);
			}
		}

		if (!privateKeyFile.empty())
		{
			errCode = SSL_CTX_use_PrivateKey_file(_pSSLContext, Poco::Path::transcode(privateKeyFile).c_str(), SSL_FILETYPE_PEM);
			if (errCode != 1)
			{
				std::string msg = Utility::getLastError();
				throw SSLContextException(std::string("Error loading private key from file ") + privateKeyFile, msg);
			}
		}

		if (!certificateFile.empty())
		{
			errCode = SSL_CTX_use_certificate_chain_file(_pSSLContext, Poco::Path::transcode(certificateFile).c_str());
			if (errCode != 1)
			{
				std::string errMsg = Utility::getLastError();
				throw SSLContextException(std::string("Error loading certificate from file ") + certificateFile, errMsg);
			}
		}

		if (isForServerUse())
			SSL_CTX_set_verify(_pSSLContext, verificationMode, &SSLManager::verifyServerCallback);
		else
			SSL_CTX_set_verify(_pSSLContext, verificationMode, &SSLManager::verifyClientCallback);

		SSL_CTX_set_cipher_list(_pSSLContext, cipherList.c_str());
		SSL_CTX_set_verify_depth(_pSSLContext, verificationDepth);
		SSL_CTX_set_mode(_pSSLContext, SSL_MODE_AUTO_RETRY);
		SSL_CTX_set_session_cache_mode(_pSSLContext, SSL_SESS_CACHE_OFF);
	}
	catch (...)
	{
		SSL_CTX_free(_pSSLContext);
		throw;
	}
}


Context::Context(
	Usage usage,
	const std::string& caLocation, 
	VerificationMode verificationMode,
	int verificationDepth,
	bool loadDefaultCAs,
	const std::string& cipherList):
	_usage(usage),
	_mode(verificationMode),
	_pSSLContext(0),
	_extendedCertificateVerification(true)
{
	Poco::Crypto::OpenSSLInitializer::initialize();
	
	createSSLContext();

	try
	{
		int errCode = 0;
		if (!caLocation.empty())
		{
			Poco::File aFile(caLocation);
			if (aFile.isDirectory())
				errCode = SSL_CTX_load_verify_locations(_pSSLContext, 0, Poco::Path::transcode(caLocation).c_str());
			else
				errCode = SSL_CTX_load_verify_locations(_pSSLContext, Poco::Path::transcode(caLocation).c_str(), 0);
			if (errCode != 1)
			{
				std::string msg = Utility::getLastError();
				throw SSLContextException(std::string("Cannot load CA file/directory at ") + caLocation, msg);
			}
		}

		if (loadDefaultCAs)
		{
			errCode = SSL_CTX_set_default_verify_paths(_pSSLContext);
			if (errCode != 1)
			{
				std::string msg = Utility::getLastError();
				throw SSLContextException("Cannot load default CA certificates", msg);
			}
		}

		if (isForServerUse())
			SSL_CTX_set_verify(_pSSLContext, verificationMode, &SSLManager::verifyServerCallback);
		else
			SSL_CTX_set_verify(_pSSLContext, verificationMode, &SSLManager::verifyClientCallback);

		SSL_CTX_set_cipher_list(_pSSLContext, cipherList.c_str());
		SSL_CTX_set_verify_depth(_pSSLContext, verificationDepth);
		SSL_CTX_set_mode(_pSSLContext, SSL_MODE_AUTO_RETRY);
		SSL_CTX_set_session_cache_mode(_pSSLContext, SSL_SESS_CACHE_OFF);
	}
	catch (...)
	{
		SSL_CTX_free(_pSSLContext);
		throw;
	}
}


Context::~Context()
{
	try
	{
		SSL_CTX_free(_pSSLContext);
		Poco::Crypto::OpenSSLInitializer::uninitialize();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void Context::useCertificate(const Poco::Crypto::X509Certificate& certificate)
{
	int errCode = SSL_CTX_use_certificate(_pSSLContext, const_cast<X509*>(certificate.certificate()));
	if (errCode != 1)
	{
		std::string msg = Utility::getLastError();
		throw SSLContextException("Cannot set certificate for Context", msg);
	}
}

	
void Context::addChainCertificate(const Poco::Crypto::X509Certificate& certificate)
{
	int errCode = SSL_CTX_add_extra_chain_cert(_pSSLContext, certificate.certificate());
	if (errCode != 1)
	{
		std::string msg = Utility::getLastError();
		throw SSLContextException("Cannot add chain certificate to Context", msg);
	}
}

	
void Context::usePrivateKey(const Poco::Crypto::RSAKey& key)
{
	int errCode = SSL_CTX_use_RSAPrivateKey(_pSSLContext, key.impl()->getRSA());
	if (errCode != 1)
	{
		std::string msg = Utility::getLastError();
		throw SSLContextException("Cannot set private key for Context", msg);
	}
}


void Context::enableSessionCache(bool flag)
{
	if (flag)
	{
		SSL_CTX_set_session_cache_mode(_pSSLContext, isForServerUse() ? SSL_SESS_CACHE_SERVER : SSL_SESS_CACHE_CLIENT);
	}
	else
	{
		SSL_CTX_set_session_cache_mode(_pSSLContext, SSL_SESS_CACHE_OFF);
	}
}


void Context::enableSessionCache(bool flag, const std::string& sessionIdContext)
{
	poco_assert (isForServerUse());

	if (flag)
	{
		SSL_CTX_set_session_cache_mode(_pSSLContext, SSL_SESS_CACHE_SERVER);
	}
	else
	{
		SSL_CTX_set_session_cache_mode(_pSSLContext, SSL_SESS_CACHE_OFF);
	}
	
	unsigned length = static_cast<unsigned>(sessionIdContext.length());
	if (length > SSL_MAX_SSL_SESSION_ID_LENGTH) length = SSL_MAX_SSL_SESSION_ID_LENGTH;
	int rc = SSL_CTX_set_session_id_context(_pSSLContext, reinterpret_cast<const unsigned char*>(sessionIdContext.data()), length);
	if (rc != 1) throw SSLContextException("cannot set session ID context");
}


bool Context::sessionCacheEnabled() const
{
	return SSL_CTX_get_session_cache_mode(_pSSLContext) != SSL_SESS_CACHE_OFF;
}


void Context::setSessionCacheSize(std::size_t size)
{
	poco_assert (isForServerUse());
	
	SSL_CTX_sess_set_cache_size(_pSSLContext, static_cast<long>(size));
}

	
std::size_t Context::getSessionCacheSize() const
{
	poco_assert (isForServerUse());
	
	return static_cast<std::size_t>(SSL_CTX_sess_get_cache_size(_pSSLContext));
}


void Context::setSessionTimeout(long seconds)
{
	poco_assert (isForServerUse());

	SSL_CTX_set_timeout(_pSSLContext, seconds);
}


long Context::getSessionTimeout() const
{
	poco_assert (isForServerUse());

	return SSL_CTX_get_timeout(_pSSLContext);
}


void Context::flushSessionCache() 
{
	poco_assert (isForServerUse());

	Poco::Timestamp now;
	SSL_CTX_flush_sessions(_pSSLContext, static_cast<long>(now.epochTime()));
}


void Context::enableExtendedCertificateVerification(bool flag)
{
	_extendedCertificateVerification = flag;
}


void Context::disableStatelessSessionResumption()
{
#if defined(SSL_OP_NO_TICKET)
	SSL_CTX_set_options(_pSSLContext, SSL_OP_NO_TICKET);
#endif
}


void Context::createSSLContext()
{
	if (SSLManager::isFIPSEnabled())
	{
		_pSSLContext = SSL_CTX_new(TLSv1_method());
	}
	else
	{
		switch (_usage)
		{
		case CLIENT_USE:
			_pSSLContext = SSL_CTX_new(SSLv23_client_method());
			break;
		case SERVER_USE:
			_pSSLContext = SSL_CTX_new(SSLv23_server_method());
			break;
		case TLSV1_CLIENT_USE:
			_pSSLContext = SSL_CTX_new(TLSv1_client_method());
			break;
		case TLSV1_SERVER_USE:
			_pSSLContext = SSL_CTX_new(TLSv1_server_method());
			break;
#if OPENSSL_VERSION_NUMBER >= 0x10000000L
		case TLSV1_1_CLIENT_USE:
			_pSSLContext = SSL_CTX_new(TLSv1_1_client_method());
			break;
		case TLSV1_1_SERVER_USE:
			_pSSLContext = SSL_CTX_new(TLSv1_1_server_method());
			break;
#endif
#if OPENSSL_VERSION_NUMBER >= 0x10001000L
		case TLSV1_2_CLIENT_USE:
			_pSSLContext = SSL_CTX_new(TLSv1_2_client_method());
			break;
		case TLSV1_2_SERVER_USE:
			_pSSLContext = SSL_CTX_new(TLSv1_2_server_method());
			break;
#endif
		default:
			throw Poco::InvalidArgumentException("Invalid or unsupported usage");
		}
	}
	if (!_pSSLContext) 
	{
		unsigned long err = ERR_get_error();
		throw SSLException("Cannot create SSL_CTX object", ERR_error_string(err, 0));
	}

	SSL_CTX_set_default_passwd_cb(_pSSLContext, &SSLManager::privateKeyPassphraseCallback);
	Utility::clearErrorStack();
	SSL_CTX_set_options(_pSSLContext, SSL_OP_ALL);
}


} } // namespace Poco::Net
