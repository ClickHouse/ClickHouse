//
// Context.cpp
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


#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include "Poco/Net/Context.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Net/SSLException.h"
#include "Poco/Net/Utility.h"
#include "Poco/Crypto/OpenSSLInitializer.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/DirectoryIterator.h"
#include "Poco/RegularExpression.h"
#include "Poco/Timestamp.h"


namespace Poco {
namespace Net {


Context::Params::Params():
	verificationMode(VERIFY_RELAXED),
	verificationDepth(9),
	loadDefaultCAs(false),
	cipherList("ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH")
{
}


Context::Context(Usage usage, const Params& params):
	_usage(usage),
	_mode(params.verificationMode),
	_pSSLContext(0),
	_extendedCertificateVerification(true)
{
	init(params);
}


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
	Params params;
	params.privateKeyFile = privateKeyFile;
	params.certificateFile = certificateFile;
	params.caLocation = caLocation;
	params.verificationMode = verificationMode;
	params.verificationDepth = verificationDepth;
	params.loadDefaultCAs = loadDefaultCAs;
	params.cipherList = cipherList;
	init(params);
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
	Params params;
	params.caLocation = caLocation;
	params.verificationMode = verificationMode;
	params.verificationDepth = verificationDepth;
	params.loadDefaultCAs = loadDefaultCAs;
	params.cipherList = cipherList;
	init(params);
}


Context::~Context()
{
    if (_pSSLContext == nullptr)
    {
        return;
    }

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

static bool poco_dir_cert(const std::string & dir)
{
	if (dir.empty())
		return false;

	File f(dir);
	return f.exists() && f.isDirectory();
}

static bool poco_dir_contains_certs(const std::string & dir)
{
	RegularExpression re("^[a-fA-F0-9]{8}\\.\\d$");
	try
	{
		for (DirectoryIterator it(dir), end; it != end; ++it)
			if (re.match(Path(it->path()).getFileName()))
				return true;
	}
	catch (Poco::Exception& exc) {}

	return false;
}

static bool poco_file_cert(const std::string & file)
{
	if (file.empty())
		return false;

	File f(file);
	return f.exists() && f.isFile();
}

static int poco_ssl_probe_and_set_default_ca_location(SSL_CTX *ctx, Context::CAPaths &caPaths)
{
	/* The probe paths are based on:
		* https://www.happyassassin.net/posts/2015/01/12/a-note-about-ssltls-trusted-certificate-stores-and-platforms/
		* Golang's crypto probing paths:
		*   https://golang.org/search?q=certFiles   and certDirectories
		*/
	static const char *paths[] = {
		"/etc/pki/tls/certs/ca-bundle.crt",
		"/etc/ssl/certs/ca-bundle.crt",
		"/etc/pki/tls/certs/ca-bundle.trust.crt",
		"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",

		"/etc/ssl/ca-bundle.pem",
		"/etc/pki/tls/cacert.pem",
		"/etc/ssl/cert.pem",
		"/etc/ssl/cacert.pem",

		"/etc/certs/ca-certificates.crt",
		"/etc/ssl/certs/ca-certificates.crt",

		"/etc/ssl/certs",

		"/usr/local/etc/ssl/cert.pem",
		"/usr/local/etc/ssl/cacert.pem",

		"/usr/local/etc/ssl/certs/cert.pem",
		"/usr/local/etc/ssl/certs/cacert.pem",

		/* BSD */
		"/usr/local/share/certs/ca-root-nss.crt",
		"/etc/openssl/certs/ca-certificates.crt",
#ifdef __APPLE__
		"/private/etc/ssl/cert.pem",
		"/private/etc/ssl/certs",
		"/usr/local/etc/openssl@1.1/cert.pem",
		"/usr/local/etc/openssl@1.0/cert.pem",
		"/usr/local/etc/openssl/certs",
		"/System/Library/OpenSSL",
#endif
#ifdef _AIX
		"/var/ssl/certs/ca-bundle.crt",
#endif
	};

	const char * dir = nullptr;
	for (const char * path : paths)
	{
		if (poco_dir_cert(path))
		{
			if (dir == nullptr)
				dir = path;

			if (poco_dir_contains_certs(path) && SSL_CTX_load_verify_locations(ctx, NULL, path))
			{
				caPaths.caDefaultDir = path;
				return 1;
			}
		}

		if (SSL_CTX_load_verify_locations(ctx, path, NULL))
		{
			caPaths.caDefaultFile = path;
			return 1;
		}
	}

	if (dir != nullptr)
	{
		caPaths.caDefaultDir = dir;
		return SSL_CTX_load_verify_locations(ctx, NULL, dir);
	}

	return 0;
}


void Context::init(const Params& params)
{
	Poco::Crypto::OpenSSLInitializer::initialize();

	createSSLContext();

	try
	{
		int errCode = 0;
		if (!params.caLocation.empty())
		{
			Poco::File aFile(params.caLocation);
			if (aFile.isDirectory())
				errCode = SSL_CTX_load_verify_locations(_pSSLContext, 0, Poco::Path::transcode(params.caLocation).c_str());
			else
				errCode = SSL_CTX_load_verify_locations(_pSSLContext, Poco::Path::transcode(params.caLocation).c_str(), 0);
			if (errCode != 1)
			{
				std::string msg = Utility::getLastError();
				throw SSLContextException(std::string("Cannot load CA file/directory at ") + params.caLocation, msg);
			}
			_caPaths.caLocation = params.caLocation;
		}

		if (params.loadDefaultCAs)
		{
			const char * dir = getenv(X509_get_default_cert_dir_env());
			if (!dir)
				dir = X509_get_default_cert_dir();

			const char * file = getenv(X509_get_default_cert_file_env());
			if (!file)
				file = X509_get_default_cert_file();

			if (poco_file_cert(file))
			{
				_caPaths.caDefaultFile = file;
				errCode = SSL_CTX_set_default_verify_paths(_pSSLContext);
			}
			else
			{
				if (poco_dir_cert(dir))
				{
					errCode = 0;
					if (!poco_dir_contains_certs(dir))
						errCode = poco_ssl_probe_and_set_default_ca_location(_pSSLContext, _caPaths);

					if (errCode == 0)
					{
						errCode = SSL_CTX_set_default_verify_paths(_pSSLContext);
						_caPaths.caDefaultDir = dir;
					}
				}
				else
					errCode = poco_ssl_probe_and_set_default_ca_location(_pSSLContext, _caPaths);
			}

			if (errCode != 1)
			{
				std::string msg = Utility::getLastError();
				throw SSLContextException("Cannot load default CA certificates", msg);
			}
		}

		if (!params.privateKeyFile.empty())
		{
			errCode = SSL_CTX_use_PrivateKey_file(_pSSLContext, Poco::Path::transcode(params.privateKeyFile).c_str(), SSL_FILETYPE_PEM);
			if (errCode != 1)
			{
				std::string msg = Utility::getLastError();
				throw SSLContextException(std::string("Error loading private key from file ") + params.privateKeyFile, msg);
			}
		}

		if (!params.certificateFile.empty())
		{
			errCode = SSL_CTX_use_certificate_chain_file(_pSSLContext, Poco::Path::transcode(params.certificateFile).c_str());
			if (errCode != 1)
			{
				std::string errMsg = Utility::getLastError();
				throw SSLContextException(std::string("Error loading certificate from file ") + params.certificateFile, errMsg);
			}
		}

		if (isForServerUse())
			SSL_CTX_set_verify(_pSSLContext, params.verificationMode, &SSLManager::verifyServerCallback);
		else
			SSL_CTX_set_verify(_pSSLContext, params.verificationMode, &SSLManager::verifyClientCallback);

		SSL_CTX_set_cipher_list(_pSSLContext, params.cipherList.c_str());
		SSL_CTX_set_verify_depth(_pSSLContext, params.verificationDepth);
		SSL_CTX_set_mode(_pSSLContext, SSL_MODE_AUTO_RETRY);
		SSL_CTX_set_session_cache_mode(_pSSLContext, SSL_SESS_CACHE_OFF);

		initDH(params.dhParamsFile);
		initECDH(params.ecdhCurve);
	}
	catch (...)
	{
		SSL_CTX_free(_pSSLContext);
		throw;
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
	X509* pCert = certificate.dup();
	int errCode = SSL_CTX_add_extra_chain_cert(_pSSLContext, pCert);
	if (errCode != 1)
	{
		X509_free(pCert);
		std::string msg = Utility::getLastError();
		throw SSLContextException("Cannot add chain certificate to Context", msg);
	}
}


void Context::addCertificateAuthority(const Crypto::X509Certificate &certificate)
{
	if (X509_STORE* store = SSL_CTX_get_cert_store(_pSSLContext))
	{
		int errCode = X509_STORE_add_cert(store, const_cast<X509*>(certificate.certificate()));
		if (errCode != 1)
		{
			std::string msg = Utility::getLastError();
			throw SSLContextException("Cannot add certificate authority to Context", msg);
		}
	}
	else
	{
		std::string msg = Utility::getLastError();
		throw SSLContextException("Cannot add certificate authority to Context", msg);
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


void Context::disableProtocols(int protocols)
{
	if (protocols & PROTO_SSLV2)
	{
#if defined(SSL_OP_NO_SSLv2)
		SSL_CTX_set_options(_pSSLContext, SSL_OP_NO_SSLv2);
#endif
	}
	if (protocols & PROTO_SSLV3)
	{
#if defined(SSL_OP_NO_SSLv3)
		SSL_CTX_set_options(_pSSLContext, SSL_OP_NO_SSLv3);
#endif
	}
	if (protocols & PROTO_TLSV1)
	{
#if defined(SSL_OP_NO_TLSv1)
		SSL_CTX_set_options(_pSSLContext, SSL_OP_NO_TLSv1);
#endif
	}
	if (protocols & PROTO_TLSV1_1)
	{
#if defined(SSL_OP_NO_TLSv1_1)
		SSL_CTX_set_options(_pSSLContext, SSL_OP_NO_TLSv1_1);
#endif
	}
	if (protocols & PROTO_TLSV1_2)
	{
#if defined(SSL_OP_NO_TLSv1_2)
		SSL_CTX_set_options(_pSSLContext, SSL_OP_NO_TLSv1_2);
#endif
	}
}


void Context::preferServerCiphers()
{
#if defined(SSL_OP_CIPHER_SERVER_PREFERENCE)
	SSL_CTX_set_options(_pSSLContext, SSL_OP_CIPHER_SERVER_PREFERENCE);
#endif
}

const Context::CAPaths &Context::getCAPaths()
{
	return _caPaths;
}


void Context::createSSLContext()
{
	if (SSLManager::isFIPSEnabled())
	{
		_pSSLContext = SSL_CTX_new(TLS_method());
	}
	else
	{
		switch (_usage)
		{
		case CLIENT_USE:
			_pSSLContext = SSL_CTX_new(TLS_client_method());
			break;
		case SERVER_USE:
			_pSSLContext = SSL_CTX_new(TLS_server_method());
			break;
		default:
			throw Poco::InvalidArgumentException("Invalid or unsupported usage");
		}
	}
	if (!_pSSLContext)
	{
		uint64_t err = ERR_get_error();
		throw SSLException("Cannot create SSL_CTX object", ERR_error_string(err, nullptr));
	}

	SSL_CTX_set_default_passwd_cb(_pSSLContext, &SSLManager::privateKeyPassphraseCallback);
	Utility::clearErrorStack();
	SSL_CTX_set_options(_pSSLContext, SSL_OP_ALL);
	SSL_CTX_set_options(_pSSLContext, SSL_OP_IGNORE_UNEXPECTED_EOF);
}


void Context::initDH(const std::string& dhParamsFile)
{
	if (!dhParamsFile.empty())
	{
        BIO* bio = BIO_new_file(dhParamsFile.c_str(), "r");
        if (!bio)
        {
            std::string msg = Utility::getLastError();
            throw SSLContextException("Error opening Diffie-Hellman parameters file " + dhParamsFile, msg);
        }

        EVP_PKEY* dh_params = PEM_read_bio_Parameters(bio, nullptr);
        BIO_free(bio);

        if (!dh_params)
        {
            std::string msg = Utility::getLastError();
            throw SSLContextException("Error reading Diffie-Hellman parameters from file " + dhParamsFile, msg);
        }

        if (SSL_CTX_set0_tmp_dh_pkey(_pSSLContext, dh_params) != 1)
        {
            EVP_PKEY_free(dh_params);
            throw SSLContextException("Failed to set DH parameters in SSL context", Utility::getLastError());
        }

        SSL_CTX_set_options(_pSSLContext, SSL_OP_SINGLE_DH_USE);
	}
	else
        SSL_CTX_set_dh_auto(_pSSLContext, 1);
}


void Context::initECDH(const std::string& curve)
{
    std::string use_curve = curve.empty() ? "P-256" : curve;
    if (SSL_CTX_set1_groups_list(_pSSLContext, use_curve.c_str()) != 1)
    {
        throw SSLContextException("Unsupported ECDH curve or group: " + use_curve);
    }
}


} } // namespace Poco::Net
