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


#include "Poco/Net/Context.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Net/SSLException.h"
#include "Poco/Net/Utility.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/DirectoryIterator.h"
#include "Poco/RegularExpression.h"
#include "Poco/Timestamp.h"
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#if defined(BORINGSSL_API_VERSION)
#if BORINGSSL_API_VERSION <= 9
#define BORINGSSL_DEPRECATED 1
#endif
#endif

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
	SSL_CTX_flush_sessions_ex(_pSSLContext, static_cast<long>(now.epochTime()));
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
#if defined(SSL_OP_NO_TLSv1) && !defined(OPENSSL_NO_TLS1)
		case TLSV1_CLIENT_USE:
			_pSSLContext = SSL_CTX_new(TLS_client_method());
			break;
		case TLSV1_SERVER_USE:
			_pSSLContext = SSL_CTX_new(TLS_server_method());
			break;
#endif
#if defined(SSL_OP_NO_TLSv1_1) && !defined(OPENSSL_NO_TLS1)
/* SSL_OP_NO_TLSv1_1 is defined in ssl.h if the library version supports TLSv1.1.
 * OPENSSL_NO_TLS1 is defined in opensslconf.h or on the compiler command line
 * if TLS1.x was removed at OpenSSL library build time via Configure options.
 */
        case TLSV1_1_CLIENT_USE:
            _pSSLContext = SSL_CTX_new(TLS_client_method());
            break;
        case TLSV1_1_SERVER_USE:
            _pSSLContext = SSL_CTX_new(TLS_server_method());
            break;
#endif
#if defined(SSL_OP_NO_TLSv1_2) && !defined(OPENSSL_NO_TLS1)
        case TLSV1_2_CLIENT_USE:
            _pSSLContext = SSL_CTX_new(TLS_client_method());
            break;
        case TLSV1_2_SERVER_USE:
            _pSSLContext = SSL_CTX_new(TLS_server_method());
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
	SSL_CTX_set_options(_pSSLContext, SSL_OP_IGNORE_UNEXPECTED_EOF);
}


void Context::initDH(const std::string& dhParamsFile)
{
#ifndef OPENSSL_NO_DH
    // 1024-bit MODP Group with 160-bit prime order subgroup (RFC5114)
	// -----BEGIN DH PARAMETERS-----
	// MIIBDAKBgQCxC4+WoIDgHd6S3l6uXVTsUsmfvPsGo8aaap3KUtI7YWBz4oZ1oj0Y
	// mDjvHi7mUsAT7LSuqQYRIySXXDzUm4O/rMvdfZDEvXCYSI6cIZpzck7/1vrlZEc4
	// +qMaT/VbzMChUa9fDci0vUW/N982XBpl5oz9p21NpwjfH7K8LkpDcQKBgQCk0cvV
	// w/00EmdlpELvuZkF+BBN0lisUH/WQGz/FCZtMSZv6h5cQVZLd35pD1UE8hMWAhe0
	// sBuIal6RVH+eJ0n01/vX07mpLuGQnQ0iY/gKdqaiTAh6CR9THb8KAWm2oorWYqTR
	// jnOvoy13nVkY0IvIhY9Nzvl8KiSFXm7rIrOy5QICAKA=
	// -----END DH PARAMETERS-----
	//

	static const unsigned char dh1024_p[] =
	{
		0xB1,0x0B,0x8F,0x96,0xA0,0x80,0xE0,0x1D,0xDE,0x92,0xDE,0x5E,
		0xAE,0x5D,0x54,0xEC,0x52,0xC9,0x9F,0xBC,0xFB,0x06,0xA3,0xC6,
		0x9A,0x6A,0x9D,0xCA,0x52,0xD2,0x3B,0x61,0x60,0x73,0xE2,0x86,
		0x75,0xA2,0x3D,0x18,0x98,0x38,0xEF,0x1E,0x2E,0xE6,0x52,0xC0,
		0x13,0xEC,0xB4,0xAE,0xA9,0x06,0x11,0x23,0x24,0x97,0x5C,0x3C,
		0xD4,0x9B,0x83,0xBF,0xAC,0xCB,0xDD,0x7D,0x90,0xC4,0xBD,0x70,
		0x98,0x48,0x8E,0x9C,0x21,0x9A,0x73,0x72,0x4E,0xFF,0xD6,0xFA,
		0xE5,0x64,0x47,0x38,0xFA,0xA3,0x1A,0x4F,0xF5,0x5B,0xCC,0xC0,
		0xA1,0x51,0xAF,0x5F,0x0D,0xC8,0xB4,0xBD,0x45,0xBF,0x37,0xDF,
		0x36,0x5C,0x1A,0x65,0xE6,0x8C,0xFD,0xA7,0x6D,0x4D,0xA7,0x08,
		0xDF,0x1F,0xB2,0xBC,0x2E,0x4A,0x43,0x71,
	};

	static const unsigned char dh1024_g[] =
	{
		0xA4,0xD1,0xCB,0xD5,0xC3,0xFD,0x34,0x12,0x67,0x65,0xA4,0x42,
		0xEF,0xB9,0x99,0x05,0xF8,0x10,0x4D,0xD2,0x58,0xAC,0x50,0x7F,
		0xD6,0x40,0x6C,0xFF,0x14,0x26,0x6D,0x31,0x26,0x6F,0xEA,0x1E,
		0x5C,0x41,0x56,0x4B,0x77,0x7E,0x69,0x0F,0x55,0x04,0xF2,0x13,
		0x16,0x02,0x17,0xB4,0xB0,0x1B,0x88,0x6A,0x5E,0x91,0x54,0x7F,
		0x9E,0x27,0x49,0xF4,0xD7,0xFB,0xD7,0xD3,0xB9,0xA9,0x2E,0xE1,
		0x90,0x9D,0x0D,0x22,0x63,0xF8,0x0A,0x76,0xA6,0xA2,0x4C,0x08,
		0x7A,0x09,0x1F,0x53,0x1D,0xBF,0x0A,0x01,0x69,0xB6,0xA2,0x8A,
		0xD6,0x62,0xA4,0xD1,0x8E,0x73,0xAF,0xA3,0x2D,0x77,0x9D,0x59,
		0x18,0xD0,0x8B,0xC8,0x85,0x8F,0x4D,0xCE,0xF9,0x7C,0x2A,0x24,
		0x85,0x5E,0x6E,0xEB,0x22,0xB3,0xB2,0xE5,
	};

	DH* dh = 0;
	if (!dhParamsFile.empty())
	{
		BIO* bio = BIO_new_file(dhParamsFile.c_str(), "r");
		if (!bio)
		{
			std::string msg = Utility::getLastError();
			throw SSLContextException(std::string("Error opening Diffie-Hellman parameters file ") + dhParamsFile, msg);
		}
		dh = PEM_read_bio_DHparams(bio, 0, 0, 0);
		BIO_free(bio);
		if (!dh)
		{
			std::string msg = Utility::getLastError();
			throw SSLContextException(std::string("Error reading Diffie-Hellman parameters from file ") + dhParamsFile, msg);
		}
	}
	else
	{
		dh = DH_new();
		if (!dh)
		{
			std::string msg = Utility::getLastError();
			throw SSLContextException("Error creating Diffie-Hellman parameters", msg);
		}
#if !defined(LIBRESSL_VERSION_NUMBER) && !defined(BORINGSSL_DEPRECATED)
		BIGNUM* p = BN_bin2bn(dh1024_p, sizeof(dh1024_p), 0);
		BIGNUM* g = BN_bin2bn(dh1024_g, sizeof(dh1024_g), 0);
		DH_set0_pqg(dh, p, 0, g);
		DH_set_length(dh, 160);
		if (!p || !g)
		{
			DH_free(dh);
			throw SSLContextException("Error creating Diffie-Hellman parameters");
		}
#else
		dh->p = BN_bin2bn(dh1024_p, sizeof(dh1024_p), 0);
		dh->g = BN_bin2bn(dh1024_g, sizeof(dh1024_g), 0);
#ifdef BORINGSSL_DEPRECATED
		dh->priv_length = 160;
#else
		dh->length = 160;
#endif
		if ((!dh->p) || (!dh->g))
		{
			DH_free(dh);
			throw SSLContextException("Error creating Diffie-Hellman parameters");
		}
#endif
	}
	SSL_CTX_set_tmp_dh(_pSSLContext, dh);
	SSL_CTX_set_options(_pSSLContext, SSL_OP_SINGLE_DH_USE);
	DH_free(dh);
#else
	if (!dhParamsFile.empty())
		throw SSLContextException("OpenSSL does not support DH");
#endif
}


void Context::initECDH(const std::string& curve)
{
#ifndef OPENSSL_NO_ECDH
	int nid = 0;
	if (!curve.empty())
	{
		nid = OBJ_sn2nid(curve.c_str());
	}
	else
	{
		nid = OBJ_sn2nid("prime256v1");
	}
	if (nid == 0)
	{
		throw SSLContextException("Unknown ECDH curve name", curve);
	}

	EC_KEY* ecdh = EC_KEY_new_by_curve_name(nid);
	if (!ecdh)
	{
		throw SSLContextException("Cannot create ECDH curve");
	}
	SSL_CTX_set_tmp_ecdh(_pSSLContext, ecdh);
	SSL_CTX_set_options(_pSSLContext, SSL_OP_SINGLE_ECDH_USE);
	EC_KEY_free(ecdh);
#endif
}


} } // namespace Poco::Net
