//
// X509Certificate.cpp
//
// Library: Crypto
// Package: Certificate
// Module:  X509Certificate
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/X509Certificate.h"
#include "Poco/Crypto/CryptoException.h"
#include "Poco/StreamCopier.h"
#include "Poco/String.h"
#include "Poco/DateTimeParser.h"
#include "Poco/Format.h"
#include <sstream>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <openssl/err.h>
#include <openssl/evp.h>


namespace Poco {
namespace Crypto {


X509Certificate::X509Certificate(std::istream& istr):
	_pCert(0)
{
	load(istr);
}


X509Certificate::X509Certificate(const std::string& path):
	_pCert(0)
{
	load(path);
}


X509Certificate::X509Certificate(X509* pCert):
	_pCert(pCert)
{
	poco_check_ptr(_pCert);

	init();
}


X509Certificate::X509Certificate(X509* pCert, bool shared):
	_pCert(pCert)
{
	poco_check_ptr(_pCert);

	if (shared)
	{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		X509_up_ref(_pCert);
#else
		_pCert->references++;
#endif
	}

	init();
}


X509Certificate::X509Certificate(const X509Certificate& cert):
	_issuerName(cert._issuerName),
	_subjectName(cert._subjectName),
	_serialNumber(cert._serialNumber),
	_pCert(cert._pCert)
{
	_pCert = X509_dup(_pCert);
}


X509Certificate& X509Certificate::operator = (const X509Certificate& cert)
{
	X509Certificate tmp(cert);
	swap(tmp);
	return *this;
}


void X509Certificate::swap(X509Certificate& cert)
{
	using std::swap;
	swap(cert._issuerName, _issuerName);
	swap(cert._subjectName, _subjectName);
	swap(cert._serialNumber, _serialNumber);
	swap(cert._pCert, _pCert);
}


X509Certificate::~X509Certificate()
{
	X509_free(_pCert);
}


void X509Certificate::load(std::istream& istr)
{
	poco_assert (!_pCert);

	std::stringstream certStream;
	Poco::StreamCopier::copyStream(istr, certStream);
	std::string cert = certStream.str();

	BIO *pBIO = BIO_new_mem_buf(const_cast<char*>(cert.data()), static_cast<int>(cert.size()));
	if (!pBIO) throw Poco::IOException("Cannot create BIO for reading certificate");
	_pCert = PEM_read_bio_X509(pBIO, 0, 0, 0);
	BIO_free(pBIO);

	if (!_pCert) throw Poco::IOException("Failed to load certificate from stream");

	init();
}


void X509Certificate::load(const std::string& path)
{
	poco_assert (!_pCert);

	BIO *pBIO = BIO_new(BIO_s_file());
	if (!pBIO) throw Poco::IOException("Cannot create BIO for reading certificate file", path);
	if (!BIO_read_filename(pBIO, const_cast<char *>(path.c_str())))
	{
		BIO_free(pBIO);
		throw Poco::OpenFileException("Cannot open certificate file for reading", path);
	}

	_pCert = PEM_read_bio_X509(pBIO, 0, 0, 0);
	BIO_free(pBIO);

	if (!_pCert) throw Poco::ReadFileException("Failed to load certificate from", path);

	init();
}


void X509Certificate::save(std::ostream& stream) const
{
	BIO *pBIO = BIO_new(BIO_s_mem());
	if (!pBIO) throw Poco::IOException("Cannot create BIO for writing certificate");
	try
	{
		if (!PEM_write_bio_X509(pBIO, _pCert))
			throw Poco::IOException("Failed to write certificate to stream");

		char *pData;
		long size;
		size = BIO_get_mem_data(pBIO, &pData);
		stream.write(pData, size);
	}
	catch (...)
	{
		BIO_free(pBIO);
		throw;
	}
	BIO_free(pBIO);
}


void X509Certificate::save(const std::string& path) const
{
	BIO *pBIO = BIO_new(BIO_s_file());
	if (!pBIO) throw Poco::IOException("Cannot create BIO for reading certificate file", path);
	if (!BIO_write_filename(pBIO, const_cast<char*>(path.c_str())))
	{
		BIO_free(pBIO);
		throw Poco::CreateFileException("Cannot create certificate file", path);
	}
	try
	{
		if (!PEM_write_bio_X509(pBIO, _pCert))
			throw Poco::WriteFileException("Failed to write certificate to file", path);
	}
	catch (...)
	{
		BIO_free(pBIO);
		throw;
	}
	BIO_free(pBIO);
}


void X509Certificate::init()
{
	char buffer[NAME_BUFFER_SIZE];
	X509_NAME_oneline(X509_get_issuer_name(_pCert), buffer, sizeof(buffer));
	_issuerName = buffer;
	X509_NAME_oneline(X509_get_subject_name(_pCert), buffer, sizeof(buffer));
	_subjectName = buffer;
	BIGNUM* pBN = ASN1_INTEGER_to_BN(X509_get_serialNumber(const_cast<X509*>(_pCert)), 0);
	if (pBN)
	{
		char* pSN = BN_bn2hex(pBN);
		if (pSN)
		{
			_serialNumber = pSN;
			OPENSSL_free(pSN);
		}
		BN_free(pBN);
	}
}


std::string X509Certificate::commonName() const
{
	return subjectName(NID_COMMON_NAME);
}


std::string X509Certificate::issuerName(NID nid) const
{
	if (X509_NAME* issuer = X509_get_issuer_name(_pCert))
	{
		char buffer[NAME_BUFFER_SIZE];
		if (X509_NAME_get_text_by_NID(issuer, nid, buffer, sizeof(buffer)) >= 0)
			return std::string(buffer);
	}
	return std::string();
}


std::string X509Certificate::subjectName(NID nid) const
{
	if (X509_NAME* subj = X509_get_subject_name(_pCert))
	{
		char buffer[NAME_BUFFER_SIZE];
		if (X509_NAME_get_text_by_NID(subj, nid, buffer, sizeof(buffer)) >= 0)
			return std::string(buffer);
	}
	return std::string();
}


void X509Certificate::extractNames(std::string& cmnName, std::set<std::string>& domainNames) const
{
	domainNames.clear();
	if (STACK_OF(GENERAL_NAME)* names = static_cast<STACK_OF(GENERAL_NAME)*>(X509_get_ext_d2i(_pCert, NID_subject_alt_name, 0, 0)))
	{
		for (int i = 0; i < sk_GENERAL_NAME_num(names); ++i)
		{
			const GENERAL_NAME* name = sk_GENERAL_NAME_value(names, i);
			if (name->type == GEN_DNS)
			{
				const char* data = reinterpret_cast<char*>(ASN1_STRING_data(name->d.ia5));
				std::size_t len = ASN1_STRING_length(name->d.ia5);
				domainNames.insert(std::string(data, len));
			}
		}
		GENERAL_NAMES_free(names);
	}

	cmnName = commonName();
	if (!cmnName.empty() && domainNames.empty())
	{
		domainNames.insert(cmnName);
	}
}


Poco::DateTime X509Certificate::validFrom() const
{
	ASN1_TIME* certTime = X509_get_notBefore(_pCert);
	std::string dateTime(reinterpret_cast<char*>(certTime->data));
	int tzd;
	return DateTimeParser::parse("%y%m%d%H%M%S", dateTime, tzd);
}


Poco::DateTime X509Certificate::expiresOn() const
{
	ASN1_TIME* certTime = X509_get_notAfter(_pCert);
	std::string dateTime(reinterpret_cast<char*>(certTime->data));
	int tzd;
	return DateTimeParser::parse("%y%m%d%H%M%S", dateTime, tzd);
}


bool X509Certificate::issuedBy(const X509Certificate& issuerCertificate) const
{
	X509* pCert = const_cast<X509*>(_pCert);
	X509* pIssuerCert = const_cast<X509*>(issuerCertificate.certificate());
	EVP_PKEY* pIssuerPublicKey = X509_get_pubkey(pIssuerCert);
	if (!pIssuerPublicKey) throw Poco::InvalidArgumentException("Issuer certificate has no public key");
	int rc = X509_verify(pCert, pIssuerPublicKey);
	EVP_PKEY_free(pIssuerPublicKey);
	return rc == 1;
}


bool X509Certificate::equals(const X509Certificate& otherCertificate) const
{
	X509* pCert = const_cast<X509*>(_pCert);
	X509* pOtherCert = const_cast<X509*>(otherCertificate.certificate());
	return X509_cmp(pCert, pOtherCert) == 0;
}


std::string X509Certificate::signatureAlgorithm() const
{
	int sigNID = NID_undef;

#if (OPENSSL_VERSION_NUMBER >=  0x1010000fL) && !defined(LIBRESSL_VERSION_NUMBER)
	sigNID = X509_get_signature_nid(_pCert);
#else
	poco_check_ptr(_pCert->sig_alg);
	sigNID = OBJ_obj2nid(_pCert->sig_alg->algorithm);
#endif

	if (sigNID != NID_undef)
	{
		const char* pAlgName = OBJ_nid2ln(sigNID);
		if (pAlgName) return std::string(pAlgName);
		else throw OpenSSLException(Poco::format("X509Certificate::"
				"signatureAlgorithm(): OBJ_nid2ln(%d)", sigNID));
	}
	else
		throw NotFoundException("X509Certificate::signatureAlgorithm()");

	return "";
}


X509Certificate::List X509Certificate::readPEM(const std::string& pemFileName)
{
	List caCertList;
	BIO* pBIO = BIO_new_file(pemFileName.c_str(), "r");
	if (pBIO == NULL) throw OpenFileException("X509Certificate::readPEM()");
	X509* x = PEM_read_bio_X509(pBIO, NULL, 0, NULL);
	if (!x) throw OpenSSLException(Poco::format("X509Certificate::readPEM(%s)", pemFileName));
	while(x)
	{
		caCertList.push_back(X509Certificate(x));
		x = PEM_read_bio_X509(pBIO, NULL, 0, NULL);
	}
	BIO_free(pBIO);
	return caCertList;
}

X509Certificate::List X509Certificate::readPEM(std::istream& istr)
{
	List caCertList;
	std::stringstream certStream;
	Poco::StreamCopier::copyStream(istr, certStream);
	std::string cert = certStream.str();

	BIO *pBIO = BIO_new_mem_buf(const_cast<char*>(cert.data()), static_cast<int>(cert.size()));
	if (!pBIO) throw Poco::IOException("Cannot create BIO for reading certificate");
	X509* x = PEM_read_bio_X509(pBIO, NULL, 0, NULL);
	if (!x) throw OpenSSLException("X509Certificate::readPEM(istream)");
	while(x)
	{
		caCertList.push_back(X509Certificate(x));
		x = PEM_read_bio_X509(pBIO, NULL, 0, NULL);
	}
	BIO_free(pBIO);
	return caCertList;
}

void X509Certificate::writePEM(const std::string& pemFileName, const List& list)
{
	BIO* pBIO = BIO_new_file(pemFileName.c_str(), "a");
	if (pBIO == NULL) throw OpenFileException("X509Certificate::writePEM()");
	List::const_iterator it = list.begin();
	List::const_iterator end = list.end();
	for (; it != end; ++it)
	{
		if (!PEM_write_bio_X509(pBIO, const_cast<X509*>(it->certificate())))
		{
			throw OpenSSLException("X509Certificate::writePEM()");
		}
	}
	BIO_free(pBIO);
}


void X509Certificate::print(std::ostream& out) const
{
	out << "subjectName: " << subjectName() << std::endl;
	out << "issuerName: " << issuerName() << std::endl;
	out << "commonName: " << commonName() << std::endl;
	out << "country: " << subjectName(X509Certificate::NID_COUNTRY) << std::endl;
	out << "localityName: " << subjectName(X509Certificate::NID_LOCALITY_NAME) << std::endl;
	out << "stateOrProvince: " << subjectName(X509Certificate::NID_STATE_OR_PROVINCE) << std::endl;
	out << "organizationName: " << subjectName(X509Certificate::NID_ORGANIZATION_NAME) << std::endl;
	out << "organizationUnitName: " << subjectName(X509Certificate::NID_ORGANIZATION_UNIT_NAME) << std::endl;
	out << "emailAddress: " << subjectName(X509Certificate::NID_PKCS9_EMAIL_ADDRESS) << std::endl;
	out << "serialNumber: " << subjectName(X509Certificate::NID_SERIAL_NUMBER) << std::endl;
}


} } // namespace Poco::Crypto
