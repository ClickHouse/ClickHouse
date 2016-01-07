//
// X509Certificate.cpp
//
// $Id: //poco/1.4/Crypto/src/X509Certificate.cpp#1 $
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
#include "Poco/StreamCopier.h"
#include "Poco/String.h"
#include "Poco/DateTimeParser.h"
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
		_pCert->references++;
	}

	init();
}


X509Certificate::X509Certificate(const X509Certificate& cert):
	_issuerName(cert._issuerName),
	_subjectName(cert._subjectName),
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
	
	if (!_pCert) throw Poco::IOException("Faild to load certificate from stream");

	init();
}


void X509Certificate::load(const std::string& path)
{
	poco_assert (!_pCert);

	BIO *pBIO = BIO_new(BIO_s_file());
	if (!pBIO) throw Poco::IOException("Cannot create BIO for reading certificate file", path);
	if (!BIO_read_filename(pBIO, path.c_str()))
	{
		BIO_free(pBIO);
		throw Poco::OpenFileException("Cannot open certificate file for reading", path);
	}
	
	_pCert = PEM_read_bio_X509(pBIO, 0, 0, 0);
	BIO_free(pBIO);
	
	if (!_pCert) throw Poco::ReadFileException("Faild to load certificate from", path);

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
		X509_NAME_get_text_by_NID(issuer, nid, buffer, sizeof(buffer));
		return std::string(buffer);
    }
    else return std::string();
}


std::string X509Certificate::subjectName(NID nid) const
{
	if (X509_NAME* subj = X509_get_subject_name(_pCert))
    {
		char buffer[NAME_BUFFER_SIZE];
		X509_NAME_get_text_by_NID(subj, nid, buffer, sizeof(buffer));
		return std::string(buffer);
    }
    else return std::string();
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


} } // namespace Poco::Crypto
