//
// X509Certificate.cpp
//
// Library: NetSSL_Win
// Package: Certificate
// Module:  X509Certificate
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/X509Certificate.h"
#include "Poco/Net/SSLException.h"
#include "Poco/StreamCopier.h"
#include "Poco/String.h"
#include "Poco/DateTimeParser.h"
#include "Poco/Base64Encoder.h"
#include "Poco/Base64Decoder.h"
#include "Poco/File.h"
#include "Poco/FileStream.h"
#include "Poco/MemoryStream.h"
#include "Poco/Buffer.h"
#include "Poco/UnicodeConverter.h"
#include "Poco/Format.h"
#include "Poco/RegularExpression.h"
#include "Poco/Net/DNS.h"


namespace Poco {
namespace Net {


X509Certificate::X509Certificate(const std::string& path):
	_pCert(0)
{
	importCertificate(path);
	init();
}


X509Certificate::X509Certificate(std::istream& istr):
	_pCert(0)
{
	importCertificate(istr);
	init();
}


X509Certificate::X509Certificate(const std::string& certName, const std::string& certStoreName, bool useMachineStore):
	_pCert(0)
{
	loadCertificate(certName, certStoreName, useMachineStore);
	init();
}


X509Certificate::X509Certificate(PCCERT_CONTEXT pCert):
	_pCert(pCert)
{
	poco_check_ptr(_pCert);

	init();
}


X509Certificate::X509Certificate(const X509Certificate& cert):
	_issuerName(cert._issuerName),
	_subjectName(cert._subjectName),
	_pCert(cert._pCert)
{
	_pCert = CertDuplicateCertificateContext(_pCert);
}


X509Certificate::X509Certificate(PCCERT_CONTEXT pCert, bool shared):
	_pCert(pCert)
{
	poco_check_ptr(_pCert);

	if (shared)
	{
		_pCert = CertDuplicateCertificateContext(_pCert);
	}

	init();
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
	CertFreeCertificateContext(_pCert);
}


void X509Certificate::init()
{
	std::string name = issuerName(NID_COUNTRY);
	if (!name.empty())
	{
		_issuerName += "/C=";
		_issuerName += name;
	}
	name = issuerName(NID_STATE_OR_PROVINCE);
	if (!name.empty())
	{
		_issuerName += "/ST=";
		_issuerName += name;
	}
	name = issuerName(NID_LOCALITY_NAME);
	if (!name.empty())
	{
		_issuerName += "/L=";
		_issuerName += name;
	}
	name = issuerName(NID_ORGANIZATION_NAME);
	if (!name.empty())
	{
		_issuerName += "/O=";
		_issuerName += name;
	}
	name = issuerName(NID_ORGANIZATION_UNIT_NAME);
	if (!name.empty())
	{
		_issuerName += "/OU=";
		_issuerName += name;
	}
	name = issuerName(NID_COMMON_NAME);
	if (!name.empty())
	{
		_issuerName += "/CN=";
		_issuerName += name;
	}

	name = subjectName(NID_COUNTRY);
	if (!name.empty())
	{
		_subjectName += "/C=";
		_subjectName += name;
	}
	name = subjectName(NID_STATE_OR_PROVINCE);
	if (!name.empty())
	{
		_subjectName += "/ST=";
		_subjectName += name;
	}
	name = subjectName(NID_LOCALITY_NAME);
	if (!name.empty())
	{
		_subjectName += "/L=";
		_subjectName += name;
	}
	name = subjectName(NID_ORGANIZATION_NAME);
	if (!name.empty())
	{
		_subjectName += "/O=";
		_subjectName += name;
	}
	name = subjectName(NID_ORGANIZATION_UNIT_NAME);
	if (!name.empty())
	{
		_subjectName += "/OU=";
		_subjectName += name;
	}
	name = subjectName(NID_COMMON_NAME);
	if (!name.empty())
	{
		_subjectName += "/CN=";
		_subjectName += name;
	}}


std::string X509Certificate::commonName() const
{
	return subjectName(NID_COMMON_NAME);
}


std::string X509Certificate::issuerName(NID nid) const
{
	std::string result;
	DWORD size = CertGetNameStringW(_pCert, CERT_NAME_ATTR_TYPE, CERT_NAME_ISSUER_FLAG, nid2oid(nid), 0, 0);
	Poco::Buffer<wchar_t> data(size);
	CertGetNameStringW(_pCert, CERT_NAME_ATTR_TYPE, CERT_NAME_ISSUER_FLAG, nid2oid(nid), data.begin(), size);
	Poco::UnicodeConverter::convert(data.begin(), result);
	return result;
}


std::string X509Certificate::subjectName(NID nid) const
{
	std::string result;
	DWORD size = CertGetNameStringW(_pCert, CERT_NAME_ATTR_TYPE, 0, nid2oid(nid), 0, 0);
	Poco::Buffer<wchar_t> data(size);
	CertGetNameStringW(_pCert, CERT_NAME_ATTR_TYPE, 0, nid2oid(nid), data.begin(), size);
	Poco::UnicodeConverter::convert(data.begin(), result);
	return result;
}


void X509Certificate::extractNames(std::string& cmnName, std::set<std::string>& domainNames) const
{
	domainNames.clear();
	cmnName = commonName();
	PCERT_EXTENSION pExt = _pCert->pCertInfo->rgExtension;
	for (int i = 0; i < _pCert->pCertInfo->cExtension; i++, pExt++)
	{
		if (std::strcmp(pExt->pszObjId, szOID_SUBJECT_ALT_NAME2) == 0)
		{
			DWORD flags(0);
#if defined(CRYPT_DECODE_ENABLE_PUNYCODE_FLAG)
			flags |= CRYPT_DECODE_ENABLE_PUNYCODE_FLAG;
#endif
			Poco::Buffer<char> buffer(256);
			DWORD bufferSize = buffer.sizeBytes();
			BOOL rc = CryptDecodeObjectEx(
					X509_ASN_ENCODING | PKCS_7_ASN_ENCODING,
					pExt->pszObjId,
					pExt->Value.pbData,
					pExt->Value.cbData,
					flags,
					0,
					buffer.begin(),
					&bufferSize);
			if (!rc && GetLastError() == ERROR_MORE_DATA)
			{
				buffer.resize(bufferSize);
				rc = CryptDecodeObjectEx(
					X509_ASN_ENCODING | PKCS_7_ASN_ENCODING,
					pExt->pszObjId,
					pExt->Value.pbData,
					pExt->Value.cbData,
					flags,
					0,
					buffer.begin(),
					&bufferSize);
			}
			if (rc)
			{
				PCERT_ALT_NAME_INFO pNameInfo = reinterpret_cast<PCERT_ALT_NAME_INFO>(buffer.begin());
				for (int i = 0; i < pNameInfo->cAltEntry; i++)
				{
					std::wstring waltName(pNameInfo->rgAltEntry[i].pwszDNSName);
					std::string altName;
					Poco::UnicodeConverter::toUTF8(waltName, altName);
					domainNames.insert(altName);
				}
			}
		}
	}
	if (!cmnName.empty() && domainNames.empty())
	{
		domainNames.insert(cmnName);
	}
}


Poco::DateTime X509Certificate::validFrom() const
{
	Poco::Timestamp ts = Poco::Timestamp::fromFileTimeNP(_pCert->pCertInfo->NotBefore.dwLowDateTime, _pCert->pCertInfo->NotBefore.dwHighDateTime);
	return Poco::DateTime(ts);
}


Poco::DateTime X509Certificate::expiresOn() const
{
	Poco::Timestamp ts = Poco::Timestamp::fromFileTimeNP(_pCert->pCertInfo->NotAfter.dwLowDateTime, _pCert->pCertInfo->NotAfter.dwHighDateTime);
	return Poco::DateTime(ts);
}


bool X509Certificate::issuedBy(const X509Certificate& issuerCertificate) const
{
	CERT_CHAIN_PARA chainPara;
	PCCERT_CHAIN_CONTEXT pChainContext = 0;
	std::memset(&chainPara, 0, sizeof(chainPara));
	chainPara.cbSize = sizeof(chainPara);

	if (!CertGetCertificateChain(
							NULL,
							_pCert,
							NULL,
							NULL,
							&chainPara,
							0,
							NULL,
							&pChainContext))
	{
		throw SSLException("Cannot get certificate chain", subjectName(), GetLastError());
	}

	bool result = false;
	for (DWORD i = 0; i < pChainContext->cChain && !result; i++)
	{
		for (DWORD k = 0; k < pChainContext->rgpChain[i]->cElement && !result; k++)
		{
			PCCERT_CONTEXT pChainCert = pChainContext->rgpChain[i]->rgpElement[k]->pCertContext;
			if (CertCompareCertificate(X509_ASN_ENCODING | PKCS_7_ASN_ENCODING, issuerCertificate.system()->pCertInfo, pChainCert->pCertInfo))
			{
				if (CertComparePublicKeyInfo(X509_ASN_ENCODING | PKCS_7_ASN_ENCODING, &issuerCertificate.system()->pCertInfo->SubjectPublicKeyInfo, &pChainCert->pCertInfo->SubjectPublicKeyInfo))
				{
					result = true;
				}
			}
		}
	}
	CertFreeCertificateChain(pChainContext);
	return result;
}


void* X509Certificate::nid2oid(NID nid)
{
	const char* result = 0;
	switch (nid)
	{
	case NID_COMMON_NAME:
		result = szOID_COMMON_NAME;
		break;
	case NID_COUNTRY:
		result = szOID_COUNTRY_NAME;
		break;
	case NID_LOCALITY_NAME:
		result = szOID_LOCALITY_NAME;
		break;
	case NID_STATE_OR_PROVINCE:
		result = szOID_STATE_OR_PROVINCE_NAME;
		break;
	case NID_ORGANIZATION_NAME:
		result = szOID_ORGANIZATION_NAME;
		break;
	case NID_ORGANIZATION_UNIT_NAME:
		result = szOID_ORGANIZATIONAL_UNIT_NAME;
		break;
	default:
		poco_bugcheck();
		result = "";
		break;
	}
	return const_cast<char*>(result);
}


void X509Certificate::loadCertificate(const std::string& certName, const std::string& certStoreName, bool useMachineStore)
{
	std::wstring wcertStore;
	Poco::UnicodeConverter::convert(certStoreName, wcertStore);
	HCERTSTORE hCertStore;
	if (useMachineStore)
		hCertStore = CertOpenStore(CERT_STORE_PROV_SYSTEM, 0, 0, CERT_SYSTEM_STORE_LOCAL_MACHINE, certStoreName.c_str());
	else
		hCertStore = CertOpenSystemStoreW(0, wcertStore.c_str());

	if (!hCertStore) throw CertificateException("Failed to open certificate store", certStoreName, GetLastError());

	CERT_RDN_ATTR cert_rdn_attr;
	cert_rdn_attr.pszObjId = szOID_COMMON_NAME;
	cert_rdn_attr.dwValueType = CERT_RDN_ANY_TYPE;
	cert_rdn_attr.Value.cbData = static_cast<DWORD>(certName.size());
	cert_rdn_attr.Value.pbData = reinterpret_cast<BYTE*>(const_cast<char*>(certName.c_str()));

	CERT_RDN cert_rdn;
	cert_rdn.cRDNAttr = 1;
	cert_rdn.rgRDNAttr = &cert_rdn_attr;

	_pCert = CertFindCertificateInStore(hCertStore, X509_ASN_ENCODING, 0, CERT_FIND_SUBJECT_ATTR, &cert_rdn, NULL);
	if (!_pCert)
	{
		CertCloseStore(hCertStore, 0);
		throw NoCertificateException(Poco::format("Failed to find certificate %s in store %s", certName, certStoreName));
	}
	CertCloseStore(hCertStore, 0);
}


void X509Certificate::importCertificate(const std::string& certPath)
{
	Poco::File certFile(certPath);
	if (!certFile.exists()) throw Poco::FileNotFoundException(certPath);
	Poco::File::FileSize size = certFile.getSize();
	if (size < 32) throw Poco::DataFormatException("certificate file too small", certPath);
	Poco::Buffer<char> buffer(static_cast<std::size_t>(size));
	Poco::FileInputStream istr(certPath);
	istr.read(buffer.begin(), buffer.size());
	if (istr.gcount() != size) throw Poco::IOException("error reading certificate file");
	importCertificate(buffer.begin(), buffer.size());
}


void X509Certificate::importCertificate(std::istream& istr)
{
	std::string data;
	Poco::StreamCopier::copyToString(istr, data);
	if (!data.empty())
	{
		importCertificate(data.data(), data.size());
	}
	else throw Poco::IOException("failed to read certificate from stream");
}


void X509Certificate::importCertificate(const char* pBuffer, std::size_t size)
{
	if (std::memcmp(pBuffer, "-----BEGIN CERTIFICATE-----", 27) == 0)
		importPEMCertificate(pBuffer + 27, size - 27);
	else
		importDERCertificate(pBuffer, size);
}


void X509Certificate::importPEMCertificate(const char* pBuffer, std::size_t size)
{
	Poco::Buffer<char> derBuffer(size);
	std::size_t derSize = 0;

	const char* pemBegin = pBuffer;
	const char* pemEnd = pemBegin + (size - 25);
	while (pemEnd > pemBegin && std::memcmp(pemEnd, "-----END CERTIFICATE-----", 25) != 0) --pemEnd;
	if (pemEnd == pemBegin) throw Poco::DataFormatException("Not a valid PEM file - end marker missing");

	Poco::MemoryInputStream istr(pemBegin, pemEnd - pemBegin);
	Poco::Base64Decoder dec(istr);

	char* derBegin = derBuffer.begin();
	char* derEnd = derBegin;

	int ch = dec.get();
	while (ch != -1)
	{
		*derEnd++ = static_cast<char>(ch);
		ch = dec.get();
	}

	importDERCertificate(derBegin, derEnd - derBegin);
}


void X509Certificate::importDERCertificate(const char* pBuffer, std::size_t size)
{
	_pCert = CertCreateCertificateContext(X509_ASN_ENCODING, reinterpret_cast<const BYTE*>(pBuffer), static_cast<DWORD>(size));
	if (!_pCert)
	{
		throw Poco::DataFormatException("Failed to load certificate from file", GetLastError());
	}
}


bool X509Certificate::verify(const std::string& hostName) const
{
	return verify(*this, hostName);
}


bool X509Certificate::verify(const Poco::Net::X509Certificate& certificate, const std::string& hostName)
{
	std::string commonName;
	std::set<std::string> dnsNames;
	certificate.extractNames(commonName, dnsNames);
	if (!commonName.empty()) dnsNames.insert(commonName);
	bool ok = (dnsNames.find(hostName) != dnsNames.end());
	if (!ok)
	{
		for (std::set<std::string>::const_iterator it = dnsNames.begin(); !ok && it != dnsNames.end(); ++it)
		{
			try
			{
				// two cases: strData contains wildcards or not
				if (containsWildcards(*it))
				{
					// a compare by IPAddress is not possible with wildcards
					// only allow compare by name
					ok = matchWildcard(*it, hostName);
				}
				else
				{
					// it depends on hostName if we compare by IP or by alias
					IPAddress ip;
					if (IPAddress::tryParse(hostName, ip))
					{
						// compare by IP
						const HostEntry& heData = DNS::resolve(*it);
						const HostEntry::AddressList& addr = heData.addresses();
						HostEntry::AddressList::const_iterator it = addr.begin();
						HostEntry::AddressList::const_iterator itEnd = addr.end();
						for (; it != itEnd && !ok; ++it)
						{
							ok = (*it == ip);
						}
					}
					else
					{
						ok = Poco::icompare(*it, hostName) == 0;
					}
				}
			}
			catch (NoAddressFoundException&)
			{
			}
			catch (HostNotFoundException&)
			{
			}
		}
	}
	return ok;
}


bool X509Certificate::containsWildcards(const std::string& commonName)
{
	return (commonName.find('*') != std::string::npos || commonName.find('?') != std::string::npos);
}


bool X509Certificate::matchWildcard(const std::string& wildcard, const std::string& hostName)
{
	// fix wildcards
	std::string wildcardExpr("^");
	wildcardExpr += Poco::replace(wildcard, ".", "\\.");
	Poco::replaceInPlace(wildcardExpr, "*", ".*");
	Poco::replaceInPlace(wildcardExpr, "..*", ".*");
	Poco::replaceInPlace(wildcardExpr, "?", ".?");
	Poco::replaceInPlace(wildcardExpr, "..?", ".?");
	wildcardExpr += "$";

	Poco::RegularExpression expr(wildcardExpr, Poco::RegularExpression::RE_CASELESS);
	return expr.match(hostName);
}


} } // namespace Poco::Net
