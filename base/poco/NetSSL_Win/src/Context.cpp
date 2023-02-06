//
// Context.cpp
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  Context
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/Context.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Net/SSLException.h"
#include "Poco/Net/Utility.h"
#include "Poco/UnicodeConverter.h"
#include "Poco/Format.h"
#include "Poco/File.h"
#include "Poco/FileStream.h"
#include "Poco/MemoryStream.h"
#include "Poco/Base64Decoder.h"
#include "Poco/Buffer.h"
#include <algorithm>
#include <cctype>


namespace Poco {
namespace Net {


const std::string Context::CERT_STORE_MY("MY");
const std::string Context::CERT_STORE_ROOT("ROOT");
const std::string Context::CERT_STORE_TRUST("TRUST");
const std::string Context::CERT_STORE_CA("CA");
const std::string Context::CERT_STORE_USERDS("USERDS");


Context::Context(Usage usage,
		const std::string& certNameOrPath, 
		VerificationMode verMode,
		int options,
		const std::string& certStore):
	_usage(usage),
	_mode(verMode),
	_options(options),
	_extendedCertificateVerification(true),
	_certNameOrPath(certNameOrPath),
	_certStoreName(certStore),
	_hMemCertStore(0),
	_hCollectionCertStore(0),
	_hRootCertStore(0),
	_hCertStore(0),
	_pCert(0),
	_securityFunctions(SSLManager::instance().securityFunctions())
{
	init();
}


Context::~Context()
{
	if (_pCert)
	{
		CertFreeCertificateContext(_pCert);
	}
	if (_hCertStore)
	{
		CertCloseStore(_hCertStore, 0);
	}
	CertCloseStore(_hCollectionCertStore, 0);
	CertCloseStore(_hMemCertStore, 0);
	if (_hRootCertStore)
	{
		CertCloseStore(_hRootCertStore, 0);
	}
	if (_hCreds.dwLower != 0 && _hCreds.dwUpper != 0)
	{
		_securityFunctions.FreeCredentialsHandle(&_hCreds);
	}
}


void Context::init()
{
	_hCreds.dwLower = 0;
	_hCreds.dwUpper = 0;

	_hMemCertStore = CertOpenStore(
					CERT_STORE_PROV_MEMORY,   // The memory provider type
					0,                        // The encoding type is not needed
					NULL,                     // Use the default provider
					0,                        // Accept the default dwFlags
					NULL);                    // pvPara is not used

	if (!_hMemCertStore)
		throw SSLException("Failed to create memory certificate store", GetLastError());

	_hCollectionCertStore = CertOpenStore(
			CERT_STORE_PROV_COLLECTION, // A collection store
			0,                          // Encoding type; not used with a collection store
			NULL,                       // Use the default provider
			0,                          // No flags
			NULL);                      // Not needed

	if (!_hCollectionCertStore)
		throw SSLException("Failed to create collection store", GetLastError());

	if (!CertAddStoreToCollection(_hCollectionCertStore, _hMemCertStore, CERT_PHYSICAL_STORE_ADD_ENABLE_FLAG, 1))
		throw SSLException("Failed to add memory certificate store to collection store", GetLastError());

	if (_options & OPT_TRUST_ROOTS_WIN_CERT_STORE)
	{
		// add root certificates
		std::wstring rootStore;
		Poco::UnicodeConverter::convert(CERT_STORE_ROOT, rootStore);
		_hRootCertStore = CertOpenSystemStoreW(0, rootStore.c_str());
		if (!_hRootCertStore)
			throw SSLException("Failed to open root certificate store", GetLastError());
		if (!CertAddStoreToCollection(_hCollectionCertStore, _hRootCertStore, CERT_PHYSICAL_STORE_ADD_ENABLE_FLAG, 1))
			throw SSLException("Failed to add root certificate store to collection store", GetLastError());
	}
}


void Context::enableExtendedCertificateVerification(bool flag)
{
	_extendedCertificateVerification = flag;
}


void Context::addTrustedCert(const Poco::Net::X509Certificate& cert)
{
	Poco::FastMutex::ScopedLock lock(_mutex);
	if (!CertAddCertificateContextToStore(_hMemCertStore, cert.system(), CERT_STORE_ADD_REPLACE_EXISTING, 0))
		throw CertificateException("Failed to add certificate to store", GetLastError());
}


Poco::Net::X509Certificate Context::certificate()
{
	if (_pCert)
		return Poco::Net::X509Certificate(_pCert, true);

	if (_certNameOrPath.empty())
		throw NoCertificateException("Certificate requested, but no certificate name or path provided");

	if (_options & OPT_LOAD_CERT_FROM_FILE)
	{
		importCertificate();
	}
	else
	{
		loadCertificate();
	}

	return Poco::Net::X509Certificate(_pCert, true);
}


void Context::loadCertificate()
{
	std::wstring wcertStore;
	Poco::UnicodeConverter::convert(_certStoreName, wcertStore);
	if (!_hCertStore)
	{
		if (_options & OPT_USE_MACHINE_STORE)
			_hCertStore = CertOpenStore(CERT_STORE_PROV_SYSTEM, 0, 0, CERT_SYSTEM_STORE_LOCAL_MACHINE, _certStoreName.c_str());
		else
			_hCertStore = CertOpenSystemStoreW(0, wcertStore.c_str());
	}
	if (!_hCertStore) throw CertificateException("Failed to open certificate store", _certStoreName, GetLastError());

	CERT_RDN_ATTR cert_rdn_attr;
	cert_rdn_attr.pszObjId = szOID_COMMON_NAME;
	cert_rdn_attr.dwValueType = CERT_RDN_ANY_TYPE;
	cert_rdn_attr.Value.cbData = (DWORD) _certNameOrPath.size();
	cert_rdn_attr.Value.pbData = (BYTE *) _certNameOrPath.c_str();

	CERT_RDN cert_rdn;
	cert_rdn.cRDNAttr = 1;
	cert_rdn.rgRDNAttr = &cert_rdn_attr;

	_pCert = CertFindCertificateInStore(_hCertStore, X509_ASN_ENCODING, 0, CERT_FIND_SUBJECT_ATTR, &cert_rdn, NULL);
	if (!_pCert) throw NoCertificateException(Poco::format("Failed to find certificate %s in store %s", _certNameOrPath, _certStoreName));
}


void Context::importCertificate()
{
	Poco::File certFile(_certNameOrPath);
	if (!certFile.exists()) throw Poco::FileNotFoundException(_certNameOrPath);
	Poco::File::FileSize size = certFile.getSize();
	Poco::Buffer<char> buffer(static_cast<std::size_t>(size));
	Poco::FileInputStream istr(_certNameOrPath);
	istr.read(buffer.begin(), buffer.size());
	if (istr.gcount() != size) throw Poco::IOException("error reading PKCS #12 certificate file");
	importCertificate(buffer.begin(), buffer.size());
}


void Context::importCertificate(const char* pBuffer, std::size_t size)
{
	std::string password;
	SSLManager::instance().PrivateKeyPassphraseRequired.notify(&SSLManager::instance(), password);
	std::wstring wpassword;
	Poco::UnicodeConverter::toUTF16(password, wpassword);

	// clear UTF-8 password
	std::fill(const_cast<char*>(password.data()), const_cast<char*>(password.data() + password.size()), 'X');

	CRYPT_DATA_BLOB blob;
	blob.cbData = static_cast<DWORD>(size);
	blob.pbData = reinterpret_cast<BYTE*>(const_cast<char*>(pBuffer));

	HCERTSTORE hTempStore =  PFXImportCertStore(&blob, wpassword.data(), PKCS12_ALLOW_OVERWRITE_KEY | PKCS12_INCLUDE_EXTENDED_PROPERTIES);

	// clear UTF-16 password
	std::fill(const_cast<wchar_t*>(wpassword.data()), const_cast<wchar_t*>(wpassword.data() + password.size()), L'X');

	if (hTempStore)
	{
		PCCERT_CONTEXT pCert = 0;
		pCert = CertEnumCertificatesInStore(hTempStore, pCert);
		while (pCert)
		{
			PCCERT_CONTEXT pStoreCert = 0;
			BOOL res = CertAddCertificateContextToStore(_hMemCertStore, pCert, CERT_STORE_ADD_REPLACE_EXISTING, &pStoreCert);
			if (res)
			{
				if (!_pCert)
				{
					_pCert = pStoreCert;
				}
				else
				{
					CertFreeCertificateContext(pStoreCert);
					pStoreCert = 0;
				}
			}
			pCert = CertEnumCertificatesInStore(hTempStore, pCert);
		}
		CertCloseStore(hTempStore, 0);
	}
	else throw CertificateException("failed to import certificate", GetLastError());
}


CredHandle& Context::credentials()
{
	Poco::FastMutex::ScopedLock lock(_mutex);

	if (_hCreds.dwLower == 0 && _hCreds.dwUpper == 0)
	{
		acquireSchannelCredentials(_hCreds);
	}
	return _hCreds;
}


void Context::acquireSchannelCredentials(CredHandle& credHandle) const
{
	SCHANNEL_CRED schannelCred;
	ZeroMemory(&schannelCred, sizeof(schannelCred));
	schannelCred.dwVersion  = SCHANNEL_CRED_VERSION;

	if (_pCert)
	{
		schannelCred.cCreds = 1; // how many cred are stored in &pCertContext
		schannelCred.paCred = &const_cast<PCCERT_CONTEXT>(_pCert);
	}

	schannelCred.grbitEnabledProtocols = proto();

	// Windows NT and Windows Me/98/95: revocation checking not supported via flags
	if (_options & Context::OPT_PERFORM_REVOCATION_CHECK)
		schannelCred.dwFlags |= SCH_CRED_REVOCATION_CHECK_CHAIN;
	else
		schannelCred.dwFlags |= SCH_CRED_IGNORE_NO_REVOCATION_CHECK | SCH_CRED_IGNORE_REVOCATION_OFFLINE;

	if (isForServerUse())
	{
		if (_mode >= Context::VERIFY_STRICT)
			schannelCred.dwFlags |= SCH_CRED_NO_SYSTEM_MAPPER;

		if (_mode == Context::VERIFY_NONE)
			schannelCred.dwFlags |= SCH_CRED_MANUAL_CRED_VALIDATION;
	}
	else
	{
		if (_mode >= Context::VERIFY_STRICT)
			schannelCred.dwFlags |= SCH_CRED_NO_DEFAULT_CREDS;
		else
			schannelCred.dwFlags |= SCH_CRED_USE_DEFAULT_CREDS;

		if (_mode == Context::VERIFY_NONE)
			schannelCred.dwFlags |= SCH_CRED_MANUAL_CRED_VALIDATION | SCH_CRED_NO_SERVERNAME_CHECK;

		if (!_extendedCertificateVerification)
			schannelCred.dwFlags |= SCH_CRED_NO_SERVERNAME_CHECK;
	}

#if defined(SCH_USE_STRONG_CRYPTO)
	if (_options & Context::OPT_USE_STRONG_CRYPTO)
		schannelCred.dwFlags |= SCH_USE_STRONG_CRYPTO;
#endif

	schannelCred.hRootStore = _hCollectionCertStore;

	TimeStamp tsExpiry;
	tsExpiry.LowPart = tsExpiry.HighPart = 0;
	SECURITY_STATUS status = _securityFunctions.AcquireCredentialsHandleW(
										NULL,
										UNISP_NAME_W,
										isForServerUse() ? SECPKG_CRED_INBOUND : SECPKG_CRED_OUTBOUND, 
										NULL,
										&schannelCred,
										NULL,
										NULL,
										&credHandle,
										&tsExpiry);

	if (status != SEC_E_OK)
	{
		throw SSLException("Failed to acquire Schannel credentials", Utility::formatError(status));
	}
}


DWORD Context::proto() const
{
	switch (_usage)
	{
	case Context::CLIENT_USE:
		return SP_PROT_SSL3_CLIENT 
			| SP_PROT_TLS1_CLIENT
#if defined(SP_PROT_TLS1_1)
			| SP_PROT_TLS1_1_CLIENT
#endif
#if defined(SP_PROT_TLS1_2)
			| SP_PROT_TLS1_2_CLIENT
#endif
			;
	case Context::SERVER_USE:
		return SP_PROT_SSL3_SERVER 
			| SP_PROT_TLS1_SERVER
#if defined(SP_PROT_TLS1_1)
			| SP_PROT_TLS1_1_SERVER
#endif
#if defined(SP_PROT_TLS1_2)
			| SP_PROT_TLS1_2_SERVER
#endif
			;
	case Context::TLSV1_CLIENT_USE:
		return SP_PROT_TLS1_CLIENT
#if defined(SP_PROT_TLS1_1)
			| SP_PROT_TLS1_1_CLIENT
#endif
#if defined(SP_PROT_TLS1_2)
			| SP_PROT_TLS1_2_CLIENT
#endif
			;
	case Context::TLSV1_SERVER_USE:
		return SP_PROT_TLS1_SERVER
#if defined(SP_PROT_TLS1_1)
			| SP_PROT_TLS1_1_SERVER
#endif
#if defined(SP_PROT_TLS1_2)
			| SP_PROT_TLS1_2_SERVER
#endif
			;
#if defined(SP_PROT_TLS1_1)
	case Context::TLSV1_1_CLIENT_USE:
		return SP_PROT_TLS1_1_CLIENT
#if defined(SP_PROT_TLS1_2)
			| SP_PROT_TLS1_2_CLIENT
#endif
			;
	case Context::TLSV1_1_SERVER_USE:
		return SP_PROT_TLS1_1_SERVER
#if defined(SP_PROT_TLS1_2)
			| SP_PROT_TLS1_2_SERVER
#endif
			;
#endif
#if defined(SP_PROT_TLS1_2)
	case Context::TLSV1_2_CLIENT_USE:
		return SP_PROT_TLS1_2_CLIENT;
	case Context::TLSV1_2_SERVER_USE:
		return SP_PROT_TLS1_2_SERVER;
#endif
	default:
		throw Poco::InvalidArgumentException("Unsupported SSL/TLS protocol version");
	}
}

} } // namespace Poco::Net
