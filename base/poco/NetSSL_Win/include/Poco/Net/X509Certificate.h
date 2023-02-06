//
// X509Certificate.h
//
// Library: NetSSL_Win
// Package: Certificate
// Module:  X509Certificate
//
// Definition of the X509Certificate class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_X509Certificate_INCLUDED
#define NetSSL_X509Certificate_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/DateTime.h"
#include <set>
#include <istream>
#include <wincrypt.h>


namespace Poco {
namespace Net {


class NetSSL_Win_API X509Certificate
	/// This class represents a X509 Certificate.
{
public:
	enum NID
		/// Name identifier for extracting information from
		/// a certificate subject's or issuer's distinguished name.
	{
		NID_COMMON_NAME,
		NID_COUNTRY,
		NID_LOCALITY_NAME,
		NID_STATE_OR_PROVINCE,
		NID_ORGANIZATION_NAME,
		NID_ORGANIZATION_UNIT_NAME	
	};
	
	explicit X509Certificate(const std::string& certPath);
		/// Creates the X509Certificate object by reading
		/// a certificate in PEM or DER format from a file.

	explicit X509Certificate(std::istream& istr);
		/// Creates the X509Certificate object by reading
		/// a certificate in PEM or DER format from a stream.

	X509Certificate(const std::string& certName, const std::string& certStoreName, bool useMachineStore = false);
		/// Creates the X509Certificate object by loading
		/// a certificate from the specified certificate store.
		///
		/// If useSystemStore is true, the machine's certificate store is used,
		/// otherwise the user's certificate store.

	explicit X509Certificate(PCCERT_CONTEXT pCert);
		/// Creates the X509Certificate from an existing
		/// WinCrypt certificate. Ownership is taken of 
		/// the certificate.

	X509Certificate(PCCERT_CONTEXT pCert, bool shared);
		/// Creates the X509Certificate from an existing
		/// WinCrypt certificate. Ownership is taken of 
		/// the certificate. If shared is true, the 
		/// certificate's reference count is incremented.

	X509Certificate(const X509Certificate& cert);
		/// Creates the certificate by copying another one.

	X509Certificate& operator = (const X509Certificate& cert);
		/// Assigns a certificate.
 
	void swap(X509Certificate& cert);
		/// Exchanges the certificate with another one.

	~X509Certificate();
		/// Destroys the X509Certificate.

	const std::string& issuerName() const;
		/// Returns the certificate issuer's distinguished name. 
		
	std::string issuerName(NID nid) const;
		/// Extracts the information specified by the given
		/// NID (name identifier) from the certificate issuer's
		/// distinguished name.
		
	const std::string& subjectName() const;
		/// Returns the certificate subject's distinguished name.

	std::string subjectName(NID nid) const;
		/// Extracts the information specified by the given
		/// NID (name identifier) from the certificate subject's
		/// distinguished name.
		
	std::string commonName() const;
		/// Returns the common name stored in the certificate
		/// subject's distinguished name.
		
	void extractNames(std::string& commonName, std::set<std::string>& domainNames) const;
		/// Extracts the common name and the alias domain names from the
		/// certificate.
		
	Poco::DateTime validFrom() const;
		/// Returns the date and time the certificate is valid from.
		
	Poco::DateTime expiresOn() const;
		/// Returns the date and time the certificate expires.
		
	bool issuedBy(const X509Certificate& issuerCertificate) const;
		/// Checks whether the certificate has been issued by
		/// the issuer given by issuerCertificate. This can be
		/// used to validate a certificate chain.
		///
		/// Verifies that the given certificate is contained in the
		/// certificate's issuer certificate chain.
		///
		/// Returns true if verification against the issuer certificate
		/// was successful, false otherwise.

	bool verify(const std::string& hostName) const;
		/// Verifies the validity of the certificate against the host name.
		///
		/// For this check to be successful, the certificate must contain
		/// a domain name that matches the domain name
		/// of the host.
		/// 
		/// Returns true if verification succeeded, or false otherwise.
		
	static bool verify(const Poco::Net::X509Certificate& cert, const std::string& hostName);
		/// Verifies the validity of the certificate against the host name.
		///
		/// For this check to be successful, the certificate must contain
		/// a domain name that matches the domain name
		/// of the host.
		///
		/// Returns true if verification succeeded, or false otherwise.

	const PCCERT_CONTEXT system() const;
		/// Returns the underlying WinCrypt certificate.

protected:
	void init();
		/// Extracts issuer and subject name from the certificate.
	
	static void* nid2oid(NID nid);
		/// Returns the OID for the given NID.

	void loadCertificate(const std::string& certName, const std::string& certStoreName, bool useMachineStore);
	void importCertificate(const std::string& certPath);
	void importCertificate(std::istream& istr);
	void importCertificate(const char* pBuffer, std::size_t size);
	void importPEMCertificate(const char* pBuffer, std::size_t size);
	void importDERCertificate(const char* pBuffer, std::size_t size);

	static bool containsWildcards(const std::string& commonName);
	static bool matchWildcard(const std::string& alias, const std::string& hostName);

private:
	std::string _issuerName;
	std::string _subjectName;
	PCCERT_CONTEXT _pCert;
};


//
// inlines
//
inline const std::string& X509Certificate::issuerName() const
{
	return _issuerName;
}


inline const std::string& X509Certificate::subjectName() const
{
	return _subjectName;
}


inline const PCCERT_CONTEXT X509Certificate::system() const
{
	return _pCert;
}


} } // namespace Poco::Net


#endif // NetSSL_X509Certificate_INCLUDED
