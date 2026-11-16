//
// DistrustedCertificates.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  DistrustedCertificates
//
// Copyright (c) 2026, ClickHouse, Inc.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_DistrustedCertificates_INCLUDED
#define NetSSL_DistrustedCertificates_INCLUDED


#include <openssl/x509.h>
#include "Poco/Net/NetSSL.h"


namespace Poco
{
namespace Net
{


    NetSSL_API int checkDistrustedCertificate(X509 * certificate);
    /// Checks the certificate against the list of distrusted public keys built into
    /// the binary, and returns `X509_V_OK` if it is not on the list, `X509_V_ERR_CERT_REVOKED`
    /// if it is, or another `X509_V_ERR_*` code if the check itself failed.
    ///
    /// The list contains the keys of CA certificates that must not be trusted even if
    /// the trust store of the operating system still contains them, e.g. because their
    /// private keys are compromised. A key is identified by the SHA-256 of its DER-encoded
    /// `SubjectPublicKeyInfo`, so that every certificate of the same key is rejected,
    /// including cross-signed ones.


}
} // namespace Poco::Net


#endif // NetSSL_DistrustedCertificates_INCLUDED
