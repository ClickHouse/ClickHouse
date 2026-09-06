//
// EmbeddedCertificates.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  EmbeddedCertificates
//
// Copyright (c) 2026, ClickHouse, Inc.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/EmbeddedCertificates.h"


#if defined(POCO_EMBEDDED_CA_CERTIFICATES)
namespace
{
	/// The root store maintained in the gRPC submodule (the Chromium root store in PEM format).
	constexpr unsigned char ca_certificates[] = {
#embed "../../../../contrib/grpc/etc/roots.pem"
	};
}
#endif


namespace Poco {
namespace Net {


std::string_view embeddedCACertificates()
{
#if defined(POCO_EMBEDDED_CA_CERTIFICATES)
	return {reinterpret_cast<const char *>(ca_certificates), sizeof(ca_certificates)};
#else
	return {};
#endif
}


} } // namespace Poco::Net
