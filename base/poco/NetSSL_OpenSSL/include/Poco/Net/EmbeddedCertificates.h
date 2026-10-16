//
// EmbeddedCertificates.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  EmbeddedCertificates
//
// CA certificates embedded into the binary at build time.
//
// Copyright (c) 2026, ClickHouse, Inc.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_EmbeddedCertificates_INCLUDED
#define NetSSL_EmbeddedCertificates_INCLUDED


#include <string_view>
#include "Poco/Net/NetSSL.h"


namespace Poco
{
namespace Net
{


    NetSSL_API std::string_view embeddedCACertificates();
    /// Returns the PEM bundle of CA certificates embedded into the binary
    /// at build time, or an empty view if the build does not contain one.
    /// It is used as a fallback when no CA certificates can be found on the
    /// filesystem, e.g. when running in a container built "from scratch".


}
} // namespace Poco::Net


#endif // NetSSL_EmbeddedCertificates_INCLUDED
