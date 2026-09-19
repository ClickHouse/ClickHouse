#pragma once

#include "config.h"

#if USE_SILK

#include <SocketIO.h>

#include <base/types.h>

#include <optional>

#if USE_SSL
#include <Poco/Net/Context.h>
#endif

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB::Proxy
{

#if USE_SSL

/// Build a server-side TLS context from the `openSSL.server.*` config section (certificate,
/// key, verification mode, ciphers, protocols). Registers the context with CertificateReloader
/// so certificates hot-reload and, when the `acme` section is present, come from the ACME provider.
Poco::Net::Context::Ptr makeServerTLSContext(const Poco::Util::AbstractConfiguration & config);

/// Build a client-side TLS context from the `openSSL.client.*` config section, used for the
/// proxy-to-backend leg (re-encrypt mode).
Poco::Net::Context::Ptr makeClientTLSContext(const Poco::Util::AbstractConfiguration & config);

#endif

/// Read the leading TLS record from the connection (recording it for forwarding) and extract the
/// server name from the ClientHello SNI extension. Returns nothing if the bytes are not a TLS
/// ClientHello or carry no SNI. Never decrypts anything.
std::optional<String> peekTLSClientHelloSNI(RecordingReader & reader);

}

#endif
