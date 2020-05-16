#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_SSL
#    include <Core/Types.h>


namespace DB
{
/// Encodes `text` and puts the result to `out` which must be at least 32 bytes long.
void encodeSHA256(const std::string_view & text, unsigned char * out);

/// Returns concatenation of error strings for all errors that OpenSSL has recorded, emptying the error queue.
String getOpenSSLErrors();

}
#endif
