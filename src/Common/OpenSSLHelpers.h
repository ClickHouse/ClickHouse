#pragma once

#include <Common/config.h>

#if USE_SSL
#    include <base/types.h>


namespace DB
{

/// Encodes `text` and returns it.
std::string encodeSHA256(std::string_view text);
std::string encodeSHA256(const void * text, size_t size);
/// `out` must be at least 32 bytes long.
void encodeSHA256(std::string_view text, unsigned char * out);
void encodeSHA256(const void * text, size_t size, unsigned char * out);

/// Returns concatenation of error strings for all errors that OpenSSL has recorded, emptying the error queue.
String getOpenSSLErrors();

}
#endif
