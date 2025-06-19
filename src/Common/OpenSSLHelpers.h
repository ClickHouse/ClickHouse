#pragma once

#include "config.h"

#include <Common/Exception.h>

#if USE_SSL
#    include <base/types.h>


namespace DB
{

/// Encodes `text` and returns it.
std::string encodeSHA256(std::string_view text);
std::string encodeSHA256(const void * text, size_t size);
std::vector<uint8_t> encodeSHA256(const std::vector<uint8_t> & data);

/// `out` must be at least 32 bytes long.
void encodeSHA256(std::string_view text, unsigned char * out);
void encodeSHA256(const void * text, size_t size, unsigned char * out);

std::vector<uint8_t> hmacSHA256(const std::vector<uint8_t> & key, const std::string & data);

/// Returns concatenation of error strings for all errors that OpenSSL has recorded, emptying the error queue.
String getOpenSSLErrors();

}
#endif
