#pragma once

#include "config.h"

#include <Common/Exception.h>

#if USE_SSL
#include <base/types.h>

#include <openssl/evp.h>

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
std::vector<uint8_t> pbkdf2SHA256(std::string_view password, const std::vector<uint8_t>& salt, int iterations);

/// FIXME
std::string calculateHMACwithSHA256(std::string to_sign, EVP_PKEY * pkey);

/// Generate Certificate Signing Request with given `subject(s)` and private key.
std::string generateCSR(std::vector<std::string>, std::string);

/// Returns concatenation of error strings for all errors that OpenSSL has recorded, emptying the error queue.
String getOpenSSLErrors();

}
#endif
