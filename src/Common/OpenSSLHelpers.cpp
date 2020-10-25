#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_SSL
#include "OpenSSLHelpers.h"
#include <ext/scope_guard.h>
#include <openssl/err.h>
#include <openssl/sha.h>

namespace DB
{
#pragma GCC diagnostic warning "-Wold-style-cast"

void encodeSHA256(const std::string_view & text, unsigned char * out)
{
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, reinterpret_cast<const UInt8 *>(text.data()), text.size());
    SHA256_Final(out, &ctx);
}

String getOpenSSLErrors()
{
    BIO * mem = BIO_new(BIO_s_mem());
    SCOPE_EXIT(BIO_free(mem));
    ERR_print_errors(mem);
    char * buf = nullptr;
    size_t size = BIO_get_mem_data(mem, &buf);
    return String(buf, size);
}

}
#endif
