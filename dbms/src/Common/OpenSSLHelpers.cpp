#include <Common/config.h>
#if USE_SSL
#include "OpenSSLHelpers.h"
#include <ext/scope_guard.h>
#include <openssl/err.h>

namespace DB
{

String getOpenSSLErrors()
{
    BIO * mem = BIO_new(BIO_s_mem());
    SCOPE_EXIT(BIO_free(mem));
    ERR_print_errors(mem);
    char * buf = nullptr;
    long size = BIO_get_mem_data(mem, &buf);
    return String(buf, size);
}

}
#endif
