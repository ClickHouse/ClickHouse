/* ClickHouse interposes `getaddrinfo` to apply `remote_url_allow_hosts` to every name resolution of the
 * server (src/Common/NameResolutionFilter.cpp). musl is linked statically, so its implementation is compiled
 * as `__musl_getaddrinfo` (see CMakeLists.txt), and this weak definition is the `getaddrinfo` of programs
 * that do not link the interposer. */

#include <netdb.h>

int __musl_getaddrinfo(const char *restrict host, const char *restrict serv, const struct addrinfo *restrict hint, struct addrinfo **restrict res);

__attribute__((__weak__)) int getaddrinfo(const char *restrict host, const char *restrict serv, const struct addrinfo *restrict hint, struct addrinfo **restrict res)
{
    return __musl_getaddrinfo(host, serv, hint, res);
}
