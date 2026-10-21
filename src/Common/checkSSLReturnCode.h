#pragma once
#include <sys/types.h>

namespace DB
{

/// Check if ret is ERR_SSL_WANT_READ.
bool checkSSLWantRead(ssize_t ret);

/// CHeck if ret is ERR_SSL_WANT_WRITE.
bool checkSSLWantWrite(ssize_t ret);

}
