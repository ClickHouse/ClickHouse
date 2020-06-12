/*
 * Public domain
 * netinet/tcp.h compatibility shim
 */

#ifndef _WIN32
#include_next <netinet/tcp.h>
#else
#include <win32netcompat.h>
#endif
