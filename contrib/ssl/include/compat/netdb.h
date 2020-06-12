/*
 * Public domain
 * netdb.h compatibility shim
 */

#ifndef _WIN32
#include_next <netdb.h>
#else
#include <win32netcompat.h>
#endif
