/*
 * Public domain
 * netinet/in.h compatibility shim
 */

#ifndef _WIN32
#include_next <netinet/in.h>
#else
#include <win32netcompat.h>
#endif
