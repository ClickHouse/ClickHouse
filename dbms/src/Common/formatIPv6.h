#pragma once

#include <common/Types.h>

#define IPV4_BINARY_LENGTH 4
#define IPV6_BINARY_LENGTH 16
#define IPV4_MAX_TEXT_LENGTH 15     /// Does not count tail zero byte.
#define IPV6_MAX_TEXT_LENGTH 39


namespace DB
{


/** Rewritten inet_ntop6 from http://svn.apache.org/repos/asf/apr/apr/trunk/network_io/unix/inet_pton.c
  *  performs significantly faster than the reference implementation due to the absence of sprintf calls,
  *  bounds checking, unnecessary string copying and length calculation.
  */
void formatIPv6(const unsigned char * src, char *& dst, UInt8 zeroed_tail_bytes_count = 0);

}
