#include <netinet/in.h>
#include <byteswap.h>

uint32_t ntohl(uint32_t n)
{
	union { int i; char c; } u = { 1 };
	return u.c ? bswap_32(n) : n;
}
