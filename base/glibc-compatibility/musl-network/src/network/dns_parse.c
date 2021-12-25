#include <string.h>
#include "lookup.h"

int __dns_parse(const unsigned char *r, int rlen, int (*callback)(void *, int, const void *, int, const void *), void *ctx)
{
	int qdcount, ancount;
	const unsigned char *p;
	int len;

	if (rlen<12) return -1;
	if ((r[3]&15)) return 0;
	p = r+12;
	qdcount = r[4]*256 + r[5];
	ancount = r[6]*256 + r[7];
	if (qdcount+ancount > 64) return -1;
	while (qdcount--) {
		while (p-r < rlen && *p-1U < 127) p++;
		if (*p>193 || (*p==193 && p[1]>254) || p>r+rlen-6)
			return -1;
		p += 5 + !!*p;
	}
	while (ancount--) {
		while (p-r < rlen && *p-1U < 127) p++;
		if (*p>193 || (*p==193 && p[1]>254) || p>r+rlen-6)
			return -1;
		p += 1 + !!*p;
		len = p[8]*256 + p[9];
		if (p+len > r+rlen) return -1;
		if (callback(ctx, p[1], p+10, len, r) < 0) return -1;
		p += 10 + len;
	}
	return 0;
}
