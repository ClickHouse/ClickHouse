#include <resolv.h>

int dn_skipname(const unsigned char *s, const unsigned char *end)
{
	const unsigned char *p = s;
	while (p < end)
		if (!*p) return p-s+1;
		else if (*p>=192)
			if (p+1<end) return p-s+2;
			else break;
		else
			if (end-p<*p+1) break;
			else p += *p + 1;
	return -1;
}
