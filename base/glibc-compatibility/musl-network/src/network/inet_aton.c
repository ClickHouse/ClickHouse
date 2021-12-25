#include <ctype.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>

int __inet_aton(const char *s0, struct in_addr *dest)
{
	const char *s = s0;
	unsigned char *d = (void *)dest;
	unsigned long a[4] = { 0 };
	char *z;
	int i;

	for (i=0; i<4; i++) {
		a[i] = strtoul(s, &z, 0);
		if (z==s || (*z && *z != '.') || !isdigit(*s))
			return 0;
		if (!*z) break;
		s=z+1;
	}
	if (i==4) return 0;
	switch (i) {
	case 0:
		a[1] = a[0] & 0xffffff;
		a[0] >>= 24;
	case 1:
		a[2] = a[1] & 0xffff;
		a[1] >>= 16;
	case 2:
		a[3] = a[2] & 0xff;
		a[2] >>= 8;
	}
	for (i=0; i<4; i++) {
		if (a[i] > 255) return 0;
		d[i] = a[i];
	}
	return 1;
}

weak_alias(__inet_aton, inet_aton);
