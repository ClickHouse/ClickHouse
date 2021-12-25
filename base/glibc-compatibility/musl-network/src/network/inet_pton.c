#include <sys/socket.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>

static int hexval(unsigned c)
{
	if (c-'0'<10) return c-'0';
	c |= 32;
	if (c-'a'<6) return c-'a'+10;
	return -1;
}

int inet_pton(int af, const char *restrict s, void *restrict a0)
{
	uint16_t ip[8];
	unsigned char *a = a0;
	int i, j, v, d, brk=-1, need_v4=0;

	if (af==AF_INET) {
		for (i=0; i<4; i++) {
			for (v=j=0; j<3 && isdigit(s[j]); j++)
				v = 10*v + s[j]-'0';
			if (j==0 || (j>1 && s[0]=='0') || v>255) return 0;
			a[i] = v;
			if (s[j]==0 && i==3) return 1;
			if (s[j]!='.') return 0;
			s += j+1;
		}
		return 0;
	} else if (af!=AF_INET6) {
		errno = EAFNOSUPPORT;
		return -1;
	}

	if (*s==':' && *++s!=':') return 0;

	for (i=0; ; i++) {
		if (s[0]==':' && brk<0) {
			brk=i;
			ip[i&7]=0;
			if (!*++s) break;
			if (i==7) return 0;
			continue;
		}
		for (v=j=0; j<4 && (d=hexval(s[j]))>=0; j++)
			v=16*v+d;
		if (j==0) return 0;
		ip[i&7] = v;
		if (!s[j] && (brk>=0 || i==7)) break;
		if (i==7) return 0;
		if (s[j]!=':') {
			if (s[j]!='.' || (i<6 && brk<0)) return 0;
			need_v4=1;
			i++;
			break;
		}
		s += j+1;
	}
	if (brk>=0) {
		memmove(ip+brk+7-i, ip+brk, 2*(i+1-brk));
		for (j=0; j<7-i; j++) ip[brk+j] = 0;
	}
	for (j=0; j<8; j++) {
		*a++ = ip[j]>>8;
		*a++ = ip[j];
	}
	if (need_v4 && inet_pton(AF_INET, (void *)s, a-4) <= 0) return 0;
	return 1;
}
