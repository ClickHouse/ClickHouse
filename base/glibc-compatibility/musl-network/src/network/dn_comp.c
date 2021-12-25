#include <string.h>
#include <resolv.h>

/* RFC 1035 message compression */

/* label start offsets of a compressed domain name s */
static int getoffs(short *offs, const unsigned char *base, const unsigned char *s)
{
	int i=0;
	for (;;) {
		while (*s & 0xc0) {
			if ((*s & 0xc0) != 0xc0) return 0;
			s = base + ((s[0]&0x3f)<<8 | s[1]);
		}
		if (!*s) return i;
		if (s-base >= 0x4000) return 0;
		offs[i++] = s-base;
		s += *s + 1;
	}
}

/* label lengths of an ascii domain name s */
static int getlens(unsigned char *lens, const char *s, int l)
{
	int i=0,j=0,k=0;
	for (;;) {
		for (; j<l && s[j]!='.'; j++);
		if (j-k-1u > 62) return 0;
		lens[i++] = j-k;
		if (j==l) return i;
		k = ++j;
	}
}

/* longest suffix match of an ascii domain with a compressed domain name dn */
static int match(int *offset, const unsigned char *base, const unsigned char *dn,
	const char *end, const unsigned char *lens, int nlen)
{
	int l, o, m=0;
	short offs[128];
	int noff = getoffs(offs, base, dn);
	if (!noff) return 0;
	for (;;) {
		l = lens[--nlen];
		o = offs[--noff];
		end -= l;
		if (l != base[o] || memcmp(base+o+1, end, l))
			return m;
		*offset = o;
		m += l;
		if (nlen) m++;
		if (!nlen || !noff) return m;
		end--;
	}
}

int dn_comp(const char *src, unsigned char *dst, int space, unsigned char **dnptrs, unsigned char **lastdnptr)
{
	int i, j, n, m=0, offset, bestlen=0, bestoff;
	unsigned char lens[127];
	unsigned char **p;
	const char *end;
	size_t l = strnlen(src, 255);
	if (l && src[l-1] == '.') l--;
	if (l>253 || space<=0) return -1;
	if (!l) {
		*dst = 0;
		return 1;
	}
	end = src+l;
	n = getlens(lens, src, l);
	if (!n) return -1;

	p = dnptrs;
	if (p && *p) for (p++; *p; p++) {
		m = match(&offset, *dnptrs, *p, end, lens, n);
		if (m > bestlen) {
			bestlen = m;
			bestoff = offset;
			if (m == l)
				break;
		}
	}

	/* encode unmatched part */
	if (space < l-bestlen+2+(bestlen-1 < l-1)) return -1;
	memcpy(dst+1, src, l-bestlen);
	for (i=j=0; i<l-bestlen; i+=lens[j++]+1)
		dst[i] = lens[j];

	/* add tail */
	if (bestlen) {
		dst[i++] = 0xc0 | bestoff>>8;
		dst[i++] = bestoff;
	} else
		dst[i++] = 0;

	/* save dst pointer */
	if (i>2 && lastdnptr && dnptrs && *dnptrs) {
		while (*p) p++;
		if (p+1 < lastdnptr) {
			*p++ = dst;
			*p=0;
		}
	}
	return i;
}
