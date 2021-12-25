#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <resolv.h>
#include "lookup.h"
#include "stdio_impl.h"
#include "syscall.h"

static int is_valid_hostname(const char *host)
{
	const unsigned char *s;
	if (strnlen(host, 255)-1 >= 254 || mbstowcs(0, host, 0) == -1) return 0;
	for (s=(void *)host; *s>=0x80 || *s=='.' || *s=='-' || isalnum(*s); s++);
	return !*s;
}

static int name_from_null(struct address buf[static 2], const char *name, int family, int flags)
{
	int cnt = 0;
	if (name) return 0;
	if (flags & AI_PASSIVE) {
		if (family != AF_INET6)
			buf[cnt++] = (struct address){ .family = AF_INET };
		if (family != AF_INET)
			buf[cnt++] = (struct address){ .family = AF_INET6 };
	} else {
		if (family != AF_INET6)
			buf[cnt++] = (struct address){ .family = AF_INET, .addr = { 127,0,0,1 } };
		if (family != AF_INET)
			buf[cnt++] = (struct address){ .family = AF_INET6, .addr = { [15] = 1 } };
	}
	return cnt;
}

static int name_from_numeric(struct address buf[static 1], const char *name, int family)
{
	return __lookup_ipliteral(buf, name, family);
}

static int name_from_hosts(struct address buf[static MAXADDRS], char canon[static 256], const char *name, int family)
{
	char line[512];
	size_t l = strlen(name);
	int cnt = 0, badfam = 0, have_canon = 0;
	unsigned char _buf[1032];
	FILE _f, *f = __fopen_rb_ca("/etc/hosts", &_f, _buf, sizeof _buf);
	if (!f) switch (errno) {
	case ENOENT:
	case ENOTDIR:
	case EACCES:
		return 0;
	default:
		return EAI_SYSTEM;
	}
	while (fgets(line, sizeof line, f) && cnt < MAXADDRS) {
		char *p, *z;

		if ((p=strchr(line, '#'))) *p++='\n', *p=0;
		for(p=line+1; (p=strstr(p, name)) &&
			(!isspace(p[-1]) || !isspace(p[l])); p++);
		if (!p) continue;

		/* Isolate IP address to parse */
		for (p=line; *p && !isspace(*p); p++);
		*p++ = 0;
		switch (name_from_numeric(buf+cnt, line, family)) {
		case 1:
			cnt++;
			break;
		case 0:
			continue;
		default:
			badfam = EAI_NONAME;
			break;
		}

		if (have_canon) continue;

		/* Extract first name as canonical name */
		for (; *p && isspace(*p); p++);
		for (z=p; *z && !isspace(*z); z++);
		*z = 0;
		if (is_valid_hostname(p)) {
			have_canon = 1;
			memcpy(canon, p, z-p+1);
		}
	}
	__fclose_ca(f);
	return cnt ? cnt : badfam;
}

struct dpc_ctx {
	struct address *addrs;
	char *canon;
	int cnt;
};

#define RR_A 1
#define RR_CNAME 5
#define RR_AAAA 28

static int dns_parse_callback(void *c, int rr, const void *data, int len, const void *packet)
{
	char tmp[256];
	struct dpc_ctx *ctx = c;
	if (ctx->cnt >= MAXADDRS) return -1;
	switch (rr) {
	case RR_A:
		if (len != 4) return -1;
		ctx->addrs[ctx->cnt].family = AF_INET;
		ctx->addrs[ctx->cnt].scopeid = 0;
		memcpy(ctx->addrs[ctx->cnt++].addr, data, 4);
		break;
	case RR_AAAA:
		if (len != 16) return -1;
		ctx->addrs[ctx->cnt].family = AF_INET6;
		ctx->addrs[ctx->cnt].scopeid = 0;
		memcpy(ctx->addrs[ctx->cnt++].addr, data, 16);
		break;
	case RR_CNAME:
		if (__dn_expand(packet, (const unsigned char *)packet + 512,
		    data, tmp, sizeof tmp) > 0 && is_valid_hostname(tmp))
			strcpy(ctx->canon, tmp);
		break;
	}
	return 0;
}

static int name_from_dns(struct address buf[static MAXADDRS], char canon[static 256], const char *name, int family, const struct resolvconf *conf)
{
	unsigned char qbuf[2][280], abuf[2][512];
	const unsigned char *qp[2] = { qbuf[0], qbuf[1] };
	unsigned char *ap[2] = { abuf[0], abuf[1] };
	int qlens[2], alens[2];
	int i, nq = 0;
	struct dpc_ctx ctx = { .addrs = buf, .canon = canon };
	static const struct { int af; int rr; } afrr[2] = {
		{ .af = AF_INET6, .rr = RR_A },
		{ .af = AF_INET, .rr = RR_AAAA },
	};

	for (i=0; i<2; i++) {
		if (family != afrr[i].af) {
			qlens[nq] = __res_mkquery(0, name, 1, afrr[i].rr,
				0, 0, 0, qbuf[nq], sizeof *qbuf);
			if (qlens[nq] == -1)
				return EAI_NONAME;
			qbuf[nq][3] = 0; /* don't need AD flag */
			nq++;
		}
	}

	if (__res_msend_rc(nq, qp, qlens, ap, alens, sizeof *abuf, conf) < 0)
		return EAI_SYSTEM;

	for (i=0; i<nq; i++) {
		if (alens[i] < 4 || (abuf[i][3] & 15) == 2) return EAI_AGAIN;
		if ((abuf[i][3] & 15) == 3) return 0;
		if ((abuf[i][3] & 15) != 0) return EAI_FAIL;
	}

	for (i=0; i<nq; i++)
		__dns_parse(abuf[i], alens[i], dns_parse_callback, &ctx);

	if (ctx.cnt) return ctx.cnt;
	return EAI_NONAME;
}

static int name_from_dns_search(struct address buf[static MAXADDRS], char canon[static 256], const char *name, int family)
{
	char search[256];
	struct resolvconf conf;
	size_t l, dots;
	char *p, *z;

	if (__get_resolv_conf(&conf, search, sizeof search) < 0) return -1;

	/* Count dots, suppress search when >=ndots or name ends in
	 * a dot, which is an explicit request for global scope. */
	for (dots=l=0; name[l]; l++) if (name[l]=='.') dots++;
	if (dots >= conf.ndots || name[l-1]=='.') *search = 0;

	/* Strip final dot for canon, fail if multiple trailing dots. */
	if (name[l-1]=='.') l--;
	if (!l || name[l-1]=='.') return EAI_NONAME;

	/* This can never happen; the caller already checked length. */
	if (l >= 256) return EAI_NONAME;

	/* Name with search domain appended is setup in canon[]. This both
	 * provides the desired default canonical name (if the requested
	 * name is not a CNAME record) and serves as a buffer for passing
	 * the full requested name to name_from_dns. */
	memcpy(canon, name, l);
	canon[l] = '.';

	for (p=search; *p; p=z) {
		for (; isspace(*p); p++);
		for (z=p; *z && !isspace(*z); z++);
		if (z==p) break;
		if (z-p < 256 - l - 1) {
			memcpy(canon+l+1, p, z-p);
			canon[z-p+1+l] = 0;
			int cnt = name_from_dns(buf, canon, canon, family, &conf);
			if (cnt) return cnt;
		}
	}

	canon[l] = 0;
	return name_from_dns(buf, canon, name, family, &conf);
}

static const struct policy {
	unsigned char addr[16];
	unsigned char len, mask;
	unsigned char prec, label;
} defpolicy[] = {
	{ "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1", 15, 0xff, 50, 0 },
	{ "\0\0\0\0\0\0\0\0\0\0\xff\xff", 11, 0xff, 35, 4 },
	{ "\x20\2", 1, 0xff, 30, 2 },
	{ "\x20\1", 3, 0xff, 5, 5 },
	{ "\xfc", 0, 0xfe, 3, 13 },
#if 0
	/* These are deprecated and/or returned to the address
	 * pool, so despite the RFC, treating them as special
	 * is probably wrong. */
	{ "", 11, 0xff, 1, 3 },
	{ "\xfe\xc0", 1, 0xc0, 1, 11 },
	{ "\x3f\xfe", 1, 0xff, 1, 12 },
#endif
	/* Last rule must match all addresses to stop loop. */
	{ "", 0, 0, 40, 1 },
};

static const struct policy *policyof(const struct in6_addr *a)
{
	int i;
	for (i=0; ; i++) {
		if (memcmp(a->s6_addr, defpolicy[i].addr, defpolicy[i].len))
			continue;
		if ((a->s6_addr[defpolicy[i].len] & defpolicy[i].mask)
		    != defpolicy[i].addr[defpolicy[i].len])
			continue;
		return defpolicy+i;
	}
}

static int labelof(const struct in6_addr *a)
{
	return policyof(a)->label;
}

static int scopeof(const struct in6_addr *a)
{
	if (IN6_IS_ADDR_MULTICAST(a)) return a->s6_addr[1] & 15;
	if (IN6_IS_ADDR_LINKLOCAL(a)) return 2;
	if (IN6_IS_ADDR_LOOPBACK(a)) return 2;
	if (IN6_IS_ADDR_SITELOCAL(a)) return 5;
	return 14;
}

static int prefixmatch(const struct in6_addr *s, const struct in6_addr *d)
{
	/* FIXME: The common prefix length should be limited to no greater
	 * than the nominal length of the prefix portion of the source
	 * address. However the definition of the source prefix length is
	 * not clear and thus this limiting is not yet implemented. */
	unsigned i;
	for (i=0; i<128 && !((s->s6_addr[i/8]^d->s6_addr[i/8])&(128>>(i%8))); i++);
	return i;
}

#define DAS_USABLE              0x40000000
#define DAS_MATCHINGSCOPE       0x20000000
#define DAS_MATCHINGLABEL       0x10000000
#define DAS_PREC_SHIFT          20
#define DAS_SCOPE_SHIFT         16
#define DAS_PREFIX_SHIFT        8
#define DAS_ORDER_SHIFT         0

static int addrcmp(const void *_a, const void *_b)
{
	const struct address *a = _a, *b = _b;
	return b->sortkey - a->sortkey;
}

int __lookup_name(struct address buf[static MAXADDRS], char canon[static 256], const char *name, int family, int flags)
{
	int cnt = 0, i, j;

	*canon = 0;
	if (name) {
		/* reject empty name and check len so it fits into temp bufs */
		size_t l = strnlen(name, 255);
		if (l-1 >= 254)
			return EAI_NONAME;
		memcpy(canon, name, l+1);
	}

	/* Procedurally, a request for v6 addresses with the v4-mapped
	 * flag set is like a request for unspecified family, followed
	 * by filtering of the results. */
	if (flags & AI_V4MAPPED) {
		if (family == AF_INET6) family = AF_UNSPEC;
		else flags -= AI_V4MAPPED;
	}

	/* Try each backend until there's at least one result. */
	cnt = name_from_null(buf, name, family, flags);
	if (!cnt) cnt = name_from_numeric(buf, name, family);
	if (!cnt && !(flags & AI_NUMERICHOST)) {
		cnt = name_from_hosts(buf, canon, name, family);
		if (!cnt) cnt = name_from_dns_search(buf, canon, name, family);
	}
	if (cnt<=0) return cnt ? cnt : EAI_NONAME;

	/* Filter/transform results for v4-mapped lookup, if requested. */
	if (flags & AI_V4MAPPED) {
		if (!(flags & AI_ALL)) {
			/* If any v6 results exist, remove v4 results. */
			for (i=0; i<cnt && buf[i].family != AF_INET6; i++);
			if (i<cnt) {
				for (j=0; i<cnt; i++) {
					if (buf[i].family == AF_INET6)
						buf[j++] = buf[i];
				}
				cnt = i = j;
			}
		}
		/* Translate any remaining v4 results to v6 */
		for (i=0; i<cnt; i++) {
			if (buf[i].family != AF_INET) continue;
			memcpy(buf[i].addr+12, buf[i].addr, 4);
			memcpy(buf[i].addr, "\0\0\0\0\0\0\0\0\0\0\xff\xff", 12);
			buf[i].family = AF_INET6;
		}
	}

	/* No further processing is needed if there are fewer than 2
	 * results or if there are only IPv4 results. */
	if (cnt<2 || family==AF_INET) return cnt;
	for (i=0; i<cnt; i++) if (buf[i].family != AF_INET) break;
	if (i==cnt) return cnt;

	int cs;
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cs);

	/* The following implements a subset of RFC 3484/6724 destination
	 * address selection by generating a single 31-bit sort key for
	 * each address. Rules 3, 4, and 7 are omitted for having
	 * excessive runtime and code size cost and dubious benefit.
	 * So far the label/precedence table cannot be customized. */
	for (i=0; i<cnt; i++) {
		int family = buf[i].family;
		int key = 0;
		struct sockaddr_in6 sa6 = { 0 }, da6 = {
			.sin6_family = AF_INET6,
			.sin6_scope_id = buf[i].scopeid,
			.sin6_port = 65535
		};
		struct sockaddr_in sa4 = { 0 }, da4 = {
			.sin_family = AF_INET,
			.sin_port = 65535
		};
		void *sa, *da;
		socklen_t salen, dalen;
		if (family == AF_INET6) {
			memcpy(da6.sin6_addr.s6_addr, buf[i].addr, 16);
			da = &da6; dalen = sizeof da6;
			sa = &sa6; salen = sizeof sa6;
		} else {
			memcpy(sa6.sin6_addr.s6_addr,
				"\0\0\0\0\0\0\0\0\0\0\xff\xff", 12);
			memcpy(da6.sin6_addr.s6_addr+12, buf[i].addr, 4);
			memcpy(da6.sin6_addr.s6_addr,
				"\0\0\0\0\0\0\0\0\0\0\xff\xff", 12);
			memcpy(da6.sin6_addr.s6_addr+12, buf[i].addr, 4);
			memcpy(&da4.sin_addr, buf[i].addr, 4);
			da = &da4; dalen = sizeof da4;
			sa = &sa4; salen = sizeof sa4;
		}
		const struct policy *dpolicy = policyof(&da6.sin6_addr);
		int dscope = scopeof(&da6.sin6_addr);
		int dlabel = dpolicy->label;
		int dprec = dpolicy->prec;
		int prefixlen = 0;
		int fd = socket(family, SOCK_DGRAM|SOCK_CLOEXEC, IPPROTO_UDP);
		if (fd >= 0) {
			if (!connect(fd, da, dalen)) {
				key |= DAS_USABLE;
				if (!getsockname(fd, sa, &salen)) {
					if (family == AF_INET) memcpy(
						sa6.sin6_addr.s6_addr+12,
						&sa4.sin_addr, 4);
					if (dscope == scopeof(&sa6.sin6_addr))
						key |= DAS_MATCHINGSCOPE;
					if (dlabel == labelof(&sa6.sin6_addr))
						key |= DAS_MATCHINGLABEL;
					prefixlen = prefixmatch(&sa6.sin6_addr,
						&da6.sin6_addr);
				}
			}
			close(fd);
		}
		key |= dprec << DAS_PREC_SHIFT;
		key |= (15-dscope) << DAS_SCOPE_SHIFT;
		key |= prefixlen << DAS_PREFIX_SHIFT;
		key |= (MAXADDRS-i) << DAS_ORDER_SHIFT;
		buf[i].sortkey = key;
	}
	qsort(buf, cnt, sizeof *buf, addrcmp);

	pthread_setcancelstate(cs, 0);

	return cnt;
}
