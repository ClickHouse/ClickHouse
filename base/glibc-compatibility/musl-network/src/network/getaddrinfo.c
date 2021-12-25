#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <endian.h>
#include <errno.h>
#include "lookup.h"

int getaddrinfo(const char *restrict host, const char *restrict serv, const struct addrinfo *restrict hint, struct addrinfo **restrict res)
{
	struct service ports[MAXSERVS];
	struct address addrs[MAXADDRS];
	char canon[256], *outcanon;
	int nservs, naddrs, nais, canon_len, i, j, k;
	int family = AF_UNSPEC, flags = 0, proto = 0, socktype = 0;
	struct aibuf *out;

	if (!host && !serv) return EAI_NONAME;

	if (hint) {
		family = hint->ai_family;
		flags = hint->ai_flags;
		proto = hint->ai_protocol;
		socktype = hint->ai_socktype;

		const int mask = AI_PASSIVE | AI_CANONNAME | AI_NUMERICHOST |
			AI_V4MAPPED | AI_ALL | AI_ADDRCONFIG | AI_NUMERICSERV;
		if ((flags & mask) != flags)
			return EAI_BADFLAGS;

		switch (family) {
		case AF_INET:
		case AF_INET6:
		case AF_UNSPEC:
			break;
		default:
			return EAI_FAMILY;
		}
	}

	if (flags & AI_ADDRCONFIG) {
		/* Define the "an address is configured" condition for address
		 * families via ability to create a socket for the family plus
		 * routability of the loopback address for the family. */
		static const struct sockaddr_in lo4 = {
			.sin_family = AF_INET, .sin_port = 65535,
			.sin_addr.s_addr = __BYTE_ORDER == __BIG_ENDIAN
				? 0x7f000001 : 0x0100007f
		};
		static const struct sockaddr_in6 lo6 = {
			.sin6_family = AF_INET6, .sin6_port = 65535,
			.sin6_addr = IN6ADDR_LOOPBACK_INIT
		};
		int tf[2] = { AF_INET, AF_INET6 };
		const void *ta[2] = { &lo4, &lo6 };
		socklen_t tl[2] = { sizeof lo4, sizeof lo6 };
		for (i=0; i<2; i++) {
			if (family==tf[1-i]) continue;
			int s = socket(tf[i], SOCK_CLOEXEC|SOCK_DGRAM,
				IPPROTO_UDP);
			if (s>=0) {
				int cs;
				pthread_setcancelstate(
					PTHREAD_CANCEL_DISABLE, &cs);
				int r = connect(s, ta[i], tl[i]);
				pthread_setcancelstate(cs, 0);
				close(s);
				if (!r) continue;
			}
			switch (errno) {
			case EADDRNOTAVAIL:
			case EAFNOSUPPORT:
			case EHOSTUNREACH:
			case ENETDOWN:
			case ENETUNREACH:
				break;
			default:
				return EAI_SYSTEM;
			}
			if (family == tf[i]) return EAI_NONAME;
			family = tf[1-i];
		}
	}

	nservs = __lookup_serv(ports, serv, proto, socktype, flags);
	if (nservs < 0) return nservs;

	naddrs = __lookup_name(addrs, canon, host, family, flags);
	if (naddrs < 0) return naddrs;

	nais = nservs * naddrs;
	canon_len = strlen(canon);
	out = calloc(1, nais * sizeof(*out) + canon_len + 1);
	if (!out) return EAI_MEMORY;

	if (canon_len) {
		outcanon = (void *)&out[nais];
		memcpy(outcanon, canon, canon_len+1);
	} else {
		outcanon = 0;
	}

	for (k=i=0; i<naddrs; i++) for (j=0; j<nservs; j++, k++) {
		out[k].slot = k;
		out[k].ai = (struct addrinfo){
			.ai_family = addrs[i].family,
			.ai_socktype = ports[j].socktype,
			.ai_protocol = ports[j].proto,
			.ai_addrlen = addrs[i].family == AF_INET
				? sizeof(struct sockaddr_in)
				: sizeof(struct sockaddr_in6),
			.ai_addr = (void *)&out[k].sa,
			.ai_canonname = outcanon };
		if (k) out[k-1].ai.ai_next = &out[k].ai;
		switch (addrs[i].family) {
		case AF_INET:
			out[k].sa.sin.sin_family = AF_INET;
			out[k].sa.sin.sin_port = htons(ports[j].port);
			memcpy(&out[k].sa.sin.sin_addr, &addrs[i].addr, 4);
			break;
		case AF_INET6:
			out[k].sa.sin6.sin6_family = AF_INET6;
			out[k].sa.sin6.sin6_port = htons(ports[j].port);
			out[k].sa.sin6.sin6_scope_id = addrs[i].scopeid;
			memcpy(&out[k].sa.sin6.sin6_addr, &addrs[i].addr, 16);
			break;			
		}
	}
	out[0].ref = nais;
	*res = &out->ai;
	return 0;
}
