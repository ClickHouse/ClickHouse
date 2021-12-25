#define _GNU_SOURCE
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include "lookup.h"

#define ALIGN (sizeof(struct { char a; char *b; }) - sizeof(char *))

int getservbyname_r(const char *name, const char *prots,
	struct servent *se, char *buf, size_t buflen, struct servent **res)
{
	struct service servs[MAXSERVS];
	int cnt, proto, align;

	*res = 0;

	/* Don't treat numeric port number strings as service records. */
	char *end = "";
	strtoul(name, &end, 10);
	if (!*end) return ENOENT;

	/* Align buffer */
	align = -(uintptr_t)buf & ALIGN-1;
	if (buflen < 2*sizeof(char *)+align)
		return ERANGE;
	buf += align;

	if (!prots) proto = 0;
	else if (!strcmp(prots, "tcp")) proto = IPPROTO_TCP;
	else if (!strcmp(prots, "udp")) proto = IPPROTO_UDP;
	else return EINVAL;

	cnt = __lookup_serv(servs, name, proto, 0, 0);
	if (cnt<0) switch (cnt) {
	case EAI_MEMORY:
	case EAI_SYSTEM:
		return ENOMEM;
	default:
		return ENOENT;
	}

	se->s_name = (char *)name;
	se->s_aliases = (void *)buf;
	se->s_aliases[0] = se->s_name;
	se->s_aliases[1] = 0;
	se->s_port = htons(servs[0].port);
	se->s_proto = servs[0].proto == IPPROTO_TCP ? "tcp" : "udp";

	*res = se;
	return 0;
}
