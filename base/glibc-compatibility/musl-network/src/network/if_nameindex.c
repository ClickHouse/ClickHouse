#define _GNU_SOURCE
#include <net/if.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "netlink.h"

#define IFADDRS_HASH_SIZE 64

struct ifnamemap {
	unsigned int hash_next;
	unsigned int index;
	unsigned char namelen;
	char name[IFNAMSIZ];
};

struct ifnameindexctx {
	unsigned int num, allocated, str_bytes;
	struct ifnamemap *list;
	unsigned int hash[IFADDRS_HASH_SIZE];
};

static int netlink_msg_to_nameindex(void *pctx, struct nlmsghdr *h)
{
	struct ifnameindexctx *ctx = pctx;
	struct ifnamemap *map;
	struct rtattr *rta;
	unsigned int i;
	int index, type, namelen, bucket;

	if (h->nlmsg_type == RTM_NEWLINK) {
		struct ifinfomsg *ifi = NLMSG_DATA(h);
		index = ifi->ifi_index;
		type = IFLA_IFNAME;
		rta = NLMSG_RTA(h, sizeof(*ifi));
	} else {
		struct ifaddrmsg *ifa = NLMSG_DATA(h);
		index = ifa->ifa_index;
		type = IFA_LABEL;
		rta = NLMSG_RTA(h, sizeof(*ifa));
	}
	for (; NLMSG_RTAOK(rta, h); rta = RTA_NEXT(rta)) {
		if (rta->rta_type != type) continue;

		namelen = RTA_DATALEN(rta) - 1;
		if (namelen > IFNAMSIZ) return 0;

		/* suppress duplicates */
		bucket = index % IFADDRS_HASH_SIZE;
		i = ctx->hash[bucket];
		while (i) {
			map = &ctx->list[i-1];
			if (map->index == index &&
			    map->namelen == namelen &&
			    memcmp(map->name, RTA_DATA(rta), namelen) == 0)
				return 0;
			i = map->hash_next;
		}

		if (ctx->num >= ctx->allocated) {
			size_t a = ctx->allocated ? ctx->allocated * 2 + 1 : 8;
			if (a > SIZE_MAX/sizeof *map) return -1;
			map = realloc(ctx->list, a * sizeof *map);
			if (!map) return -1;
			ctx->list = map;
			ctx->allocated = a;
		}
		map = &ctx->list[ctx->num];
		map->index = index;
		map->namelen = namelen;
		memcpy(map->name, RTA_DATA(rta), namelen);
		ctx->str_bytes += namelen + 1;
		ctx->num++;
		map->hash_next = ctx->hash[bucket];
		ctx->hash[bucket] = ctx->num;
		return 0;
	}
	return 0;
}

struct if_nameindex *if_nameindex()
{
	struct ifnameindexctx _ctx, *ctx = &_ctx;
	struct if_nameindex *ifs = 0, *d;
	struct ifnamemap *s;
	char *p;
	int i;
	int cs;

	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cs);
	memset(ctx, 0, sizeof(*ctx));
	if (__rtnetlink_enumerate(AF_UNSPEC, AF_INET, netlink_msg_to_nameindex, ctx) < 0) goto err;

	ifs = malloc(sizeof(struct if_nameindex[ctx->num+1]) + ctx->str_bytes);
	if (!ifs) goto err;

	p = (char*)(ifs + ctx->num + 1);
	for (i = ctx->num, d = ifs, s = ctx->list; i; i--, s++, d++) {
		d->if_index = s->index;
		d->if_name = p;
		memcpy(p, s->name, s->namelen);
		p += s->namelen;
		*p++ = 0;
	}
	d->if_index = 0;
	d->if_name = 0;
err:
	pthread_setcancelstate(cs, 0);
	free(ctx->list);
	errno = ENOBUFS;
	return ifs;
}
