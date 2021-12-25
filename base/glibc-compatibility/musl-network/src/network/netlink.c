#include <errno.h>
#include <string.h>
#include <syscall.h>
#include <sys/socket.h>
#include "netlink.h"

static int __netlink_enumerate(int fd, unsigned int seq, int type, int af,
	int (*cb)(void *ctx, struct nlmsghdr *h), void *ctx)
{
	struct nlmsghdr *h;
	union {
		uint8_t buf[8192];
		struct {
			struct nlmsghdr nlh;
			struct rtgenmsg g;
		} req;
		struct nlmsghdr reply;
	} u;
	int r, ret;

	memset(&u.req, 0, sizeof(u.req));
	u.req.nlh.nlmsg_len = sizeof(u.req);
	u.req.nlh.nlmsg_type = type;
	u.req.nlh.nlmsg_flags = NLM_F_DUMP | NLM_F_REQUEST;
	u.req.nlh.nlmsg_seq = seq;
	u.req.g.rtgen_family = af;
	r = send(fd, &u.req, sizeof(u.req), 0);
	if (r < 0) return r;

	while (1) {
		r = recv(fd, u.buf, sizeof(u.buf), MSG_DONTWAIT);
		if (r <= 0) return -1;
		for (h = &u.reply; NLMSG_OK(h, (void*)&u.buf[r]); h = NLMSG_NEXT(h)) {
			if (h->nlmsg_type == NLMSG_DONE) return 0;
			if (h->nlmsg_type == NLMSG_ERROR) return -1;
			ret = cb(ctx, h);
			if (ret) return ret;
		}
	}
}

int __rtnetlink_enumerate(int link_af, int addr_af, int (*cb)(void *ctx, struct nlmsghdr *h), void *ctx)
{
	int fd, r;

	fd = socket(PF_NETLINK, SOCK_RAW|SOCK_CLOEXEC, NETLINK_ROUTE);
	if (fd < 0) return -1;
	r = __netlink_enumerate(fd, 1, RTM_GETLINK, link_af, cb, ctx);
	if (!r) r = __netlink_enumerate(fd, 2, RTM_GETADDR, addr_af, cb, ctx);
	__syscall(SYS_close,fd);
	return r;
}
