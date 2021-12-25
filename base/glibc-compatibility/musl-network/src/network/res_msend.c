#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <string.h>
#include <poll.h>
#include <time.h>
#include <ctype.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include "stdio_impl.h"
#include "syscall.h"
#include "lookup.h"

static void cleanup(void *p)
{
	__syscall(SYS_close, (intptr_t)p);
}

static unsigned long mtime()
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return (unsigned long)ts.tv_sec * 1000
		+ ts.tv_nsec / 1000000;
}

int __res_msend_rc(int nqueries, const unsigned char *const *queries,
	const int *qlens, unsigned char *const *answers, int *alens, int asize,
	const struct resolvconf *conf)
{
	int fd;
	int timeout, attempts, retry_interval, servfail_retry;
	union {
		struct sockaddr_in sin;
		struct sockaddr_in6 sin6;
	} sa = {0}, ns[MAXNS] = {{0}};
	socklen_t sl = sizeof sa.sin;
	int nns = 0;
	int family = AF_INET;
	int rlen;
	int next;
	int i, j;
	int cs;
	struct pollfd pfd;
	unsigned long t0, t1, t2;

	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cs);

	timeout = 1000*conf->timeout;
	attempts = conf->attempts;

	for (nns=0; nns<conf->nns; nns++) {
		const struct address *iplit = &conf->ns[nns];
		if (iplit->family == AF_INET) {
			memcpy(&ns[nns].sin.sin_addr, iplit->addr, 4);
			ns[nns].sin.sin_port = htons(53);
			ns[nns].sin.sin_family = AF_INET;
		} else {
			sl = sizeof sa.sin6;
			memcpy(&ns[nns].sin6.sin6_addr, iplit->addr, 16);
			ns[nns].sin6.sin6_port = htons(53);
			ns[nns].sin6.sin6_scope_id = iplit->scopeid;
			ns[nns].sin6.sin6_family = family = AF_INET6;
		}
	}

	/* Get local address and open/bind a socket */
	sa.sin.sin_family = family;
	fd = socket(family, SOCK_DGRAM|SOCK_CLOEXEC|SOCK_NONBLOCK, 0);

	/* Handle case where system lacks IPv6 support */
	if (fd < 0 && family == AF_INET6 && errno == EAFNOSUPPORT) {
		fd = socket(AF_INET, SOCK_DGRAM|SOCK_CLOEXEC|SOCK_NONBLOCK, 0);
		family = AF_INET;
	}
	if (fd < 0 || bind(fd, (void *)&sa, sl) < 0) {
		if (fd >= 0) close(fd);
		pthread_setcancelstate(cs, 0);
		return -1;
	}

	/* Past this point, there are no errors. Each individual query will
	 * yield either no reply (indicated by zero length) or an answer
	 * packet which is up to the caller to interpret. */

	pthread_cleanup_push(cleanup, (void *)(intptr_t)fd);
	pthread_setcancelstate(cs, 0);

	/* Convert any IPv4 addresses in a mixed environment to v4-mapped */
	if (family == AF_INET6) {
		setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &(int){0}, sizeof 0);
		for (i=0; i<nns; i++) {
			if (ns[i].sin.sin_family != AF_INET) continue;
			memcpy(ns[i].sin6.sin6_addr.s6_addr+12,
				&ns[i].sin.sin_addr, 4);
			memcpy(ns[i].sin6.sin6_addr.s6_addr,
				"\0\0\0\0\0\0\0\0\0\0\xff\xff", 12);
			ns[i].sin6.sin6_family = AF_INET6;
			ns[i].sin6.sin6_flowinfo = 0;
			ns[i].sin6.sin6_scope_id = 0;
		}
	}

	memset(alens, 0, sizeof *alens * nqueries);

	pfd.fd = fd;
	pfd.events = POLLIN;
	retry_interval = timeout / attempts;
	next = 0;
	t0 = t2 = mtime();
	t1 = t2 - retry_interval;

	for (; t2-t0 < timeout; t2=mtime()) {
		if (t2-t1 >= retry_interval) {
			/* Query all configured namservers in parallel */
			for (i=0; i<nqueries; i++)
				if (!alens[i])
					for (j=0; j<nns; j++)
						sendto(fd, queries[i],
							qlens[i], MSG_NOSIGNAL,
							(void *)&ns[j], sl);
			t1 = t2;
			servfail_retry = 2 * nqueries;
		}

		/* Wait for a response, or until time to retry */
		if (poll(&pfd, 1, t1+retry_interval-t2) <= 0) continue;

		while ((rlen = recvfrom(fd, answers[next], asize, 0,
		  (void *)&sa, (socklen_t[1]){sl})) >= 0) {

			/* Ignore non-identifiable packets */
			if (rlen < 4) continue;

			/* Ignore replies from addresses we didn't send to */
			for (j=0; j<nns && memcmp(ns+j, &sa, sl); j++);
			if (j==nns) continue;

			/* Find which query this answer goes with, if any */
			for (i=next; i<nqueries && (
				answers[next][0] != queries[i][0] ||
				answers[next][1] != queries[i][1] ); i++);
			if (i==nqueries) continue;
			if (alens[i]) continue;

			/* Only accept positive or negative responses;
			 * retry immediately on server failure, and ignore
			 * all other codes such as refusal. */
			switch (answers[next][3] & 15) {
			case 0:
			case 3:
				break;
			case 2:
				if (servfail_retry && servfail_retry--)
					sendto(fd, queries[i],
						qlens[i], MSG_NOSIGNAL,
						(void *)&ns[j], sl);
			default:
				continue;
			}

			/* Store answer in the right slot, or update next
			 * available temp slot if it's already in place. */
			alens[i] = rlen;
			if (i == next)
				for (; next<nqueries && alens[next]; next++);
			else
				memcpy(answers[i], answers[next], rlen);

			if (next == nqueries) goto out;
		}
	}
out:
	pthread_cleanup_pop(1);

	return 0;
}

int __res_msend(int nqueries, const unsigned char *const *queries,
	const int *qlens, unsigned char *const *answers, int *alens, int asize)
{
	struct resolvconf conf;
	if (__get_resolv_conf(&conf, 0, 0) < 0) return -1;
	return __res_msend_rc(nqueries, queries, qlens, answers, alens, asize, &conf);
}
