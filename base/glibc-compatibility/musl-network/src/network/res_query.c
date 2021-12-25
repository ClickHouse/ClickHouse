#define _BSD_SOURCE
#include <resolv.h>
#include <netdb.h>

int res_query(const char *name, int class, int type, unsigned char *dest, int len)
{
	unsigned char q[280];
	int ql = __res_mkquery(0, name, class, type, 0, 0, 0, q, sizeof q);
	if (ql < 0) return ql;
	int r = __res_send(q, ql, dest, len);
	if (r<12) {
		h_errno = TRY_AGAIN;
		return -1;
	}
	if ((dest[3] & 15) == 3) {
		h_errno = HOST_NOT_FOUND;
		return -1;
	}
	if ((dest[3] & 15) == 0 && !dest[6] && !dest[7]) {
		h_errno = NO_DATA;
		return -1;
	}
	return r;
}

weak_alias(res_query, res_search);
