#include <time.h>

int clock_gettime(clockid_t, struct timespec *);

/* There is no other implemented value than TIME_UTC; all other values
 * are considered erroneous. */
int timespec_get(struct timespec * ts, int base)
{
	if (base != TIME_UTC) return 0;
	int ret = clock_gettime(CLOCK_REALTIME, ts);
	return ret < 0 ? 0 : base;
}
