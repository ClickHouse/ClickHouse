/* glibc exports `lgammaf` as `lgammaf@@GLIBC_2.23`, which is newer than the oldest
 * glibc that ClickHouse binaries must run on, so a reference to it fails the
 * compatibility check. Provide `lgammaf` here so that it resolves locally instead.
 *
 * The double-precision `lgamma_r` from `lgamma.c` is evaluated and rounded to float,
 * the same way `lgammal.c` reuses it for `lgammal_r`. Double has enough headroom that
 * the result is correctly rounded: measured against glibc over the whole float range
 * this agrees bit for bit, whereas a port of musl's single-precision kernel loses up
 * to ~1700 ulp at negative arguments, where the reflection formula cancels. */

#include <math.h>

extern double lgamma_r(double x, int *signgamp);

float lgammaf_r(float x, int *signgamp)
{
	/* Gamma has poles at the negative integers. `lgamma_r` misses them because it
	 * detects them with `sin(pi*x) == 0`, which never holds exactly for the double
	 * approximation of pi, so it returns a large finite value instead. Zero is not
	 * handled here: `lgamma_r` already returns the right thing for it. */
	if (x < 0 && x == truncf(x))
	{
		*signgamp = 1;
		return INFINITY;
	}
	return (float)lgamma_r((double)x, signgamp);
}

extern int signgam;

float lgammaf(float x)
{
	return lgammaf_r(x, &signgam);
}
