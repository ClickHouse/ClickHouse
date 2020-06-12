/* $OpenBSD: o_time.c,v 1.15 2014/06/12 15:49:27 deraadt Exp $ */
/* Written by Richard Levitte (richard@levitte.org) for the OpenSSL
 * project 2001.
 */
/* Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project 2008.
 */
/* ====================================================================
 * Copyright (c) 2001 The OpenSSL Project.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. All advertising materials mentioning features or use of this
 *    software must display the following acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit. (http://www.OpenSSL.org/)"
 *
 * 4. The names "OpenSSL Toolkit" and "OpenSSL Project" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    licensing@OpenSSL.org.
 *
 * 5. Products derived from this software may not be called "OpenSSL"
 *    nor may "OpenSSL" appear in their names without prior written
 *    permission of the OpenSSL Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit (http://www.OpenSSL.org/)"
 *
 * THIS SOFTWARE IS PROVIDED BY THE OpenSSL PROJECT ``AS IS'' AND ANY
 * EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE OpenSSL PROJECT OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * ====================================================================
 *
 * This product includes cryptographic software written by Eric Young
 * (eay@cryptsoft.com).  This product includes software written by Tim
 * Hudson (tjh@cryptsoft.com).
 *
 */

#include <string.h>

#include "o_time.h"

/* Take a tm structure and add an offset to it. This avoids any OS issues
 * with restricted date types and overflows which cause the year 2038
 * problem.
 */

#define SECS_PER_DAY (24 * 60 * 60)

static long date_to_julian(int y, int m, int d);
static void julian_to_date(long jd, int *y, int *m, int *d);

int
OPENSSL_gmtime_adj(struct tm *tm, int off_day, long offset_sec)
{
	int offset_hms, offset_day;
	long time_jd;
	int time_year, time_month, time_day;
	/* split offset into days and day seconds */
	offset_day = offset_sec / SECS_PER_DAY;
	/* Avoid sign issues with % operator */
	offset_hms = offset_sec - (offset_day * SECS_PER_DAY);
	offset_day += off_day;
	/* Add current time seconds to offset */
	offset_hms += tm->tm_hour * 3600 + tm->tm_min * 60 + tm->tm_sec;
	/* Adjust day seconds if overflow */
	if (offset_hms >= SECS_PER_DAY) {
		offset_day++;
		offset_hms -= SECS_PER_DAY;
	} else if (offset_hms < 0) {
		offset_day--;
		offset_hms += SECS_PER_DAY;
	}

	/* Convert date of time structure into a Julian day number.
	 */

	time_year = tm->tm_year + 1900;
	time_month = tm->tm_mon + 1;
	time_day = tm->tm_mday;

	time_jd = date_to_julian(time_year, time_month, time_day);

	/* Work out Julian day of new date */
	time_jd += offset_day;

	if (time_jd < 0)
		return 0;

	/* Convert Julian day back to date */

	julian_to_date(time_jd, &time_year, &time_month, &time_day);

	if (time_year < 1900 || time_year > 9999)
		return 0;

	/* Update tm structure */

	tm->tm_year = time_year - 1900;
	tm->tm_mon = time_month - 1;
	tm->tm_mday = time_day;

	tm->tm_hour = offset_hms / 3600;
	tm->tm_min = (offset_hms / 60) % 60;
	tm->tm_sec = offset_hms % 60;

	return 1;

}

/* Convert date to and from julian day
 * Uses Fliegel & Van Flandern algorithm
 */
static long
date_to_julian(int y, int m, int d)
{
	return (1461 * (y + 4800 + (m - 14) / 12)) / 4 +
	    (367 * (m - 2 - 12 * ((m - 14) / 12))) / 12 -
	    (3 * ((y + 4900 + (m - 14) / 12) / 100)) / 4 +
	    d - 32075;
}

static void
julian_to_date(long jd, int *y, int *m, int *d)
{
	long  L = jd + 68569;
	long  n = (4 * L) / 146097;
	long  i, j;

	L = L - (146097 * n + 3) / 4;
	i = (4000 * (L + 1)) / 1461001;
	L = L - (1461 * i) / 4 + 31;
	j = (80 * L) / 2447;
	*d = L - (2447 * j) / 80;
	L = j / 11;
	*m = j + 2 - (12 * L);
	*y = 100 * (n - 49) + i + L;
}
