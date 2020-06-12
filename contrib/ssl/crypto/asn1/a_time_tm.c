/* $OpenBSD: a_time_tm.c,v 1.14 2017/08/28 17:42:47 jsing Exp $ */
/*
 * Copyright (c) 2015 Bob Beck <beck@openbsd.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <openssl/asn1t.h>
#include <openssl/err.h>

#include "o_time.h"

#define RFC5280 0
#define GENTIME_LENGTH 15
#define UTCTIME_LENGTH 13

int
ASN1_time_tm_cmp(struct tm *tm1, struct tm *tm2)
{
	if (tm1->tm_year < tm2->tm_year)
		return (-1);
	if (tm1->tm_year > tm2->tm_year)
		return (1);
	if (tm1->tm_mon < tm2->tm_mon)
		return (-1);
	if (tm1->tm_mon > tm2->tm_mon)
		return (1);
	if (tm1->tm_mday < tm2->tm_mday)
		return (-1);
	if (tm1->tm_mday > tm2->tm_mday)
		return (1);
	if (tm1->tm_hour < tm2->tm_hour)
		return (-1);
	if (tm1->tm_hour > tm2->tm_hour)
		return (1);
	if (tm1->tm_min < tm2->tm_min)
		return (-1);
	if (tm1->tm_min > tm2->tm_min)
		return (1);
	if (tm1->tm_sec < tm2->tm_sec)
		return (-1);
	if (tm1->tm_sec > tm2->tm_sec)
		return (1);
	return 0;
}

int
ASN1_time_tm_clamp_notafter(struct tm *tm)
{
#ifdef SMALL_TIME_T
	struct tm broken_os_epoch_tm;
	time_t broken_os_epoch_time = INT_MAX;

	if (gmtime_r(&broken_os_epoch_time, &broken_os_epoch_tm) == NULL)
		return 0;

	if (ASN1_time_tm_cmp(tm, &broken_os_epoch_tm) == 1)
		memcpy(tm, &broken_os_epoch_tm, sizeof(*tm));
#endif
	return 1;
}

/* Format a time as an RFC 5280 format Generalized time */
char *
gentime_string_from_tm(struct tm *tm)
{
	char *ret = NULL;
	int year;

	year = tm->tm_year + 1900;
	if (year < 0 || year > 9999)
		return (NULL);

	if (asprintf(&ret, "%04u%02u%02u%02u%02u%02uZ", year,
	    tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min,
	    tm->tm_sec) == -1)
		ret = NULL;

	return (ret);
}

/* Format a time as an RFC 5280 format UTC time */
char *
utctime_string_from_tm(struct tm *tm)
{
	char *ret = NULL;

	if (tm->tm_year >= 150 || tm->tm_year < 50)
		return (NULL);

	if (asprintf(&ret, "%02u%02u%02u%02u%02u%02uZ",
	    tm->tm_year % 100,  tm->tm_mon + 1, tm->tm_mday,
	    tm->tm_hour, tm->tm_min, tm->tm_sec) == -1)
		ret = NULL;

	return (ret);
}

/* Format a time correctly for an X509 object as per RFC 5280 */
char *
rfc5280_string_from_tm(struct tm *tm)
{
	char *ret = NULL;
	int year;

	year = tm->tm_year + 1900;
	if (year < 1950 || year > 9999)
		return (NULL);

	if (year < 2050)
		ret = utctime_string_from_tm(tm);
	else
		ret = gentime_string_from_tm(tm);

	return (ret);
}

/*
 * Parse an RFC 5280 format ASN.1 time string.
 *
 * mode must be:
 * 0 if we expect to parse a time as specified in RFC 5280 for an X509 object.
 * V_ASN1_UTCTIME if we wish to parse an RFC5280 format UTC time.
 * V_ASN1_GENERALIZEDTIME if we wish to parse an RFC5280 format Generalized time.
 *
 * Returns:
 * -1 if the string was invalid.
 * V_ASN1_UTCTIME if the string validated as a UTC time string.
 * V_ASN1_GENERALIZEDTIME if the string validated as a Generalized time string.
 *
 * Fills in *tm with the corresponding time if tm is non NULL.
 */
#define	ATOI2(ar)	((ar) += 2, ((ar)[-2] - '0') * 10 + ((ar)[-1] - '0'))
int
ASN1_time_parse(const char *bytes, size_t len, struct tm *tm, int mode)
{
	size_t i;
	int type = 0;
	struct tm ltm;
	struct tm *lt;
	const char *p;

	if (bytes == NULL)
		return (-1);

	/* Constrain to valid lengths. */
	if (len != UTCTIME_LENGTH && len != GENTIME_LENGTH)
		return (-1);

	lt = tm;
	if (lt == NULL) {
		memset(&ltm, 0, sizeof(ltm));
		lt = &ltm;
	}

	/* Timezone is required and must be GMT (Zulu). */
	if (bytes[len - 1] != 'Z')
		return (-1);

	/* Make sure everything else is digits. */
	for (i = 0; i < len - 1; i++) {
		if (isdigit((unsigned char)bytes[i]))
			continue;
		return (-1);
	}

	/*
	 * Validate and convert the time
	 */
	p = bytes;
	switch (len) {
	case GENTIME_LENGTH:
		if (mode == V_ASN1_UTCTIME)
			return (-1);
		lt->tm_year = (ATOI2(p) * 100) - 1900;	/* cc */
		type = V_ASN1_GENERALIZEDTIME;
		/* FALLTHROUGH */
	case UTCTIME_LENGTH:
		if (type == 0) {
			if (mode == V_ASN1_GENERALIZEDTIME)
				return (-1);
			type = V_ASN1_UTCTIME;
		}
		lt->tm_year += ATOI2(p);		/* yy */
		if (type == V_ASN1_UTCTIME) {
			if (lt->tm_year < 50)
				lt->tm_year += 100;
		}
		lt->tm_mon = ATOI2(p) - 1;		/* mm */
		if (lt->tm_mon < 0 || lt->tm_mon > 11)
			return (-1);
		lt->tm_mday = ATOI2(p);			/* dd */
		if (lt->tm_mday < 1 || lt->tm_mday > 31)
			return (-1);
		lt->tm_hour = ATOI2(p);			/* HH */
		if (lt->tm_hour < 0 || lt->tm_hour > 23)
			return (-1);
		lt->tm_min = ATOI2(p);			/* MM */
		if (lt->tm_min < 0 || lt->tm_min > 59)
			return (-1);
		lt->tm_sec = ATOI2(p);			/* SS */
		/* Leap second 60 is not accepted. Reconsider later? */
		if (lt->tm_sec < 0 || lt->tm_sec > 59)
			return (-1);
		break;
	default:
		return (-1);
	}

	return (type);
}

/*
 * ASN1_TIME generic functions.
 */

static int
ASN1_TIME_set_string_internal(ASN1_TIME *s, const char *str, int mode)
{
	int type;
	char *tmp;

	if ((type = ASN1_time_parse(str, strlen(str), NULL, mode)) == -1)
		return (0);
	if (mode != 0 && mode != type)
		return (0);

	if (s == NULL)
		return (1);

	if ((tmp = strdup(str)) == NULL)
		return (0);
	free(s->data);
	s->data = tmp;
	s->length = strlen(tmp);
	s->type = type;

	return (1);
}

static ASN1_TIME *
ASN1_TIME_adj_internal(ASN1_TIME *s, time_t t, int offset_day, long offset_sec,
    int mode)
{
	int allocated = 0;
	struct tm tm;
	size_t len;
	char * p;

 	if (gmtime_r(&t, &tm) == NULL)
 		return (NULL);

	if (offset_day || offset_sec) {
		if (!OPENSSL_gmtime_adj(&tm, offset_day, offset_sec))
			return (NULL);
	}

	switch (mode) {
	case V_ASN1_UTCTIME:
		p = utctime_string_from_tm(&tm);
		break;
	case V_ASN1_GENERALIZEDTIME:
		p = gentime_string_from_tm(&tm);
		break;
	case RFC5280:
		p = rfc5280_string_from_tm(&tm);
		break;
	default:
		return (NULL);
	}
	if (p == NULL) {
		ASN1error(ASN1_R_ILLEGAL_TIME_VALUE);
		return (NULL);
	}

	if (s == NULL) {
		if ((s = ASN1_TIME_new()) == NULL)
			return (NULL);
		allocated = 1;
	}

	len = strlen(p);
	switch (len) {
	case GENTIME_LENGTH:
		s->type = V_ASN1_GENERALIZEDTIME;
		break;
 	case UTCTIME_LENGTH:
		s->type = V_ASN1_UTCTIME;
		break;
	default:
		if (allocated)
			ASN1_TIME_free(s);
		free(p);
		return (NULL);
	}
	free(s->data);
	s->data = p;
	s->length = len;
	return (s);
}

ASN1_TIME *
ASN1_TIME_set(ASN1_TIME *s, time_t t)
{
	return (ASN1_TIME_adj(s, t, 0, 0));
}

ASN1_TIME *
ASN1_TIME_set_tm(ASN1_TIME *s, struct tm *tm)
{
	time_t t;

	if ((t = timegm(tm)) == -1)
		return NULL;
	return (ASN1_TIME_adj(s, t, 0, 0));
}

ASN1_TIME *
ASN1_TIME_adj(ASN1_TIME *s, time_t t, int offset_day, long offset_sec)
{
	return (ASN1_TIME_adj_internal(s, t, offset_day, offset_sec, RFC5280));
}

int
ASN1_TIME_check(ASN1_TIME *t)
{
	if (t->type != V_ASN1_GENERALIZEDTIME && t->type != V_ASN1_UTCTIME)
		return (0);
	return (t->type == ASN1_time_parse(t->data, t->length, NULL, t->type));
}

ASN1_GENERALIZEDTIME *
ASN1_TIME_to_generalizedtime(ASN1_TIME *t, ASN1_GENERALIZEDTIME **out)
{
	ASN1_GENERALIZEDTIME *tmp = NULL;
	struct tm tm;
	char *str;

	if (t->type != V_ASN1_GENERALIZEDTIME && t->type != V_ASN1_UTCTIME)
		return (NULL);

	memset(&tm, 0, sizeof(tm));
	if (t->type != ASN1_time_parse(t->data, t->length, &tm, t->type))
		return (NULL);
	if ((str = gentime_string_from_tm(&tm)) == NULL)
		return (NULL);

	if (out != NULL)
		tmp = *out;
	if (tmp == NULL && (tmp = ASN1_GENERALIZEDTIME_new()) == NULL) {
		free(str);
		return (NULL);
	}
	if (out != NULL)
		*out = tmp;

	free(tmp->data);
	tmp->data = str;
	tmp->length = strlen(str);
	return (tmp);
}

int
ASN1_TIME_set_string(ASN1_TIME *s, const char *str)
{
	return (ASN1_TIME_set_string_internal(s, str, 0));
}

/*
 * ASN1_UTCTIME wrappers
 */

int
ASN1_UTCTIME_check(ASN1_UTCTIME *d)
{
	if (d->type != V_ASN1_UTCTIME)
		return (0);
	return (d->type == ASN1_time_parse(d->data, d->length, NULL, d->type));
}

int
ASN1_UTCTIME_set_string(ASN1_UTCTIME *s, const char *str)
{
	if (s != NULL && s->type != V_ASN1_UTCTIME)
		return (0);
	return (ASN1_TIME_set_string_internal(s, str, V_ASN1_UTCTIME));
}

ASN1_UTCTIME *
ASN1_UTCTIME_set(ASN1_UTCTIME *s, time_t t)
{
	return (ASN1_UTCTIME_adj(s, t, 0, 0));
}

ASN1_UTCTIME *
ASN1_UTCTIME_adj(ASN1_UTCTIME *s, time_t t, int offset_day, long offset_sec)
{
	return (ASN1_TIME_adj_internal(s, t, offset_day, offset_sec,
	    V_ASN1_UTCTIME));
}

int
ASN1_UTCTIME_cmp_time_t(const ASN1_UTCTIME *s, time_t t2)
{
	struct tm tm1, tm2;

	/*
	 * This function has never handled failure conditions properly
	 * and should be deprecated. The OpenSSL version used to
	 * simply follow NULL pointers on failure. BoringSSL and
	 * OpenSSL now make it return -2 on failure.
	 *
	 * The danger is that users of this function will not
	 * differentiate the -2 failure case from t1 < t2.
	 */
	if (ASN1_time_parse(s->data, s->length, &tm1, V_ASN1_UTCTIME) == -1)
		return (-2); /* XXX */

	if (gmtime_r(&t2, &tm2) == NULL)
		return (-2); /* XXX */

	return ASN1_time_tm_cmp(&tm1, &tm2);
}

/*
 * ASN1_GENERALIZEDTIME wrappers
 */

int
ASN1_GENERALIZEDTIME_check(ASN1_GENERALIZEDTIME *d)
{
	if (d->type != V_ASN1_GENERALIZEDTIME)
		return (0);
	return (d->type == ASN1_time_parse(d->data, d->length, NULL, d->type));
}

int
ASN1_GENERALIZEDTIME_set_string(ASN1_GENERALIZEDTIME *s, const char *str)
{
	if (s != NULL && s->type != V_ASN1_GENERALIZEDTIME)
		return (0);
	return (ASN1_TIME_set_string_internal(s, str, V_ASN1_GENERALIZEDTIME));
}

ASN1_GENERALIZEDTIME *
ASN1_GENERALIZEDTIME_set(ASN1_GENERALIZEDTIME *s, time_t t)
{
	return (ASN1_GENERALIZEDTIME_adj(s, t, 0, 0));
}

ASN1_GENERALIZEDTIME *
ASN1_GENERALIZEDTIME_adj(ASN1_GENERALIZEDTIME *s, time_t t, int offset_day,
    long offset_sec)
{
	return (ASN1_TIME_adj_internal(s, t, offset_day, offset_sec,
	    V_ASN1_GENERALIZEDTIME));
}
