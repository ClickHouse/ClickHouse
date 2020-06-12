/* $OpenBSD: b_print.c,v 1.25 2014/06/12 15:49:28 deraadt Exp $ */

/* Theo de Raadt places this file in the public domain. */

#include <openssl/bio.h>

int
BIO_printf(BIO *bio, const char *format, ...)
{
	va_list args;
	int ret;

	va_start(args, format);
	ret = BIO_vprintf(bio, format, args);
	va_end(args);
	return (ret);
}

#ifdef HAVE_FUNOPEN
static int
_BIO_write(void *cookie, const char *buf, int nbytes)
{
	return BIO_write(cookie, buf, nbytes);
}

int
BIO_vprintf(BIO *bio, const char *format, va_list args)
{
	int ret;
	FILE *fp;

	fp = funopen(bio, NULL, &_BIO_write, NULL, NULL);
	if (fp == NULL) {
		ret = -1;
		goto fail;
	}
	ret = vfprintf(fp, format, args);
	fclose(fp);
fail:
	return (ret);
}

#else /* !HAVE_FUNOPEN */

int
BIO_vprintf(BIO *bio, const char *format, va_list args)
{
	int ret;
	char *buf = NULL;

	ret = vasprintf(&buf, format, args);
	if (buf == NULL) {
		ret = -1;
		goto fail;
	}
	BIO_write(bio, buf, ret);
	free(buf);
fail:
	return (ret);
}

#endif /* HAVE_FUNOPEN */

/*
 * BIO_snprintf and BIO_vsnprintf return -1 for overflow,
 * due to the history of this API.  Justification:
 *
 * Traditional snprintf surfaced in 4.4BSD, and returned
 * "number of bytes wanted". Solaris and Windows opted to
 * return -1.  A draft standard was written which returned -1.
 * Due to the large volume of code already using the first
 * semantics, the draft was repaired before standardization to
 * specify "number of bytes wanted" plus "-1 for character conversion
 * style errors".  Solaris adapted to this rule, but Windows stuck
 * with -1.
 *
 * Original OpenSSL comment which is full of lies:
 *
 * "In case of truncation, return -1 like traditional snprintf.
 * (Current drafts for ISO/IEC 9899 say snprintf should return
 * the number of characters that would have been written,
 * had the buffer been large enough.)"
 */
int
BIO_snprintf(char *buf, size_t n, const char *format, ...)
{
	va_list args;
	int ret;

	va_start(args, format);
	ret = vsnprintf(buf, n, format, args);
	va_end(args);

	if (ret >= n || ret == -1)
		return (-1);
	return (ret);
}

int
BIO_vsnprintf(char *buf, size_t n, const char *format, va_list args)
{
	int ret;

	ret = vsnprintf(buf, n, format, args);

	if (ret >= n || ret == -1)
		return (-1);
	return (ret);
}
