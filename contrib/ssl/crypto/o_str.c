/* $OpenBSD: o_str.c,v 1.9 2014/07/09 20:22:14 tedu Exp $ */
/*
 * Written by Theo de Raadt.  Public domain.
 */

#include <string.h>

int OPENSSL_strcasecmp(const char *str1, const char *str2);
int OPENSSL_strncasecmp(const char *str1, const char *str2, size_t n);

int
OPENSSL_strncasecmp(const char *str1, const char *str2, size_t n)
{
	return strncasecmp(str1, str2, n);
}

int
OPENSSL_strcasecmp(const char *str1, const char *str2)
{
	return strcasecmp(str1, str2);
}
