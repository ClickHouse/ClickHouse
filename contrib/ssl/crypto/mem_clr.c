/* $OpenBSD: mem_clr.c,v 1.4 2014/06/12 15:49:27 deraadt Exp $ */

/* Ted Unangst places this file in the public domain. */
#include <string.h>
#include <openssl/crypto.h>

void
OPENSSL_cleanse(void *ptr, size_t len)
{
	explicit_bzero(ptr, len);
}
