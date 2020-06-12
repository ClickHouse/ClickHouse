/* $OpenBSD: tls_util.c,v 1.9 2017/06/22 18:03:57 jsing Exp $ */
/*
 * Copyright (c) 2014 Joel Sing <jsing@openbsd.org>
 * Copyright (c) 2015 Reyk Floeter <reyk@openbsd.org>
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

#include <sys/stat.h>

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

#include "tls.h"
#include "tls_internal.h"

/*
 * Extract the host and port from a colon separated value. For a literal IPv6
 * address the address must be contained with square braces. If a host and
 * port are successfully extracted, the function will return 0 and the
 * caller is responsible for freeing the host and port. If no port is found
 * then the function will return 1, with both host and port being NULL.
 * On memory allocation failure -1 will be returned.
 */
int
tls_host_port(const char *hostport, char **host, char **port)
{
	char *h, *p, *s;
	int rv = 1;

	*host = NULL;
	*port = NULL;

	if ((s = strdup(hostport)) == NULL)
		goto fail;

	h = p = s;

	/* See if this is an IPv6 literal with square braces. */
	if (p[0] == '[') {
		h++;
		if ((p = strchr(s, ']')) == NULL)
			goto done;
		*p++ = '\0';
	}

	/* Find the port seperator. */
	if ((p = strchr(p, ':')) == NULL)
		goto done;

	/* If there is another separator then we have issues. */
	if (strchr(p + 1, ':') != NULL)
		goto done;

	*p++ = '\0';

	if (asprintf(host, "%s", h) == -1)
		goto fail;
	if (asprintf(port, "%s", p) == -1)
		goto fail;

	rv = 0;
	goto done;

 fail:
	free(*host);
	*host = NULL;
	free(*port);
	*port = NULL;
	rv = -1;

 done:
	free(s);

	return (rv);
}

int
tls_password_cb(char *buf, int size, int rwflag, void *u)
{
	size_t len;

	if (size < 0)
		return (0);

	if (u == NULL) {
		memset(buf, 0, size);
		return (0);
	}

	if ((len = strlcpy(buf, u, size)) >= (size_t)size)
		return (0);

	return (len);
}

uint8_t *
tls_load_file(const char *name, size_t *len, char *password)
{
	FILE *fp;
	EVP_PKEY *key = NULL;
	BIO *bio = NULL;
	char *data;
	uint8_t *buf = NULL;
	struct stat st;
	size_t size = 0;
	int fd = -1;
	ssize_t n;

	*len = 0;

	if ((fd = open(name, O_RDONLY)) == -1)
		return (NULL);

	/* Just load the file into memory without decryption */
	if (password == NULL) {
		if (fstat(fd, &st) != 0)
			goto fail;
		if (st.st_size < 0)
			goto fail;
		size = (size_t)st.st_size;
		if ((buf = malloc(size)) == NULL)
			goto fail;
		n = read(fd, buf, size);
		if (n < 0 || (size_t)n != size)
			goto fail;
		close(fd);
		goto done;
	}

	/* Or read the (possibly) encrypted key from file */
	if ((fp = fdopen(fd, "r")) == NULL)
		goto fail;
	fd = -1;

	key = PEM_read_PrivateKey(fp, NULL, tls_password_cb, password);
	fclose(fp);
	if (key == NULL)
		goto fail;

	/* Write unencrypted key to memory buffer */
	if ((bio = BIO_new(BIO_s_mem())) == NULL)
		goto fail;
	if (!PEM_write_bio_PrivateKey(bio, key, NULL, NULL, 0, NULL, NULL))
		goto fail;
	if ((size = BIO_get_mem_data(bio, &data)) <= 0)
		goto fail;
	if ((buf = malloc(size)) == NULL)
		goto fail;
	memcpy(buf, data, size);

	BIO_free_all(bio);
	EVP_PKEY_free(key);

 done:
	*len = size;
	return (buf);

 fail:
	if (fd != -1)
		close(fd);
	freezero(buf, size);
	BIO_free_all(bio);
	EVP_PKEY_free(key);

	return (NULL);
}

void
tls_unload_file(uint8_t *buf, size_t len)
{
	freezero(buf, len);
}
