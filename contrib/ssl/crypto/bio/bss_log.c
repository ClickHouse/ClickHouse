/* $OpenBSD: bss_log.c,v 1.21 2014/07/11 08:44:47 jsing Exp $ */
/* ====================================================================
 * Copyright (c) 1999 The OpenSSL Project.  All rights reserved.
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

/*
	Why BIO_s_log?

	BIO_s_log is useful for system daemons (or services under NT).
	It is one-way BIO, it sends all stuff to syslogd (on system that
	commonly use that), or event log (on NT), or OPCOM (on OpenVMS).

*/

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <syslog.h>

#include <openssl/buffer.h>
#include <openssl/err.h>

#ifndef NO_SYSLOG

static int slg_write(BIO *h, const char *buf, int num);
static int slg_puts(BIO *h, const char *str);
static long slg_ctrl(BIO *h, int cmd, long arg1, void *arg2);
static int slg_new(BIO *h);
static int slg_free(BIO *data);
static void xopenlog(BIO* bp, char* name, int level);
static void xsyslog(BIO* bp, int priority, const char* string);
static void xcloselog(BIO* bp);

static BIO_METHOD methods_slg = {
	.type = BIO_TYPE_MEM,
	.name = "syslog",
	.bwrite = slg_write,
	.bputs = slg_puts,
	.ctrl = slg_ctrl,
	.create = slg_new,
	.destroy = slg_free
};

BIO_METHOD *
BIO_s_log(void)
{
	return (&methods_slg);
}

static int
slg_new(BIO *bi)
{
	bi->init = 1;
	bi->num = 0;
	bi->ptr = NULL;
	xopenlog(bi, "application", LOG_DAEMON);
	return (1);
}

static int
slg_free(BIO *a)
{
	if (a == NULL)
		return (0);
	xcloselog(a);
	return (1);
}

static int
slg_write(BIO *b, const char *in, int inl)
{
	int ret = inl;
	char* buf;
	char* pp;
	int priority, i;
	static const struct {
		int strl;
		char str[10];
		int log_level;
	}
	mapping[] = {
		{ 6, "PANIC ", LOG_EMERG },
		{ 6, "EMERG ", LOG_EMERG },
		{ 4, "EMR ", LOG_EMERG },
		{ 6, "ALERT ", LOG_ALERT },
		{ 4, "ALR ", LOG_ALERT },
		{ 5, "CRIT ", LOG_CRIT },
		{ 4, "CRI ", LOG_CRIT },
		{ 6, "ERROR ", LOG_ERR },
		{ 4, "ERR ", LOG_ERR },
		{ 8, "WARNING ", LOG_WARNING },
		{ 5, "WARN ", LOG_WARNING },
		{ 4, "WAR ", LOG_WARNING },
		{ 7, "NOTICE ", LOG_NOTICE },
		{ 5, "NOTE ", LOG_NOTICE },
		{ 4, "NOT ", LOG_NOTICE },
		{ 5, "INFO ", LOG_INFO },
		{ 4, "INF ", LOG_INFO },
		{ 6, "DEBUG ", LOG_DEBUG },
		{ 4, "DBG ", LOG_DEBUG },
		{ 0, "", LOG_ERR } /* The default */
	};

	if ((buf = malloc(inl + 1)) == NULL) {
		return (0);
	}
	strlcpy(buf, in, inl + 1);
	i = 0;
	while (strncmp(buf, mapping[i].str, mapping[i].strl) != 0)
		i++;
	priority = mapping[i].log_level;
	pp = buf + mapping[i].strl;

	xsyslog(b, priority, pp);

	free(buf);
	return (ret);
}

static long
slg_ctrl(BIO *b, int cmd, long num, void *ptr)
{
	switch (cmd) {
	case BIO_CTRL_SET:
		xcloselog(b);
		xopenlog(b, ptr, num);
		break;
	default:
		break;
	}
	return (0);
}

static int
slg_puts(BIO *bp, const char *str)
{
	int n, ret;

	n = strlen(str);
	ret = slg_write(bp, str, n);
	return (ret);
}


static void
xopenlog(BIO* bp, char* name, int level)
{
	openlog(name, LOG_PID|LOG_CONS, level);
}

static void
xsyslog(BIO *bp, int priority, const char *string)
{
	syslog(priority, "%s", string);
}

static void
xcloselog(BIO* bp)
{
	closelog();
}

#endif /* NO_SYSLOG */
