/* $OpenBSD: opensslv.h,v 1.43.4.1 2017/12/11 10:50:37 bcook Exp $ */
#ifndef HEADER_OPENSSLV_H
#define HEADER_OPENSSLV_H

/* These will change with each release of LibreSSL-portable */
#define LIBRESSL_VERSION_NUMBER	0x2060400fL
#define LIBRESSL_VERSION_TEXT	"LibreSSL 2.6.4"

/* These will never change */
//This breaks poco build: #define OPENSSL_VERSION_NUMBER	0x20000000L
#define OPENSSL_VERSION_NUMBER	0x10000000L

#define OPENSSL_VERSION_TEXT	LIBRESSL_VERSION_TEXT
#define OPENSSL_VERSION_PTEXT	" part of " OPENSSL_VERSION_TEXT

#define SHLIB_VERSION_HISTORY ""
#define SHLIB_VERSION_NUMBER "1.0.0"

#endif /* HEADER_OPENSSLV_H */
