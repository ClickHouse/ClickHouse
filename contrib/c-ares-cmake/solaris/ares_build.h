/* include/ares_build.h.  Generated from ares_build.h.in by configure.  */
#ifndef __CARES_BUILD_H
#define __CARES_BUILD_H


/* Copyright (C) 2009 - 2021 by Daniel Stenberg et al
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose and without fee is hereby granted, provided
 * that the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of M.I.T. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  M.I.T. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 */

/* ================================================================ */
/*               NOTES FOR CONFIGURE CAPABLE SYSTEMS                */
/* ================================================================ */

/*
 * NOTE 1:
 * -------
 *
 * Nothing in this file is intended to be modified or adjusted by the
 * c-ares library user nor by the c-ares library builder.
 *
 * If you think that something actually needs to be changed, adjusted
 * or fixed in this file, then, report it on the c-ares development
 * mailing list: http://lists.haxx.se/listinfo/c-ares/
 *
 * This header file shall only export symbols which are 'cares' or 'CARES'
 * prefixed, otherwise public name space would be polluted.
 *
 * NOTE 2:
 * -------
 *
 * Right now you might be staring at file ares_build.h.in or ares_build.h,
 * this is due to the following reason:
 *
 * On systems capable of running the configure script, the configure process
 * will overwrite the distributed ares_build.h file with one that is suitable
 * and specific to the library being configured and built, which is generated
 * from the ares_build.h.in template file.
 *
 */

/* ================================================================ */
/*  DEFINITION OF THESE SYMBOLS SHALL NOT TAKE PLACE ANYWHERE ELSE  */
/* ================================================================ */

#ifdef CARES_TYPEOF_ARES_SOCKLEN_T
#  error "CARES_TYPEOF_ARES_SOCKLEN_T shall not be defined except in ares_build.h"
   Error Compilation_aborted_CARES_TYPEOF_ARES_SOCKLEN_T_already_defined
#endif

#define CARES_HAVE_ARPA_NAMESER_H 1
#define CARES_HAVE_ARPA_NAMESER_COMPAT_H 1

/* ================================================================ */
/*  EXTERNAL INTERFACE SETTINGS FOR CONFIGURE CAPABLE SYSTEMS ONLY  */
/* ================================================================ */

/* Configure process defines this to 1 when it finds out that system  */
/* header file ws2tcpip.h must be included by the external interface. */
/* #undef CARES_PULL_WS2TCPIP_H */
#ifdef CARES_PULL_WS2TCPIP_H
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#  include <winsock2.h>
#  include <ws2tcpip.h>
#endif

/* Configure process defines this to 1 when it finds out that system   */
/* header file sys/types.h must be included by the external interface. */
#define CARES_PULL_SYS_TYPES_H 1
#ifdef CARES_PULL_SYS_TYPES_H
#  include <sys/types.h>
#endif

/* Configure process defines this to 1 when it finds out that system    */
/* header file sys/socket.h must be included by the external interface. */
#define CARES_PULL_SYS_SOCKET_H 1
#ifdef CARES_PULL_SYS_SOCKET_H
#  include <sys/socket.h>
#endif

/* Integral data type used for ares_socklen_t. */
#define CARES_TYPEOF_ARES_SOCKLEN_T socklen_t

/* Data type definition of ares_socklen_t. */
typedef CARES_TYPEOF_ARES_SOCKLEN_T ares_socklen_t;

/* Integral data type used for ares_ssize_t. */
#define CARES_TYPEOF_ARES_SSIZE_T ssize_t

/* Data type definition of ares_ssize_t. */
typedef CARES_TYPEOF_ARES_SSIZE_T ares_ssize_t;

#endif /* __CARES_BUILD_H */
