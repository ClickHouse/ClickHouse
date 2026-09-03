/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 1990,1991,2008 by the Massachusetts Institute of Technology.
 * All Rights Reserved.
 *
 * Export of this software from the United States of America may
 *   require a specific license from the United States Government.
 *   It is the responsibility of any person or organization contemplating
 *   export to obtain such a license before exporting.
 *
 * WITHIN THAT CONSTRAINT, permission to use, copy, modify, and
 * distribute this software and its documentation for any purpose and
 * without fee is hereby granted, provided that the above copyright
 * notice appear in all copies and that both that copyright notice and
 * this permission notice appear in supporting documentation, and that
 * the name of M.I.T. not be used in advertising or publicity pertaining
 * to distribution of the software without specific, written prior
 * permission.  Furthermore if you modify this software you must label
 * your software as modified software and not distribute it in such a
 * fashion that it might be confused with the original M.I.T. software.
 * M.I.T. makes no representations about the suitability of
 * this software for any purpose.  It is provided "as is" without express
 * or implied warranty.
 */

/* Site- and OS- dependent configuration */

#ifndef KRB5_OSCONF__
#define KRB5_OSCONF__

#if !defined(_WIN32)
/* Don't try to pull in autoconf.h for Windows, since it's not used */
#ifndef KRB5_AUTOCONF__
#define KRB5_AUTOCONF__
#include "autoconf.h"
#endif
#endif

#if defined(__MACH__) && defined(__APPLE__)
# include <TargetConditionals.h>
#endif

#if defined(_WIN32)
#define DEFAULT_PROFILE_FILENAME "krb5.ini"
#else /* !_WINDOWS */
#if TARGET_OS_MAC
#define DEFAULT_SECURE_PROFILE_PATH "/Library/Preferences/edu.mit.Kerberos:/etc/krb5.conf:/usr/local/etc/krb5.conf"
#define DEFAULT_PROFILE_PATH        ("~/Library/Preferences/edu.mit.Kerberos" ":" DEFAULT_SECURE_PROFILE_PATH)
#define KRB5_PLUGIN_BUNDLE_DIR       "/System/Library/KerberosPlugins/KerberosFrameworkPlugins"
#define KDB5_PLUGIN_BUNDLE_DIR       "/System/Library/KerberosPlugins/KerberosDatabasePlugins"
#define KRB5_AUTHDATA_PLUGIN_BUNDLE_DIR  "/System/Library/KerberosPlugins/KerberosAuthDataPlugins"
#else
#define DEFAULT_SECURE_PROFILE_PATH     "/etc/krb5.conf:/usr/local/etc/krb5.conf"
#define DEFAULT_PROFILE_PATH        DEFAULT_SECURE_PROFILE_PATH
#endif
#endif /* _WINDOWS  */

#ifdef _WIN32
#define DEFAULT_PLUGIN_BASE_DIR "%{LIBDIR}\\plugins"
#else
#define DEFAULT_PLUGIN_BASE_DIR "/usr/local/lib/krb5/plugins"
#endif

#if defined(_WIN64)
#define PLUGIN_EXT              "64.dll"
#elif defined(_WIN32)
#define PLUGIN_EXT              "32.dll"
#else
#define PLUGIN_EXT              ".so"
#endif

#define KDC_DIR                 "/usr/local/var/krb5kdc"
#define KDC_RUN_DIR             "/run/krb5kdc"
#define DEFAULT_KDB_FILE        KDC_DIR "/principal"
#define DEFAULT_KEYFILE_STUB    KDC_DIR "/.k5."
#define KRB5_DEFAULT_ADMIN_ACL  KDC_DIR "/krb5_adm.acl"
/* Used by old admin server */
#define DEFAULT_ADMIN_ACL       KDC_DIR "/kadm_old.acl"

/* Location of KDC profile */
#define DEFAULT_KDC_PROFILE     KDC_DIR "/kdc.conf"
#define KDC_PROFILE_ENV         "KRB5_KDC_PROFILE"

#if TARGET_OS_MAC
#define DEFAULT_KDB_LIB_PATH    { KDB5_PLUGIN_BUNDLE_DIR, "/usr/local/lib/krb5/plugins/kdb", NULL }
#else
#define DEFAULT_KDB_LIB_PATH    { "/usr/local/lib/krb5/plugins/kdb", NULL }
#endif

#define DEFAULT_KDC_ENCTYPE     ENCTYPE_AES256_CTS_HMAC_SHA1_96
#define KDCRCACHE               "dfl:krb5kdc_rcache"

#define KDC_PORTNAME            "kerberos" /* for /etc/services or equiv. */

#define KRB5_DEFAULT_PORT       88

#define DEFAULT_KPASSWD_PORT    464

#define DEFAULT_KDC_UDP_PORTLIST "88"
#define DEFAULT_KDC_TCP_PORTLIST "88"
#define DEFAULT_TCP_LISTEN_BACKLOG 5

/*
 * Defaults for the KADM5 admin system.
 */
#define DEFAULT_KADM5_KEYTAB    KDC_DIR "/kadm5.keytab"
#define DEFAULT_KADM5_ACL_FILE  KDC_DIR "/kadm5.acl"
#define DEFAULT_KADM5_PORT      749 /* assigned by IANA */

#define KRB5_DEFAULT_SUPPORTED_ENCTYPES                 \
    "aes256-cts-hmac-sha1-96:normal "                   \
    "aes128-cts-hmac-sha1-96:normal"

#define MAX_DGRAM_SIZE  65536

#define RCTMPDIR        "/var/tmp" /* directory to store replay caches */

#define KRB5_PATH_TTY   "/dev/tty"
#define KRB5_PATH_LOGIN "/usr/local/sbin/login.krb5"
#define KRB5_PATH_RLOGIN "/usr/local/bin/rlogin"

#define KRB5_ENV_CCNAME "KRB5CCNAME"

/*
 * krb5 replica support follows
 */

#define KPROP_DEFAULT_FILE KDC_DIR "/replica_datatrans"
#define KPROPD_DEFAULT_FILE KDC_DIR "/from_master"
#define KPROPD_DEFAULT_KDB5_UTIL "/usr/local/sbin/kdb5_util"
#define KPROPD_DEFAULT_KPROP "/usr/local/sbin/kprop"
#define KPROPD_DEFAULT_KRB_DB DEFAULT_KDB_FILE
#define KPROPD_ACL_FILE KDC_DIR "/kpropd.acl"

/*
 * GSS mechglue
 */
#define MECH_CONF "/usr/local/etc/gss/mech"
#define MECH_LIB_PREFIX "/usr/local/lib/gss/"

#endif /* KRB5_OSCONF__ */
