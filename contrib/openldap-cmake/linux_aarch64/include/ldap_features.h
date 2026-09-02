/* include/ldap_features.h. Generated from ldap_features.hin by configure. */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2020 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

/*
 * LDAP Features
 */

#ifndef _LDAP_FEATURES_H
#define _LDAP_FEATURES_H 1

/* OpenLDAP API version macros */
#define LDAP_VENDOR_VERSION 20501
#define LDAP_VENDOR_VERSION_MAJOR 2
#define LDAP_VENDOR_VERSION_MINOR 5
#define LDAP_VENDOR_VERSION_PATCH X

/*
** WORK IN PROGRESS!
**
** OpenLDAP reentrancy/thread-safeness should be dynamically
** checked using ldap_get_option().
**
** The -lldap implementation is not thread-safe.
**
** The -lldap_r implementation is:
**     LDAP_API_FEATURE_THREAD_SAFE (basic thread safety)
** but also be:
**     LDAP_API_FEATURE_SESSION_THREAD_SAFE
**     LDAP_API_FEATURE_OPERATION_THREAD_SAFE
**
** The preprocessor flag LDAP_API_FEATURE_X_OPENLDAP_THREAD_SAFE
** can be used to determine if -lldap_r is available at compile
** time.  You must define LDAP_THREAD_SAFE if and only if you
** link with -lldap_r.
**
** If you fail to define LDAP_THREAD_SAFE when linking with
** -lldap_r or define LDAP_THREAD_SAFE when linking with -lldap,
** provided header definitions and declarations may be incorrect.
**
*/

/* is -lldap_r available or not */
#define LDAP_API_FEATURE_X_OPENLDAP_THREAD_SAFE 1

/* LDAP v2 Referrals */
/* #undef LDAP_API_FEATURE_X_OPENLDAP_V2_REFERRALS */

#endif /* LDAP_FEATURES */
