/* include/lber_types.h. Generated from lber_types.hin by configure. */
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
 * LBER types
 */

#ifndef _LBER_TYPES_H
#define _LBER_TYPES_H

#include <ldap_cdefs.h>

LDAP_BEGIN_DECL

/* LBER boolean, enum, integers (32 bits or larger) */
#define LBER_INT_T int

/* LBER tags (32 bits or larger) */
#define LBER_TAG_T long

/* LBER socket descriptor */
#define LBER_SOCKET_T int

/* LBER lengths (32 bits or larger) */
#define LBER_LEN_T long

/* ------------------------------------------------------------ */

/* booleans, enumerations, and integers */
typedef LBER_INT_T ber_int_t;

/* signed and unsigned versions */
typedef signed LBER_INT_T ber_sint_t;
typedef unsigned LBER_INT_T ber_uint_t;

/* tags */
typedef unsigned LBER_TAG_T ber_tag_t;

/* "socket" descriptors */
typedef LBER_SOCKET_T ber_socket_t;

/* lengths */
typedef unsigned LBER_LEN_T ber_len_t;

/* signed lengths */
typedef signed LBER_LEN_T ber_slen_t;

LDAP_END_DECL

#endif /* _LBER_TYPES_H */
