/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1


/* Substitute the variable and function names.  */
#define yyparse         pcap_parse
#define yylex           pcap_lex
#define yyerror         pcap_error
#define yydebug         pcap_debug
#define yynerrs         pcap_nerrs

/* First part of user prologue.  */
#line 47 "grammar.y"

/*
 * Copyright (c) 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996
 *	The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that: (1) source code distributions
 * retain the above copyright notice and this paragraph in its entirety, (2)
 * distributions including binary code include the above copyright notice and
 * this paragraph in its entirety in the documentation or other materials
 * provided with the distribution, and (3) all advertising materials mentioning
 * features or use of this software display the following acknowledgement:
 * ``This product includes software developed by the University of California,
 * Lawrence Berkeley Laboratory and its contributors.'' Neither the name of
 * the University nor the names of its contributors may be used to endorse
 * or promote products derived from this software without specific prior
 * written permission.
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
 *
 */

#include <config.h>

/*
 * grammar.h requires gencode.h and sometimes breaks in a polluted namespace
 * (see ftmacros.h), so include it early.
 */
#include "gencode.h"
#include "grammar.h"

#include <stdlib.h>

#ifndef _WIN32
#include <sys/types.h>
#include <sys/socket.h>

#if __STDC__
struct mbuf;
struct rtentry;
#endif

#include <netinet/in.h>
#include <arpa/inet.h>
#endif /* _WIN32 */

#include <stdio.h>

#include "diag-control.h"

#include "pcap-int.h"

#include "scanner.h"

#include "llc.h"
#include "ieee80211.h"
#include "pflog.h"
#include <pcap/namedb.h>

#ifdef HAVE_OS_PROTO_H
#include "os-proto.h"
#endif

/*
 * Work around some bugs in Berkeley YACC prior to the 2017-07-09
 * release.
 *
 * The 2005-05-05 release was the first one to define YYPATCH, so
 * we treat any release that either 1) doesn't define YYPATCH or
 * 2) defines it to a value < 20170709 as being buggy.
 */
#if defined(YYBYACC) && (!defined(YYPATCH) || YYPATCH < 20170709)
/*
 * Both Berkeley YACC and Bison define yydebug (under whatever name
 * it has) as a global, but Bison does so only if YYDEBUG is defined.
 * Berkeley YACC, prior to the 2017-07-09 release, defines it even if
 * YYDEBUG isn't defined; declare it here to suppress a warning.  The
 * 2017-07-09 release fixes that.
 */
#if !defined(YYDEBUG)
extern int yydebug;
#endif

/*
 * In Berkeley YACC, prior to the 2017-07-09 release, yynerrs (under
 * whatever name it has) is global, even if it's building a reentrant
 * parser.  In Bison, and in the Berkeley YACC 2017-07-09 release and
 * later, it's local in reentrant parsers.
 *
 * Declare it to squelch a warning.
 */
extern int yynerrs;
#endif

#define QSET(q, p, d, a) (q).proto = (unsigned char)(p),\
			 (q).dir = (unsigned char)(d),\
			 (q).addr = (unsigned char)(a)

struct tok {
	int v;			/* value */
	const char *s;		/* string */
};

static const struct tok ieee80211_types[] = {
	{ IEEE80211_FC0_TYPE_DATA, "data" },
	{ IEEE80211_FC0_TYPE_MGT, "mgt" },
	{ IEEE80211_FC0_TYPE_MGT, "management" },
	{ IEEE80211_FC0_TYPE_CTL, "ctl" },
	{ IEEE80211_FC0_TYPE_CTL, "control" },
	{ 0, NULL }
};
static const struct tok ieee80211_mgt_subtypes[] = {
	{ IEEE80211_FC0_SUBTYPE_ASSOC_REQ, "assocreq" },
	{ IEEE80211_FC0_SUBTYPE_ASSOC_REQ, "assoc-req" },
	{ IEEE80211_FC0_SUBTYPE_ASSOC_RESP, "assocresp" },
	{ IEEE80211_FC0_SUBTYPE_ASSOC_RESP, "assoc-resp" },
	{ IEEE80211_FC0_SUBTYPE_REASSOC_REQ, "reassocreq" },
	{ IEEE80211_FC0_SUBTYPE_REASSOC_REQ, "reassoc-req" },
	{ IEEE80211_FC0_SUBTYPE_REASSOC_RESP, "reassocresp" },
	{ IEEE80211_FC0_SUBTYPE_REASSOC_RESP, "reassoc-resp" },
	{ IEEE80211_FC0_SUBTYPE_PROBE_REQ, "probereq" },
	{ IEEE80211_FC0_SUBTYPE_PROBE_REQ, "probe-req" },
	{ IEEE80211_FC0_SUBTYPE_PROBE_RESP, "proberesp" },
	{ IEEE80211_FC0_SUBTYPE_PROBE_RESP, "probe-resp" },
	{ IEEE80211_FC0_SUBTYPE_BEACON, "beacon" },
	{ IEEE80211_FC0_SUBTYPE_ATIM, "atim" },
	{ IEEE80211_FC0_SUBTYPE_DISASSOC, "disassoc" },
	{ IEEE80211_FC0_SUBTYPE_DISASSOC, "disassociation" },
	{ IEEE80211_FC0_SUBTYPE_AUTH, "auth" },
	{ IEEE80211_FC0_SUBTYPE_AUTH, "authentication" },
	{ IEEE80211_FC0_SUBTYPE_DEAUTH, "deauth" },
	{ IEEE80211_FC0_SUBTYPE_DEAUTH, "deauthentication" },
	{ 0, NULL }
};
static const struct tok ieee80211_ctl_subtypes[] = {
	{ IEEE80211_FC0_SUBTYPE_PS_POLL, "ps-poll" },
	{ IEEE80211_FC0_SUBTYPE_RTS, "rts" },
	{ IEEE80211_FC0_SUBTYPE_CTS, "cts" },
	{ IEEE80211_FC0_SUBTYPE_ACK, "ack" },
	{ IEEE80211_FC0_SUBTYPE_CF_END, "cf-end" },
	{ IEEE80211_FC0_SUBTYPE_CF_END_ACK, "cf-end-ack" },
	{ 0, NULL }
};
static const struct tok ieee80211_data_subtypes[] = {
	{ IEEE80211_FC0_SUBTYPE_DATA, "data" },
	{ IEEE80211_FC0_SUBTYPE_CF_ACK, "data-cf-ack" },
	{ IEEE80211_FC0_SUBTYPE_CF_POLL, "data-cf-poll" },
	{ IEEE80211_FC0_SUBTYPE_CF_ACPL, "data-cf-ack-poll" },
	{ IEEE80211_FC0_SUBTYPE_NODATA, "null" },
	{ IEEE80211_FC0_SUBTYPE_NODATA_CF_ACK, "cf-ack" },
	{ IEEE80211_FC0_SUBTYPE_NODATA_CF_POLL, "cf-poll"  },
	{ IEEE80211_FC0_SUBTYPE_NODATA_CF_ACPL, "cf-ack-poll" },
	{ IEEE80211_FC0_SUBTYPE_QOS|IEEE80211_FC0_SUBTYPE_DATA, "qos-data" },
	{ IEEE80211_FC0_SUBTYPE_QOS|IEEE80211_FC0_SUBTYPE_CF_ACK, "qos-data-cf-ack" },
	{ IEEE80211_FC0_SUBTYPE_QOS|IEEE80211_FC0_SUBTYPE_CF_POLL, "qos-data-cf-poll" },
	{ IEEE80211_FC0_SUBTYPE_QOS|IEEE80211_FC0_SUBTYPE_CF_ACPL, "qos-data-cf-ack-poll" },
	{ IEEE80211_FC0_SUBTYPE_QOS|IEEE80211_FC0_SUBTYPE_NODATA, "qos" },
	{ IEEE80211_FC0_SUBTYPE_QOS|IEEE80211_FC0_SUBTYPE_NODATA_CF_POLL, "qos-cf-poll" },
	{ IEEE80211_FC0_SUBTYPE_QOS|IEEE80211_FC0_SUBTYPE_NODATA_CF_ACPL, "qos-cf-ack-poll" },
	{ 0, NULL }
};
static const struct tok llc_s_subtypes[] = {
	{ LLC_RR, "rr" },
	{ LLC_RNR, "rnr" },
	{ LLC_REJ, "rej" },
	{ 0, NULL }
};
static const struct tok llc_u_subtypes[] = {
	{ LLC_UI, "ui" },
	{ LLC_UA, "ua" },
	{ LLC_DISC, "disc" },
	{ LLC_DM, "dm" },
	{ LLC_SABME, "sabme" },
	{ LLC_TEST, "test" },
	{ LLC_XID, "xid" },
	{ LLC_FRMR, "frmr" },
	{ 0, NULL }
};
struct type2tok {
	int type;
	const struct tok *tok;
};
static const struct type2tok ieee80211_type_subtypes[] = {
	{ IEEE80211_FC0_TYPE_MGT, ieee80211_mgt_subtypes },
	{ IEEE80211_FC0_TYPE_CTL, ieee80211_ctl_subtypes },
	{ IEEE80211_FC0_TYPE_DATA, ieee80211_data_subtypes },
	{ 0, NULL }
};

static int
str2tok(const char *str, const struct tok *toks)
{
	int i;

	for (i = 0; toks[i].s != NULL; i++) {
		if (pcapint_strcasecmp(toks[i].s, str) == 0) {
			/*
			 * Just in case somebody is using this to
			 * generate values of -1/0xFFFFFFFF.
			 * That won't work, as it's indistinguishable
			 * from an error.
			 */
			if (toks[i].v == -1)
				abort();
			return (toks[i].v);
		}
	}
	return (-1);
}

static const struct qual qerr = { Q_UNDEF, Q_UNDEF, Q_UNDEF, Q_UNDEF };

static void
yyerror(void *yyscanner _U_, compiler_state_t *cstate, const char *msg)
{
	bpf_set_error(cstate, "can't parse filter expression: %s", msg);
}

static const struct tok pflog_reasons[] = {
	{ PFRES_MATCH,		"match" },
	{ PFRES_BADOFF,		"bad-offset" },
	{ PFRES_FRAG,		"fragment" },
	{ PFRES_SHORT,		"short" },
	{ PFRES_NORM,		"normalize" },
	{ PFRES_MEMORY,		"memory" },
	{ PFRES_TS,		"bad-timestamp" },
	{ PFRES_CONGEST,	"congestion" },
	{ PFRES_IPOPTIONS,	"ip-option" },
	{ PFRES_PROTCKSUM,	"proto-cksum" },
	{ PFRES_BADSTATE,	"state-mismatch" },
	{ PFRES_STATEINS,	"state-insert" },
	{ PFRES_MAXSTATES,	"state-limit" },
	{ PFRES_SRCLIMIT,	"src-limit" },
	{ PFRES_SYNPROXY,	"synproxy" },
#if defined(__FreeBSD__)
	{ PFRES_MAPFAILED,	"map-failed" },
#elif defined(__NetBSD__)
	{ PFRES_STATELOCKED,	"state-locked" },
#elif defined(__OpenBSD__)
	{ PFRES_TRANSLATE,	"translate" },
	{ PFRES_NOROUTE,	"no-route" },
#elif defined(__APPLE__)
	{ PFRES_DUMMYNET,	"dummynet" },
#endif
	{ 0, NULL }
};

static int
pfreason_to_num(compiler_state_t *cstate, const char *reason)
{
	int i;

	i = str2tok(reason, pflog_reasons);
	if (i == -1)
		bpf_set_error(cstate, "unknown PF reason \"%s\"", reason);
	return (i);
}

static const struct tok pflog_actions[] = {
	{ PF_PASS,		"pass" },
	{ PF_PASS,		"accept" },	/* alias for "pass" */
	{ PF_DROP,		"drop" },
	{ PF_DROP,		"block" },	/* alias for "drop" */
	{ PF_SCRUB,		"scrub" },
	{ PF_NOSCRUB,		"noscrub" },
	{ PF_NAT,		"nat" },
	{ PF_NONAT,		"nonat" },
	{ PF_BINAT,		"binat" },
	{ PF_NOBINAT,		"nobinat" },
	{ PF_RDR,		"rdr" },
	{ PF_NORDR,		"nordr" },
	{ PF_SYNPROXY_DROP,	"synproxy-drop" },
#if defined(__FreeBSD__)
	{ PF_DEFER,		"defer" },
#elif defined(__OpenBSD__)
	{ PF_DEFER,		"defer" },
	{ PF_MATCH,		"match" },
	{ PF_DIVERT,		"divert" },
	{ PF_RT,		"rt" },
	{ PF_AFRT,		"afrt" },
#elif defined(__APPLE__)
	{ PF_DUMMYNET,		"dummynet" },
	{ PF_NODUMMYNET,	"nodummynet" },
	{ PF_NAT64,		"nat64" },
	{ PF_NONAT64,		"nonat64" },
#endif
	{ 0, NULL },
};

static int
pfaction_to_num(compiler_state_t *cstate, const char *action)
{
	int i;

	i = str2tok(action, pflog_actions);
	if (i == -1)
		bpf_set_error(cstate, "unknown PF action \"%s\"", action);
	return (i);
}

/*
 * For calls that might return an "an error occurred" value.
 */
#define CHECK_INT_VAL(val)	if (val == -1) YYABORT
#define CHECK_PTR_VAL(val)	if (val == NULL) YYABORT

DIAG_OFF_BISON_BYACC

#line 386 "grammar.c"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "grammar.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_DST = 3,                        /* DST  */
  YYSYMBOL_SRC = 4,                        /* SRC  */
  YYSYMBOL_HOST = 5,                       /* HOST  */
  YYSYMBOL_GATEWAY = 6,                    /* GATEWAY  */
  YYSYMBOL_NET = 7,                        /* NET  */
  YYSYMBOL_NETMASK = 8,                    /* NETMASK  */
  YYSYMBOL_PORT = 9,                       /* PORT  */
  YYSYMBOL_PORTRANGE = 10,                 /* PORTRANGE  */
  YYSYMBOL_LESS = 11,                      /* LESS  */
  YYSYMBOL_GREATER = 12,                   /* GREATER  */
  YYSYMBOL_PROTO = 13,                     /* PROTO  */
  YYSYMBOL_PROTOCHAIN = 14,                /* PROTOCHAIN  */
  YYSYMBOL_CBYTE = 15,                     /* CBYTE  */
  YYSYMBOL_ARP = 16,                       /* ARP  */
  YYSYMBOL_RARP = 17,                      /* RARP  */
  YYSYMBOL_IP = 18,                        /* IP  */
  YYSYMBOL_SCTP = 19,                      /* SCTP  */
  YYSYMBOL_TCP = 20,                       /* TCP  */
  YYSYMBOL_UDP = 21,                       /* UDP  */
  YYSYMBOL_ICMP = 22,                      /* ICMP  */
  YYSYMBOL_IGMP = 23,                      /* IGMP  */
  YYSYMBOL_IGRP = 24,                      /* IGRP  */
  YYSYMBOL_PIM = 25,                       /* PIM  */
  YYSYMBOL_VRRP = 26,                      /* VRRP  */
  YYSYMBOL_CARP = 27,                      /* CARP  */
  YYSYMBOL_ATALK = 28,                     /* ATALK  */
  YYSYMBOL_AARP = 29,                      /* AARP  */
  YYSYMBOL_DECNET = 30,                    /* DECNET  */
  YYSYMBOL_LAT = 31,                       /* LAT  */
  YYSYMBOL_SCA = 32,                       /* SCA  */
  YYSYMBOL_MOPRC = 33,                     /* MOPRC  */
  YYSYMBOL_MOPDL = 34,                     /* MOPDL  */
  YYSYMBOL_TK_BROADCAST = 35,              /* TK_BROADCAST  */
  YYSYMBOL_TK_MULTICAST = 36,              /* TK_MULTICAST  */
  YYSYMBOL_NUM = 37,                       /* NUM  */
  YYSYMBOL_INBOUND = 38,                   /* INBOUND  */
  YYSYMBOL_OUTBOUND = 39,                  /* OUTBOUND  */
  YYSYMBOL_IFINDEX = 40,                   /* IFINDEX  */
  YYSYMBOL_PF_IFNAME = 41,                 /* PF_IFNAME  */
  YYSYMBOL_PF_RSET = 42,                   /* PF_RSET  */
  YYSYMBOL_PF_RNR = 43,                    /* PF_RNR  */
  YYSYMBOL_PF_SRNR = 44,                   /* PF_SRNR  */
  YYSYMBOL_PF_REASON = 45,                 /* PF_REASON  */
  YYSYMBOL_PF_ACTION = 46,                 /* PF_ACTION  */
  YYSYMBOL_TYPE = 47,                      /* TYPE  */
  YYSYMBOL_SUBTYPE = 48,                   /* SUBTYPE  */
  YYSYMBOL_DIR = 49,                       /* DIR  */
  YYSYMBOL_ADDR1 = 50,                     /* ADDR1  */
  YYSYMBOL_ADDR2 = 51,                     /* ADDR2  */
  YYSYMBOL_ADDR3 = 52,                     /* ADDR3  */
  YYSYMBOL_ADDR4 = 53,                     /* ADDR4  */
  YYSYMBOL_RA = 54,                        /* RA  */
  YYSYMBOL_TA = 55,                        /* TA  */
  YYSYMBOL_LINK = 56,                      /* LINK  */
  YYSYMBOL_GEQ = 57,                       /* GEQ  */
  YYSYMBOL_LEQ = 58,                       /* LEQ  */
  YYSYMBOL_NEQ = 59,                       /* NEQ  */
  YYSYMBOL_ID = 60,                        /* ID  */
  YYSYMBOL_EID = 61,                       /* EID  */
  YYSYMBOL_HID = 62,                       /* HID  */
  YYSYMBOL_HID6 = 63,                      /* HID6  */
  YYSYMBOL_AID = 64,                       /* AID  */
  YYSYMBOL_LSH = 65,                       /* LSH  */
  YYSYMBOL_RSH = 66,                       /* RSH  */
  YYSYMBOL_LEN = 67,                       /* LEN  */
  YYSYMBOL_IPV6 = 68,                      /* IPV6  */
  YYSYMBOL_ICMPV6 = 69,                    /* ICMPV6  */
  YYSYMBOL_AH = 70,                        /* AH  */
  YYSYMBOL_ESP = 71,                       /* ESP  */
  YYSYMBOL_VLAN = 72,                      /* VLAN  */
  YYSYMBOL_MPLS = 73,                      /* MPLS  */
  YYSYMBOL_PPPOED = 74,                    /* PPPOED  */
  YYSYMBOL_PPPOES = 75,                    /* PPPOES  */
  YYSYMBOL_GENEVE = 76,                    /* GENEVE  */
  YYSYMBOL_ISO = 77,                       /* ISO  */
  YYSYMBOL_ESIS = 78,                      /* ESIS  */
  YYSYMBOL_CLNP = 79,                      /* CLNP  */
  YYSYMBOL_ISIS = 80,                      /* ISIS  */
  YYSYMBOL_L1 = 81,                        /* L1  */
  YYSYMBOL_L2 = 82,                        /* L2  */
  YYSYMBOL_IIH = 83,                       /* IIH  */
  YYSYMBOL_LSP = 84,                       /* LSP  */
  YYSYMBOL_SNP = 85,                       /* SNP  */
  YYSYMBOL_CSNP = 86,                      /* CSNP  */
  YYSYMBOL_PSNP = 87,                      /* PSNP  */
  YYSYMBOL_STP = 88,                       /* STP  */
  YYSYMBOL_IPX = 89,                       /* IPX  */
  YYSYMBOL_NETBEUI = 90,                   /* NETBEUI  */
  YYSYMBOL_LANE = 91,                      /* LANE  */
  YYSYMBOL_LLC = 92,                       /* LLC  */
  YYSYMBOL_METAC = 93,                     /* METAC  */
  YYSYMBOL_BCC = 94,                       /* BCC  */
  YYSYMBOL_SC = 95,                        /* SC  */
  YYSYMBOL_ILMIC = 96,                     /* ILMIC  */
  YYSYMBOL_OAMF4EC = 97,                   /* OAMF4EC  */
  YYSYMBOL_OAMF4SC = 98,                   /* OAMF4SC  */
  YYSYMBOL_OAM = 99,                       /* OAM  */
  YYSYMBOL_OAMF4 = 100,                    /* OAMF4  */
  YYSYMBOL_CONNECTMSG = 101,               /* CONNECTMSG  */
  YYSYMBOL_METACONNECT = 102,              /* METACONNECT  */
  YYSYMBOL_VPI = 103,                      /* VPI  */
  YYSYMBOL_VCI = 104,                      /* VCI  */
  YYSYMBOL_RADIO = 105,                    /* RADIO  */
  YYSYMBOL_FISU = 106,                     /* FISU  */
  YYSYMBOL_LSSU = 107,                     /* LSSU  */
  YYSYMBOL_MSU = 108,                      /* MSU  */
  YYSYMBOL_HFISU = 109,                    /* HFISU  */
  YYSYMBOL_HLSSU = 110,                    /* HLSSU  */
  YYSYMBOL_HMSU = 111,                     /* HMSU  */
  YYSYMBOL_SIO = 112,                      /* SIO  */
  YYSYMBOL_OPC = 113,                      /* OPC  */
  YYSYMBOL_DPC = 114,                      /* DPC  */
  YYSYMBOL_SLS = 115,                      /* SLS  */
  YYSYMBOL_HSIO = 116,                     /* HSIO  */
  YYSYMBOL_HOPC = 117,                     /* HOPC  */
  YYSYMBOL_HDPC = 118,                     /* HDPC  */
  YYSYMBOL_HSLS = 119,                     /* HSLS  */
  YYSYMBOL_LEX_ERROR = 120,                /* LEX_ERROR  */
  YYSYMBOL_OR = 121,                       /* OR  */
  YYSYMBOL_AND = 122,                      /* AND  */
  YYSYMBOL_123_ = 123,                     /* '!'  */
  YYSYMBOL_124_ = 124,                     /* '|'  */
  YYSYMBOL_125_ = 125,                     /* '&'  */
  YYSYMBOL_126_ = 126,                     /* '+'  */
  YYSYMBOL_127_ = 127,                     /* '-'  */
  YYSYMBOL_128_ = 128,                     /* '*'  */
  YYSYMBOL_129_ = 129,                     /* '/'  */
  YYSYMBOL_UMINUS = 130,                   /* UMINUS  */
  YYSYMBOL_131_ = 131,                     /* ')'  */
  YYSYMBOL_132_ = 132,                     /* '('  */
  YYSYMBOL_133_ = 133,                     /* '>'  */
  YYSYMBOL_134_ = 134,                     /* '='  */
  YYSYMBOL_135_ = 135,                     /* '<'  */
  YYSYMBOL_136_ = 136,                     /* '['  */
  YYSYMBOL_137_ = 137,                     /* ']'  */
  YYSYMBOL_138_ = 138,                     /* ':'  */
  YYSYMBOL_139_ = 139,                     /* '%'  */
  YYSYMBOL_140_ = 140,                     /* '^'  */
  YYSYMBOL_YYACCEPT = 141,                 /* $accept  */
  YYSYMBOL_prog = 142,                     /* prog  */
  YYSYMBOL_null = 143,                     /* null  */
  YYSYMBOL_expr = 144,                     /* expr  */
  YYSYMBOL_and = 145,                      /* and  */
  YYSYMBOL_or = 146,                       /* or  */
  YYSYMBOL_id = 147,                       /* id  */
  YYSYMBOL_nid = 148,                      /* nid  */
  YYSYMBOL_not = 149,                      /* not  */
  YYSYMBOL_paren = 150,                    /* paren  */
  YYSYMBOL_pid = 151,                      /* pid  */
  YYSYMBOL_qid = 152,                      /* qid  */
  YYSYMBOL_term = 153,                     /* term  */
  YYSYMBOL_head = 154,                     /* head  */
  YYSYMBOL_rterm = 155,                    /* rterm  */
  YYSYMBOL_pqual = 156,                    /* pqual  */
  YYSYMBOL_dqual = 157,                    /* dqual  */
  YYSYMBOL_aqual = 158,                    /* aqual  */
  YYSYMBOL_ndaqual = 159,                  /* ndaqual  */
  YYSYMBOL_pname = 160,                    /* pname  */
  YYSYMBOL_other = 161,                    /* other  */
  YYSYMBOL_pfvar = 162,                    /* pfvar  */
  YYSYMBOL_p80211 = 163,                   /* p80211  */
  YYSYMBOL_type = 164,                     /* type  */
  YYSYMBOL_subtype = 165,                  /* subtype  */
  YYSYMBOL_type_subtype = 166,             /* type_subtype  */
  YYSYMBOL_pllc = 167,                     /* pllc  */
  YYSYMBOL_dir = 168,                      /* dir  */
  YYSYMBOL_reason = 169,                   /* reason  */
  YYSYMBOL_action = 170,                   /* action  */
  YYSYMBOL_relop = 171,                    /* relop  */
  YYSYMBOL_irelop = 172,                   /* irelop  */
  YYSYMBOL_arth = 173,                     /* arth  */
  YYSYMBOL_narth = 174,                    /* narth  */
  YYSYMBOL_byteop = 175,                   /* byteop  */
  YYSYMBOL_pnum = 176,                     /* pnum  */
  YYSYMBOL_atmtype = 177,                  /* atmtype  */
  YYSYMBOL_atmmultitype = 178,             /* atmmultitype  */
  YYSYMBOL_atmfield = 179,                 /* atmfield  */
  YYSYMBOL_atmvalue = 180,                 /* atmvalue  */
  YYSYMBOL_atmfieldvalue = 181,            /* atmfieldvalue  */
  YYSYMBOL_atmlistvalue = 182,             /* atmlistvalue  */
  YYSYMBOL_mtp2type = 183,                 /* mtp2type  */
  YYSYMBOL_mtp3field = 184,                /* mtp3field  */
  YYSYMBOL_mtp3value = 185,                /* mtp3value  */
  YYSYMBOL_mtp3fieldvalue = 186,           /* mtp3fieldvalue  */
  YYSYMBOL_mtp3listvalue = 187             /* mtp3listvalue  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_int16 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if !defined yyoverflow

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* !defined yyoverflow */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  3
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   800

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  141
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  47
/* YYNRULES -- Number of rules.  */
#define YYNRULES  221
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  296

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   378


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   123,     2,     2,     2,   139,   125,     2,
     132,   131,   128,   126,     2,   127,     2,   129,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,   138,     2,
     135,   134,   133,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,   136,     2,   137,   140,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,   124,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   130
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   431,   431,   443,   445,   447,   448,   449,   450,   451,
     453,   455,   457,   458,   460,   462,   463,   482,   501,   520,
     545,   570,   571,   572,   574,   576,   578,   579,   580,   582,
     584,   586,   587,   589,   590,   591,   592,   593,   601,   603,
     604,   605,   606,   608,   610,   611,   612,   613,   614,   615,
     618,   619,   622,   623,   624,   625,   626,   627,   628,   629,
     630,   631,   632,   633,   636,   637,   638,   639,   642,   644,
     645,   646,   647,   648,   649,   650,   651,   652,   653,   654,
     655,   656,   657,   658,   659,   660,   661,   662,   663,   664,
     665,   666,   667,   668,   669,   670,   671,   672,   673,   674,
     675,   676,   677,   678,   679,   680,   681,   682,   684,   685,
     686,   687,   688,   689,   690,   691,   692,   693,   694,   695,
     696,   697,   698,   699,   700,   701,   702,   703,   706,   707,
     708,   709,   710,   711,   714,   719,   722,   726,   729,   735,
     744,   750,   773,   790,   791,   815,   818,   819,   835,   836,
     839,   842,   843,   844,   846,   847,   848,   850,   851,   853,
     854,   855,   856,   857,   858,   859,   860,   861,   862,   863,
     864,   865,   866,   867,   869,   870,   871,   872,   873,   875,
     876,   878,   879,   880,   881,   882,   883,   884,   886,   887,
     888,   889,   892,   893,   895,   896,   897,   898,   900,   907,
     908,   911,   912,   913,   914,   915,   916,   919,   920,   921,
     922,   923,   924,   925,   926,   928,   929,   930,   931,   933,
     946,   947
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if YYDEBUG || 0
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "DST", "SRC", "HOST",
  "GATEWAY", "NET", "NETMASK", "PORT", "PORTRANGE", "LESS", "GREATER",
  "PROTO", "PROTOCHAIN", "CBYTE", "ARP", "RARP", "IP", "SCTP", "TCP",
  "UDP", "ICMP", "IGMP", "IGRP", "PIM", "VRRP", "CARP", "ATALK", "AARP",
  "DECNET", "LAT", "SCA", "MOPRC", "MOPDL", "TK_BROADCAST", "TK_MULTICAST",
  "NUM", "INBOUND", "OUTBOUND", "IFINDEX", "PF_IFNAME", "PF_RSET",
  "PF_RNR", "PF_SRNR", "PF_REASON", "PF_ACTION", "TYPE", "SUBTYPE", "DIR",
  "ADDR1", "ADDR2", "ADDR3", "ADDR4", "RA", "TA", "LINK", "GEQ", "LEQ",
  "NEQ", "ID", "EID", "HID", "HID6", "AID", "LSH", "RSH", "LEN", "IPV6",
  "ICMPV6", "AH", "ESP", "VLAN", "MPLS", "PPPOED", "PPPOES", "GENEVE",
  "ISO", "ESIS", "CLNP", "ISIS", "L1", "L2", "IIH", "LSP", "SNP", "CSNP",
  "PSNP", "STP", "IPX", "NETBEUI", "LANE", "LLC", "METAC", "BCC", "SC",
  "ILMIC", "OAMF4EC", "OAMF4SC", "OAM", "OAMF4", "CONNECTMSG",
  "METACONNECT", "VPI", "VCI", "RADIO", "FISU", "LSSU", "MSU", "HFISU",
  "HLSSU", "HMSU", "SIO", "OPC", "DPC", "SLS", "HSIO", "HOPC", "HDPC",
  "HSLS", "LEX_ERROR", "OR", "AND", "'!'", "'|'", "'&'", "'+'", "'-'",
  "'*'", "'/'", "UMINUS", "')'", "'('", "'>'", "'='", "'<'", "'['", "']'",
  "':'", "'%'", "'^'", "$accept", "prog", "null", "expr", "and", "or",
  "id", "nid", "not", "paren", "pid", "qid", "term", "head", "rterm",
  "pqual", "dqual", "aqual", "ndaqual", "pname", "other", "pfvar",
  "p80211", "type", "subtype", "type_subtype", "pllc", "dir", "reason",
  "action", "relop", "irelop", "arth", "narth", "byteop", "pnum",
  "atmtype", "atmmultitype", "atmfield", "atmvalue", "atmfieldvalue",
  "atmlistvalue", "mtp2type", "mtp3field", "mtp3value", "mtp3fieldvalue",
  "mtp3listvalue", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-217)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-42)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
    -217,    28,   223,  -217,    13,    18,    21,  -217,  -217,  -217,
    -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,
    -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,    41,
     -30,    24,    51,    79,   -25,    26,  -217,  -217,  -217,  -217,
    -217,  -217,   -24,   -24,  -217,   -24,   -24,  -217,  -217,  -217,
    -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,
    -217,  -217,   -23,  -217,  -217,  -217,  -217,  -217,  -217,  -217,
    -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,
    -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,
    -217,   576,  -217,   -50,   459,   459,  -217,    19,  -217,   745,
       3,  -217,  -217,  -217,   558,  -217,  -217,  -217,  -217,    -5,
    -217,    39,  -217,  -217,   -14,  -217,  -217,  -217,  -217,  -217,
    -217,  -217,  -217,  -217,  -217,   -24,  -217,  -217,  -217,  -217,
    -217,  -217,   576,  -103,   -49,  -217,  -217,   341,   341,  -217,
    -100,     2,    12,  -217,  -217,    -7,    -3,  -217,  -217,  -217,
      19,    19,  -217,    -4,    31,  -217,  -217,  -217,  -217,  -217,
    -217,  -217,  -217,  -217,   -22,    78,   -18,  -217,  -217,  -217,
    -217,  -217,  -217,    60,  -217,  -217,  -217,   576,  -217,  -217,
    -217,   576,   576,   576,   576,   576,   576,   576,   576,  -217,
    -217,  -217,   576,   576,   576,   576,  -217,   125,   126,   127,
    -217,  -217,  -217,   132,   133,   144,  -217,  -217,  -217,  -217,
    -217,  -217,  -217,   145,    12,   602,  -217,   341,   341,  -217,
      10,  -217,  -217,  -217,  -217,  -217,   123,   149,   150,  -217,
    -217,    63,   -50,    12,   191,   192,   194,   195,  -217,  -217,
     151,  -217,  -217,  -217,  -217,  -217,  -217,   585,    64,    64,
     607,    49,   -66,   -66,   -49,   -49,   602,   602,   602,   602,
    -217,   -97,  -217,  -217,  -217,   -92,  -217,  -217,  -217,   -95,
    -217,  -217,  -217,  -217,    19,    19,  -217,  -217,  -217,  -217,
     -12,  -217,   163,  -217,   125,  -217,   132,  -217,  -217,  -217,
    -217,  -217,    65,  -217,  -217,  -217
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       4,     0,    51,     1,     0,     0,     0,    71,    72,    70,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,    85,    86,    88,    87,   179,   113,   114,     0,
       0,     0,     0,     0,     0,     0,    69,   173,    89,    90,
      91,    92,   117,   119,   120,   122,   124,    93,    94,   103,
      95,    96,    97,    98,    99,   100,   102,   101,   104,   105,
     106,   181,   143,   182,   183,   186,   187,   184,   185,   188,
     189,   190,   191,   192,   193,   107,   201,   202,   203,   204,
     205,   206,   207,   208,   209,   210,   211,   212,   213,   214,
      24,     0,    25,     2,    51,    51,     5,     0,    31,     0,
      50,    44,   125,   127,     0,   158,   157,    45,    46,     0,
      48,     0,   110,   111,     0,   115,   128,   129,   130,   131,
     148,   149,   132,   150,   133,     0,   116,   118,   121,   123,
     145,   144,     0,     0,   171,    11,    10,    51,    51,    32,
       0,   158,   157,    15,    21,    18,    20,    22,    39,    12,
       0,     0,    13,    53,    52,    64,    68,    65,    66,    67,
      36,    37,   108,   109,     0,     0,     0,    58,    59,    60,
      61,    62,    63,    34,    35,    38,   126,     0,   152,   154,
     156,     0,     0,     0,     0,     0,     0,     0,     0,   151,
     153,   155,     0,     0,     0,     0,   198,     0,     0,     0,
      47,   194,   219,     0,     0,     0,    49,   215,   175,   174,
     177,   178,   176,     0,     0,     0,     7,    51,    51,     6,
     157,     9,     8,    40,   172,   180,     0,     0,     0,    23,
      26,    30,     0,    29,     0,     0,     0,     0,   138,   139,
     135,   142,   136,   146,   147,   137,    33,     0,   169,   170,
     167,   166,   161,   162,   163,   164,   165,   168,    42,    43,
     199,     0,   195,   196,   220,     0,   216,   217,   112,   157,
      17,    16,    19,    14,     0,     0,    55,    57,    54,    56,
       0,   159,     0,   197,     0,   218,     0,    27,    28,   140,
     141,   134,     0,   200,   221,   160
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -217,  -217,  -217,   199,   -26,  -216,   -91,  -133,     7,    -2,
    -217,  -217,   -77,  -217,  -217,  -217,  -217,    32,  -217,     9,
    -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,  -217,
     -43,   -34,   -27,   -81,  -217,   -38,  -217,  -217,  -217,  -217,
    -195,  -217,  -217,  -217,  -217,  -180,  -217
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
       0,     1,     2,   140,   137,   138,   229,   149,   150,   132,
     231,   232,    96,    97,    98,    99,   173,   174,   175,   133,
     101,   102,   176,   240,   291,   242,   103,   245,   122,   124,
     194,   195,   104,   105,   213,   106,   107,   108,   109,   200,
     201,   261,   110,   111,   206,   207,   265
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      95,   226,   260,   -41,   126,   127,   148,   128,   129,    94,
     -13,   100,   120,    26,   141,   238,   275,   139,   230,   243,
     130,   135,   136,   264,   135,   289,   -29,   -29,     3,   135,
     116,   223,   196,   177,   283,   121,   225,   131,   239,   285,
     125,   125,   244,   125,   125,   284,   216,   221,   290,   286,
     112,   141,   178,   179,   180,   113,    26,   142,   114,   152,
     219,   222,   187,   188,   134,   155,   198,   157,   204,   158,
     159,   135,   136,   192,   193,   199,   202,   205,   115,   143,
     144,   145,   146,   147,   117,   230,   123,   214,   118,   293,
     192,   193,    95,    95,   142,   151,   178,   179,   180,   220,
     220,    94,    94,   100,   100,   215,   294,   197,    92,   203,
     208,   209,   152,   233,   181,   182,   119,   234,   235,   210,
     211,   212,   227,   125,   -41,   -41,   228,    92,   189,   190,
     191,   -13,   -13,   224,   -41,   218,   218,   141,   241,   177,
     139,   -13,    90,   225,   217,   217,   100,   100,   151,   125,
     247,    92,   236,   237,   248,   249,   250,   251,   252,   253,
     254,   255,   196,   262,   263,   256,   257,   258,   259,   202,
     266,    92,   189,   190,   191,   185,   186,   187,   188,   220,
     269,   267,   268,   287,   288,   270,   271,   272,   192,   193,
     185,   186,   187,   188,   273,   276,   277,   278,   279,   280,
     292,    93,   295,   192,   193,   246,   274,     0,     0,     0,
       0,     0,     0,     0,     0,   218,    95,     0,     0,     0,
       0,     0,     0,    -3,   217,   217,   100,   100,     0,     0,
       0,     0,     0,     0,     4,     5,   152,   152,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    19,    20,    21,    22,    23,    24,    25,     0,     0,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
       0,     0,   151,   151,     0,     0,     0,     0,     0,    36,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      37,    38,    39,    40,    41,    42,    43,    44,    45,    46,
      47,    48,    49,    50,    51,    52,    53,    54,    55,    56,
      57,    58,    59,    60,    61,    62,    63,    64,    65,    66,
      67,    68,    69,    70,    71,    72,    73,    74,    75,    76,
      77,    78,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,     0,     0,     0,    90,     0,     0,     0,
      91,     0,     4,     5,     0,    92,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,    16,    17,    18,    19,
      20,    21,    22,    23,    24,    25,     0,     0,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    36,     0,     0,
       0,   143,   144,   145,   146,   147,     0,     0,    37,    38,
      39,    40,    41,    42,    43,    44,    45,    46,    47,    48,
      49,    50,    51,    52,    53,    54,    55,    56,    57,    58,
      59,    60,    61,    62,    63,    64,    65,    66,    67,    68,
      69,    70,    71,    72,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,     0,     0,     0,    90,     0,     0,     0,    91,     0,
       4,     5,     0,    92,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,     0,     0,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    36,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    37,    38,    39,    40,
      41,    42,    43,    44,    45,    46,    47,    48,    49,    50,
      51,    52,    53,    54,    55,    56,    57,    58,    59,    60,
      61,    62,    63,    64,    65,    66,    67,    68,    69,    70,
      71,    72,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,     0,
       0,     0,    90,     0,     0,     0,    91,     0,     0,     0,
       0,    92,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,     0,     0,    26,     0,   178,   179,   180,     0,     0,
       0,     0,     0,   181,   182,     0,     0,     0,     0,     0,
       0,     0,    36,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    37,    38,    39,    40,    41,     0,     0,
     181,   182,     0,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56,    57,    58,    59,    60,   181,   182,     0,
       0,     0,   181,   182,     0,     0,     0,     0,     0,     0,
       0,    75,   183,   184,   185,   186,   187,   188,     0,     0,
       0,   189,   190,   191,     0,     0,     0,   192,   193,     0,
       0,     0,     0,    91,     0,     0,     0,     0,    92,   183,
     184,   185,   186,   187,   188,     0,     0,     0,     0,     0,
       0,     0,   281,   282,   192,   193,   183,   184,   185,   186,
     187,   188,   184,   185,   186,   187,   188,     0,     0,     0,
       0,   192,   193,     0,     0,     0,   192,   193,   153,   154,
     155,   156,   157,     0,   158,   159,     0,     0,   160,   161,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     162,   163,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   164,   165,   166,   167,   168,   169,   170,   171,
     172
};

static const yytype_int16 yycheck[] =
{
       2,     8,   197,     0,    42,    43,    97,    45,    46,     2,
       0,     2,    37,    37,    95,    37,   232,    94,   151,    37,
      43,   121,   122,   203,   121,    37,   121,   122,     0,   121,
      60,   131,    37,   136,   131,    60,   131,    60,    60,   131,
      42,    43,    60,    45,    46,   261,   137,   138,    60,   265,
      37,   132,    57,    58,    59,    37,    37,    95,    37,    97,
     137,   138,   128,   129,    91,     5,   109,     7,   111,     9,
      10,   121,   122,   139,   140,   109,    37,   111,    37,    60,
      61,    62,    63,    64,    60,   218,    60,   125,    37,   284,
     139,   140,    94,    95,   132,    97,    57,    58,    59,   137,
     138,    94,    95,    94,    95,   132,   286,   109,   132,   111,
     124,   125,   150,   151,    65,    66,    37,   121,   122,   133,
     134,   135,   129,   125,   121,   122,   129,   132,   133,   134,
     135,   121,   122,   131,   131,   137,   138,   218,    60,   136,
     217,   131,   123,   131,   137,   138,   137,   138,   150,   151,
     177,   132,   121,   122,   181,   182,   183,   184,   185,   186,
     187,   188,    37,    37,    37,   192,   193,   194,   195,    37,
      37,   132,   133,   134,   135,   126,   127,   128,   129,   217,
     218,    37,    37,   274,   275,    62,    37,    37,   139,   140,
     126,   127,   128,   129,   131,     4,     4,     3,     3,    48,
      37,     2,   137,   139,   140,   173,   232,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   217,   218,    -1,    -1,    -1,
      -1,    -1,    -1,     0,   217,   218,   217,   218,    -1,    -1,
      -1,    -1,    -1,    -1,    11,    12,   274,   275,    15,    16,
      17,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    -1,    -1,
      37,    38,    39,    40,    41,    42,    43,    44,    45,    46,
      -1,    -1,   274,   275,    -1,    -1,    -1,    -1,    -1,    56,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      67,    68,    69,    70,    71,    72,    73,    74,    75,    76,
      77,    78,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,    94,    95,    96,
      97,    98,    99,   100,   101,   102,   103,   104,   105,   106,
     107,   108,   109,   110,   111,   112,   113,   114,   115,   116,
     117,   118,   119,    -1,    -1,    -1,   123,    -1,    -1,    -1,
     127,    -1,    11,    12,    -1,   132,    15,    16,    17,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    -1,    -1,    37,    38,
      39,    40,    41,    42,    43,    44,    45,    46,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    56,    -1,    -1,
      -1,    60,    61,    62,    63,    64,    -1,    -1,    67,    68,
      69,    70,    71,    72,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,    98,
      99,   100,   101,   102,   103,   104,   105,   106,   107,   108,
     109,   110,   111,   112,   113,   114,   115,   116,   117,   118,
     119,    -1,    -1,    -1,   123,    -1,    -1,    -1,   127,    -1,
      11,    12,    -1,   132,    15,    16,    17,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    -1,    -1,    37,    38,    39,    40,
      41,    42,    43,    44,    45,    46,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    56,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    67,    68,    69,    70,
      71,    72,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,    98,    99,   100,
     101,   102,   103,   104,   105,   106,   107,   108,   109,   110,
     111,   112,   113,   114,   115,   116,   117,   118,   119,    -1,
      -1,    -1,   123,    -1,    -1,    -1,   127,    -1,    -1,    -1,
      -1,   132,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    -1,    -1,    37,    -1,    57,    58,    59,    -1,    -1,
      -1,    -1,    -1,    65,    66,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    56,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    67,    68,    69,    70,    71,    -1,    -1,
      65,    66,    -1,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    65,    66,    -1,
      -1,    -1,    65,    66,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   105,   124,   125,   126,   127,   128,   129,    -1,    -1,
      -1,   133,   134,   135,    -1,    -1,    -1,   139,   140,    -1,
      -1,    -1,    -1,   127,    -1,    -1,    -1,    -1,   132,   124,
     125,   126,   127,   128,   129,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   137,   138,   139,   140,   124,   125,   126,   127,
     128,   129,   125,   126,   127,   128,   129,    -1,    -1,    -1,
      -1,   139,   140,    -1,    -1,    -1,   139,   140,     3,     4,
       5,     6,     7,    -1,     9,    10,    -1,    -1,    13,    14,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      35,    36,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    47,    48,    49,    50,    51,    52,    53,    54,
      55
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,   142,   143,     0,    11,    12,    15,    16,    17,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    37,    38,    39,    40,
      41,    42,    43,    44,    45,    46,    56,    67,    68,    69,
      70,    71,    72,    73,    74,    75,    76,    77,    78,    79,
      80,    81,    82,    83,    84,    85,    86,    87,    88,    89,
      90,    91,    92,    93,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   115,   116,   117,   118,   119,
     123,   127,   132,   144,   149,   150,   153,   154,   155,   156,
     160,   161,   162,   167,   173,   174,   176,   177,   178,   179,
     183,   184,    37,    37,    37,    37,    60,    60,    37,    37,
      37,    60,   169,    60,   170,   150,   176,   176,   176,   176,
      43,    60,   150,   160,   173,   121,   122,   145,   146,   153,
     144,   174,   176,    60,    61,    62,    63,    64,   147,   148,
     149,   150,   176,     3,     4,     5,     6,     7,     9,    10,
      13,    14,    35,    36,    47,    48,    49,    50,    51,    52,
      53,    54,    55,   157,   158,   159,   163,   136,    57,    58,
      59,    65,    66,   124,   125,   126,   127,   128,   129,   133,
     134,   135,   139,   140,   171,   172,    37,   150,   171,   172,
     180,   181,    37,   150,   171,   172,   185,   186,   124,   125,
     133,   134,   135,   175,   176,   173,   147,   149,   150,   153,
     176,   147,   153,   131,   131,   131,     8,   129,   129,   147,
     148,   151,   152,   176,   121,   122,   121,   122,    37,    60,
     164,    60,   166,    37,    60,   168,   158,   173,   173,   173,
     173,   173,   173,   173,   173,   173,   173,   173,   173,   173,
     181,   182,    37,    37,   186,   187,    37,    37,    37,   176,
      62,    37,    37,   131,   145,   146,     4,     4,     3,     3,
      48,   137,   138,   131,   146,   131,   146,   147,   147,    37,
      60,   165,    37,   181,   186,   137
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_uint8 yyr1[] =
{
       0,   141,   142,   142,   143,   144,   144,   144,   144,   144,
     145,   146,   147,   147,   147,   148,   148,   148,   148,   148,
     148,   148,   148,   148,   149,   150,   151,   151,   151,   152,
     152,   153,   153,   154,   154,   154,   154,   154,   154,   155,
     155,   155,   155,   155,   155,   155,   155,   155,   155,   155,
     156,   156,   157,   157,   157,   157,   157,   157,   157,   157,
     157,   157,   157,   157,   158,   158,   158,   158,   159,   160,
     160,   160,   160,   160,   160,   160,   160,   160,   160,   160,
     160,   160,   160,   160,   160,   160,   160,   160,   160,   160,
     160,   160,   160,   160,   160,   160,   160,   160,   160,   160,
     160,   160,   160,   160,   160,   160,   160,   160,   161,   161,
     161,   161,   161,   161,   161,   161,   161,   161,   161,   161,
     161,   161,   161,   161,   161,   161,   161,   161,   162,   162,
     162,   162,   162,   162,   163,   163,   163,   163,   164,   164,
     165,   165,   166,   167,   167,   167,   168,   168,   169,   169,
     170,   171,   171,   171,   172,   172,   172,   173,   173,   174,
     174,   174,   174,   174,   174,   174,   174,   174,   174,   174,
     174,   174,   174,   174,   175,   175,   175,   175,   175,   176,
     176,   177,   177,   177,   177,   177,   177,   177,   178,   178,
     178,   178,   179,   179,   180,   180,   180,   180,   181,   182,
     182,   183,   183,   183,   183,   183,   183,   184,   184,   184,
     184,   184,   184,   184,   184,   185,   185,   185,   185,   186,
     187,   187
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     1,     0,     1,     3,     3,     3,     3,
       1,     1,     1,     1,     3,     1,     3,     3,     1,     3,
       1,     1,     1,     2,     1,     1,     1,     3,     3,     1,
       1,     1,     2,     3,     2,     2,     2,     2,     2,     2,
       3,     1,     3,     3,     1,     1,     1,     2,     1,     2,
       1,     0,     1,     1,     3,     3,     3,     3,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     2,     2,
       2,     2,     4,     1,     1,     2,     2,     1,     2,     1,
       1,     2,     1,     2,     1,     1,     2,     1,     2,     2,
       2,     2,     2,     2,     4,     2,     2,     2,     1,     1,
       1,     1,     1,     1,     2,     2,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     4,
       6,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       3,     2,     3,     1,     1,     1,     1,     1,     1,     1,
       3,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     2,     3,     1,     1,
       3,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     2,     2,     3,     1,
       1,     3
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (yyscanner, cstate, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, yyscanner, cstate); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, void *yyscanner, compiler_state_t *cstate)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (yyscanner);
  YY_USE (cstate);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, void *yyscanner, compiler_state_t *cstate)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep, yyscanner, cstate);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp,
                 int yyrule, void *yyscanner, compiler_state_t *cstate)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)], yyscanner, cstate);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, yyscanner, cstate); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif






/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, void *yyscanner, compiler_state_t *cstate)
{
  YY_USE (yyvaluep);
  YY_USE (yyscanner);
  YY_USE (cstate);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (void *yyscanner, compiler_state_t *cstate)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

    /* Number of syntax errors so far.  */
    int yynerrs YY_ATTRIBUTE_UNUSED = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;



#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, yyscanner);
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* prog: null expr  */
#line 432 "grammar.y"
{
	/*
	 * I'm not sure we have a reason to use yynerrs, but it's
	 * declared, and incremented, whether we need it or not,
	 * which means that Clang 15 will give a "used but not
	 * set" warning.  This should suppress the warning for
	 * yynerrs without suppressing it for other variables.
	 */
	(void) yynerrs;
	CHECK_INT_VAL(finish_parse(cstate, (yyvsp[0].blk).b));
}
#line 1922 "grammar.c"
    break;

  case 4: /* null: %empty  */
#line 445 "grammar.y"
                                { (yyval.blk).q = qerr; }
#line 1928 "grammar.c"
    break;

  case 6: /* expr: expr and term  */
#line 448 "grammar.y"
                                { gen_and((yyvsp[-2].blk).b, (yyvsp[0].blk).b); (yyval.blk) = (yyvsp[0].blk); }
#line 1934 "grammar.c"
    break;

  case 7: /* expr: expr and id  */
#line 449 "grammar.y"
                                { gen_and((yyvsp[-2].blk).b, (yyvsp[0].blk).b); (yyval.blk) = (yyvsp[0].blk); }
#line 1940 "grammar.c"
    break;

  case 8: /* expr: expr or term  */
#line 450 "grammar.y"
                                { gen_or((yyvsp[-2].blk).b, (yyvsp[0].blk).b); (yyval.blk) = (yyvsp[0].blk); }
#line 1946 "grammar.c"
    break;

  case 9: /* expr: expr or id  */
#line 451 "grammar.y"
                                { gen_or((yyvsp[-2].blk).b, (yyvsp[0].blk).b); (yyval.blk) = (yyvsp[0].blk); }
#line 1952 "grammar.c"
    break;

  case 10: /* and: AND  */
#line 453 "grammar.y"
                                { (yyval.blk) = (yyvsp[-1].blk); }
#line 1958 "grammar.c"
    break;

  case 11: /* or: OR  */
#line 455 "grammar.y"
                                { (yyval.blk) = (yyvsp[-1].blk); }
#line 1964 "grammar.c"
    break;

  case 13: /* id: pnum  */
#line 458 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_ncode(cstate, NULL, (yyvsp[0].h),
						   (yyval.blk).q = (yyvsp[-1].blk).q))); }
#line 1971 "grammar.c"
    break;

  case 14: /* id: paren pid ')'  */
#line 460 "grammar.y"
                                { (yyval.blk) = (yyvsp[-1].blk); }
#line 1977 "grammar.c"
    break;

  case 15: /* nid: ID  */
#line 462 "grammar.y"
                                { CHECK_PTR_VAL((yyvsp[0].s)); CHECK_PTR_VAL(((yyval.blk).b = gen_scode(cstate, (yyvsp[0].s), (yyval.blk).q = (yyvsp[-1].blk).q))); }
#line 1983 "grammar.c"
    break;

  case 16: /* nid: HID '/' NUM  */
#line 463 "grammar.y"
                                {
				  CHECK_PTR_VAL((yyvsp[-2].s));
				  /* Check whether HID/NUM is being used when appropriate */
				  (yyval.blk).q = (yyvsp[-3].blk).q;
				  if ((yyval.blk).q.addr == Q_PORT) {
					bpf_set_error(cstate, "'port' modifier applied to IP address and prefix length");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PORTRANGE) {
					bpf_set_error(cstate, "'portrange' modifier applied to IP address and prefix length");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PROTO) {
					bpf_set_error(cstate, "'proto' modifier applied to IP address and prefix length");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PROTOCHAIN) {
					bpf_set_error(cstate, "'protochain' modifier applied to IP address and prefix length");
					YYABORT;
				  }
				  CHECK_PTR_VAL(((yyval.blk).b = gen_mcode(cstate, (yyvsp[-2].s), NULL, (yyvsp[0].h), (yyval.blk).q)));
				}
#line 2007 "grammar.c"
    break;

  case 17: /* nid: HID NETMASK HID  */
#line 482 "grammar.y"
                                {
				  CHECK_PTR_VAL((yyvsp[-2].s));
				  /* Check whether HID mask HID is being used when appropriate */
				  (yyval.blk).q = (yyvsp[-3].blk).q;
				  if ((yyval.blk).q.addr == Q_PORT) {
					bpf_set_error(cstate, "'port' modifier applied to IP address and netmask");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PORTRANGE) {
					bpf_set_error(cstate, "'portrange' modifier applied to IP address and netmask");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PROTO) {
					bpf_set_error(cstate, "'proto' modifier applied to IP address and netmask");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PROTOCHAIN) {
					bpf_set_error(cstate, "'protochain' modifier applied to IP address and netmask");
					YYABORT;
				  }
				  CHECK_PTR_VAL(((yyval.blk).b = gen_mcode(cstate, (yyvsp[-2].s), (yyvsp[0].s), 0, (yyval.blk).q)));
				}
#line 2031 "grammar.c"
    break;

  case 18: /* nid: HID  */
#line 501 "grammar.y"
                                {
				  CHECK_PTR_VAL((yyvsp[0].s));
				  /* Check whether HID is being used when appropriate */
				  (yyval.blk).q = (yyvsp[-1].blk).q;
				  if ((yyval.blk).q.addr == Q_PORT) {
					bpf_set_error(cstate, "'port' modifier applied to IP address");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PORTRANGE) {
					bpf_set_error(cstate, "'portrange' modifier applied to IP address");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PROTO) {
					bpf_set_error(cstate, "'proto' modifier applied to IP address");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PROTOCHAIN) {
					bpf_set_error(cstate, "'protochain' modifier applied to IP address");
					YYABORT;
				  }
				  CHECK_PTR_VAL(((yyval.blk).b = gen_ncode(cstate, (yyvsp[0].s), 0, (yyval.blk).q)));
				}
#line 2055 "grammar.c"
    break;

  case 19: /* nid: HID6 '/' NUM  */
#line 520 "grammar.y"
                                {
				  CHECK_PTR_VAL((yyvsp[-2].s));
#ifdef INET6
				  /* Check whether HID6/NUM is being used when appropriate */
				  (yyval.blk).q = (yyvsp[-3].blk).q;
				  if ((yyval.blk).q.addr == Q_PORT) {
					bpf_set_error(cstate, "'port' modifier applied to IP address and prefix length");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PORTRANGE) {
					bpf_set_error(cstate, "'portrange' modifier applied to IP address and prefix length");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PROTO) {
					bpf_set_error(cstate, "'proto' modifier applied to IP address and prefix length ");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PROTOCHAIN) {
					bpf_set_error(cstate, "'protochain' modifier applied to IP address and prefix length");
					YYABORT;
				  }
				  CHECK_PTR_VAL(((yyval.blk).b = gen_mcode6(cstate, (yyvsp[-2].s), (yyvsp[0].h), (yyval.blk).q)));
#else
				  bpf_set_error(cstate, "IPv6 addresses not supported "
					"in this configuration");
				  YYABORT;
#endif /*INET6*/
				}
#line 2085 "grammar.c"
    break;

  case 20: /* nid: HID6  */
#line 545 "grammar.y"
                                {
				  CHECK_PTR_VAL((yyvsp[0].s));
#ifdef INET6
				  /* Check whether HID6 is being used when appropriate */
				  (yyval.blk).q = (yyvsp[-1].blk).q;
				  if ((yyval.blk).q.addr == Q_PORT) {
					bpf_set_error(cstate, "'port' modifier applied to IP address");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PORTRANGE) {
					bpf_set_error(cstate, "'portrange' modifier applied to IP address");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PROTO) {
					bpf_set_error(cstate, "'proto' modifier applied to 'ip6addr/prefixlen");
					YYABORT;
				  } else if ((yyval.blk).q.addr == Q_PROTOCHAIN) {
					bpf_set_error(cstate, "'protochain' modifier applied to IP address");
					YYABORT;
				  }
				  CHECK_PTR_VAL(((yyval.blk).b = gen_mcode6(cstate, (yyvsp[0].s), 128, (yyval.blk).q)));
#else
				  bpf_set_error(cstate, "IPv6 addresses not supported "
					"in this configuration");
				  YYABORT;
#endif /*INET6*/
				}
#line 2115 "grammar.c"
    break;

  case 21: /* nid: EID  */
#line 570 "grammar.y"
                                { CHECK_PTR_VAL((yyvsp[0].s)); CHECK_PTR_VAL(((yyval.blk).b = gen_ecode(cstate, (yyvsp[0].s), (yyval.blk).q = (yyvsp[-1].blk).q))); }
#line 2121 "grammar.c"
    break;

  case 22: /* nid: AID  */
#line 571 "grammar.y"
                                { CHECK_PTR_VAL((yyvsp[0].s)); CHECK_PTR_VAL(((yyval.blk).b = gen_acode(cstate, (yyvsp[0].s), (yyval.blk).q = (yyvsp[-1].blk).q))); }
#line 2127 "grammar.c"
    break;

  case 23: /* nid: not id  */
#line 572 "grammar.y"
                                { gen_not((yyvsp[0].blk).b); (yyval.blk) = (yyvsp[0].blk); }
#line 2133 "grammar.c"
    break;

  case 24: /* not: '!'  */
#line 574 "grammar.y"
                                { (yyval.blk) = (yyvsp[-1].blk); }
#line 2139 "grammar.c"
    break;

  case 25: /* paren: '('  */
#line 576 "grammar.y"
                                { (yyval.blk) = (yyvsp[-1].blk); }
#line 2145 "grammar.c"
    break;

  case 27: /* pid: qid and id  */
#line 579 "grammar.y"
                                { gen_and((yyvsp[-2].blk).b, (yyvsp[0].blk).b); (yyval.blk) = (yyvsp[0].blk); }
#line 2151 "grammar.c"
    break;

  case 28: /* pid: qid or id  */
#line 580 "grammar.y"
                                { gen_or((yyvsp[-2].blk).b, (yyvsp[0].blk).b); (yyval.blk) = (yyvsp[0].blk); }
#line 2157 "grammar.c"
    break;

  case 29: /* qid: pnum  */
#line 582 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_ncode(cstate, NULL, (yyvsp[0].h),
						   (yyval.blk).q = (yyvsp[-1].blk).q))); }
#line 2164 "grammar.c"
    break;

  case 32: /* term: not term  */
#line 587 "grammar.y"
                                { gen_not((yyvsp[0].blk).b); (yyval.blk) = (yyvsp[0].blk); }
#line 2170 "grammar.c"
    break;

  case 33: /* head: pqual dqual aqual  */
#line 589 "grammar.y"
                                { QSET((yyval.blk).q, (yyvsp[-2].i), (yyvsp[-1].i), (yyvsp[0].i)); }
#line 2176 "grammar.c"
    break;

  case 34: /* head: pqual dqual  */
#line 590 "grammar.y"
                                { QSET((yyval.blk).q, (yyvsp[-1].i), (yyvsp[0].i), Q_DEFAULT); }
#line 2182 "grammar.c"
    break;

  case 35: /* head: pqual aqual  */
#line 591 "grammar.y"
                                { QSET((yyval.blk).q, (yyvsp[-1].i), Q_DEFAULT, (yyvsp[0].i)); }
#line 2188 "grammar.c"
    break;

  case 36: /* head: pqual PROTO  */
#line 592 "grammar.y"
                                { QSET((yyval.blk).q, (yyvsp[-1].i), Q_DEFAULT, Q_PROTO); }
#line 2194 "grammar.c"
    break;

  case 37: /* head: pqual PROTOCHAIN  */
#line 593 "grammar.y"
                                {
#ifdef NO_PROTOCHAIN
				  bpf_set_error(cstate, "protochain not supported");
				  YYABORT;
#else
				  QSET((yyval.blk).q, (yyvsp[-1].i), Q_DEFAULT, Q_PROTOCHAIN);
#endif
				}
#line 2207 "grammar.c"
    break;

  case 38: /* head: pqual ndaqual  */
#line 601 "grammar.y"
                                { QSET((yyval.blk).q, (yyvsp[-1].i), Q_DEFAULT, (yyvsp[0].i)); }
#line 2213 "grammar.c"
    break;

  case 39: /* rterm: head id  */
#line 603 "grammar.y"
                                { (yyval.blk) = (yyvsp[0].blk); }
#line 2219 "grammar.c"
    break;

  case 40: /* rterm: paren expr ')'  */
#line 604 "grammar.y"
                                { (yyval.blk).b = (yyvsp[-1].blk).b; (yyval.blk).q = (yyvsp[-2].blk).q; }
#line 2225 "grammar.c"
    break;

  case 41: /* rterm: pname  */
#line 605 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_proto_abbrev(cstate, (yyvsp[0].i)))); (yyval.blk).q = qerr; }
#line 2231 "grammar.c"
    break;

  case 42: /* rterm: arth relop arth  */
#line 606 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_relation(cstate, (yyvsp[-1].i), (yyvsp[-2].a), (yyvsp[0].a), 0)));
				  (yyval.blk).q = qerr; }
#line 2238 "grammar.c"
    break;

  case 43: /* rterm: arth irelop arth  */
#line 608 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_relation(cstate, (yyvsp[-1].i), (yyvsp[-2].a), (yyvsp[0].a), 1)));
				  (yyval.blk).q = qerr; }
#line 2245 "grammar.c"
    break;

  case 44: /* rterm: other  */
#line 610 "grammar.y"
                                { (yyval.blk).b = (yyvsp[0].rblk); (yyval.blk).q = qerr; }
#line 2251 "grammar.c"
    break;

  case 45: /* rterm: atmtype  */
#line 611 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_atmtype_abbrev(cstate, (yyvsp[0].i)))); (yyval.blk).q = qerr; }
#line 2257 "grammar.c"
    break;

  case 46: /* rterm: atmmultitype  */
#line 612 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_atmmulti_abbrev(cstate, (yyvsp[0].i)))); (yyval.blk).q = qerr; }
#line 2263 "grammar.c"
    break;

  case 47: /* rterm: atmfield atmvalue  */
#line 613 "grammar.y"
                                { (yyval.blk).b = (yyvsp[0].blk).b; (yyval.blk).q = qerr; }
#line 2269 "grammar.c"
    break;

  case 48: /* rterm: mtp2type  */
#line 614 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_mtp2type_abbrev(cstate, (yyvsp[0].i)))); (yyval.blk).q = qerr; }
#line 2275 "grammar.c"
    break;

  case 49: /* rterm: mtp3field mtp3value  */
#line 615 "grammar.y"
                                { (yyval.blk).b = (yyvsp[0].blk).b; (yyval.blk).q = qerr; }
#line 2281 "grammar.c"
    break;

  case 51: /* pqual: %empty  */
#line 619 "grammar.y"
                                { (yyval.i) = Q_DEFAULT; }
#line 2287 "grammar.c"
    break;

  case 52: /* dqual: SRC  */
#line 622 "grammar.y"
                                { (yyval.i) = Q_SRC; }
#line 2293 "grammar.c"
    break;

  case 53: /* dqual: DST  */
#line 623 "grammar.y"
                                { (yyval.i) = Q_DST; }
#line 2299 "grammar.c"
    break;

  case 54: /* dqual: SRC OR DST  */
#line 624 "grammar.y"
                                { (yyval.i) = Q_OR; }
#line 2305 "grammar.c"
    break;

  case 55: /* dqual: DST OR SRC  */
#line 625 "grammar.y"
                                { (yyval.i) = Q_OR; }
#line 2311 "grammar.c"
    break;

  case 56: /* dqual: SRC AND DST  */
#line 626 "grammar.y"
                                { (yyval.i) = Q_AND; }
#line 2317 "grammar.c"
    break;

  case 57: /* dqual: DST AND SRC  */
#line 627 "grammar.y"
                                { (yyval.i) = Q_AND; }
#line 2323 "grammar.c"
    break;

  case 58: /* dqual: ADDR1  */
#line 628 "grammar.y"
                                { (yyval.i) = Q_ADDR1; }
#line 2329 "grammar.c"
    break;

  case 59: /* dqual: ADDR2  */
#line 629 "grammar.y"
                                { (yyval.i) = Q_ADDR2; }
#line 2335 "grammar.c"
    break;

  case 60: /* dqual: ADDR3  */
#line 630 "grammar.y"
                                { (yyval.i) = Q_ADDR3; }
#line 2341 "grammar.c"
    break;

  case 61: /* dqual: ADDR4  */
#line 631 "grammar.y"
                                { (yyval.i) = Q_ADDR4; }
#line 2347 "grammar.c"
    break;

  case 62: /* dqual: RA  */
#line 632 "grammar.y"
                                { (yyval.i) = Q_RA; }
#line 2353 "grammar.c"
    break;

  case 63: /* dqual: TA  */
#line 633 "grammar.y"
                                { (yyval.i) = Q_TA; }
#line 2359 "grammar.c"
    break;

  case 64: /* aqual: HOST  */
#line 636 "grammar.y"
                                { (yyval.i) = Q_HOST; }
#line 2365 "grammar.c"
    break;

  case 65: /* aqual: NET  */
#line 637 "grammar.y"
                                { (yyval.i) = Q_NET; }
#line 2371 "grammar.c"
    break;

  case 66: /* aqual: PORT  */
#line 638 "grammar.y"
                                { (yyval.i) = Q_PORT; }
#line 2377 "grammar.c"
    break;

  case 67: /* aqual: PORTRANGE  */
#line 639 "grammar.y"
                                { (yyval.i) = Q_PORTRANGE; }
#line 2383 "grammar.c"
    break;

  case 68: /* ndaqual: GATEWAY  */
#line 642 "grammar.y"
                                { (yyval.i) = Q_GATEWAY; }
#line 2389 "grammar.c"
    break;

  case 69: /* pname: LINK  */
#line 644 "grammar.y"
                                { (yyval.i) = Q_LINK; }
#line 2395 "grammar.c"
    break;

  case 70: /* pname: IP  */
#line 645 "grammar.y"
                                { (yyval.i) = Q_IP; }
#line 2401 "grammar.c"
    break;

  case 71: /* pname: ARP  */
#line 646 "grammar.y"
                                { (yyval.i) = Q_ARP; }
#line 2407 "grammar.c"
    break;

  case 72: /* pname: RARP  */
#line 647 "grammar.y"
                                { (yyval.i) = Q_RARP; }
#line 2413 "grammar.c"
    break;

  case 73: /* pname: SCTP  */
#line 648 "grammar.y"
                                { (yyval.i) = Q_SCTP; }
#line 2419 "grammar.c"
    break;

  case 74: /* pname: TCP  */
#line 649 "grammar.y"
                                { (yyval.i) = Q_TCP; }
#line 2425 "grammar.c"
    break;

  case 75: /* pname: UDP  */
#line 650 "grammar.y"
                                { (yyval.i) = Q_UDP; }
#line 2431 "grammar.c"
    break;

  case 76: /* pname: ICMP  */
#line 651 "grammar.y"
                                { (yyval.i) = Q_ICMP; }
#line 2437 "grammar.c"
    break;

  case 77: /* pname: IGMP  */
#line 652 "grammar.y"
                                { (yyval.i) = Q_IGMP; }
#line 2443 "grammar.c"
    break;

  case 78: /* pname: IGRP  */
#line 653 "grammar.y"
                                { (yyval.i) = Q_IGRP; }
#line 2449 "grammar.c"
    break;

  case 79: /* pname: PIM  */
#line 654 "grammar.y"
                                { (yyval.i) = Q_PIM; }
#line 2455 "grammar.c"
    break;

  case 80: /* pname: VRRP  */
#line 655 "grammar.y"
                                { (yyval.i) = Q_VRRP; }
#line 2461 "grammar.c"
    break;

  case 81: /* pname: CARP  */
#line 656 "grammar.y"
                                { (yyval.i) = Q_CARP; }
#line 2467 "grammar.c"
    break;

  case 82: /* pname: ATALK  */
#line 657 "grammar.y"
                                { (yyval.i) = Q_ATALK; }
#line 2473 "grammar.c"
    break;

  case 83: /* pname: AARP  */
#line 658 "grammar.y"
                                { (yyval.i) = Q_AARP; }
#line 2479 "grammar.c"
    break;

  case 84: /* pname: DECNET  */
#line 659 "grammar.y"
                                { (yyval.i) = Q_DECNET; }
#line 2485 "grammar.c"
    break;

  case 85: /* pname: LAT  */
#line 660 "grammar.y"
                                { (yyval.i) = Q_LAT; }
#line 2491 "grammar.c"
    break;

  case 86: /* pname: SCA  */
#line 661 "grammar.y"
                                { (yyval.i) = Q_SCA; }
#line 2497 "grammar.c"
    break;

  case 87: /* pname: MOPDL  */
#line 662 "grammar.y"
                                { (yyval.i) = Q_MOPDL; }
#line 2503 "grammar.c"
    break;

  case 88: /* pname: MOPRC  */
#line 663 "grammar.y"
                                { (yyval.i) = Q_MOPRC; }
#line 2509 "grammar.c"
    break;

  case 89: /* pname: IPV6  */
#line 664 "grammar.y"
                                { (yyval.i) = Q_IPV6; }
#line 2515 "grammar.c"
    break;

  case 90: /* pname: ICMPV6  */
#line 665 "grammar.y"
                                { (yyval.i) = Q_ICMPV6; }
#line 2521 "grammar.c"
    break;

  case 91: /* pname: AH  */
#line 666 "grammar.y"
                                { (yyval.i) = Q_AH; }
#line 2527 "grammar.c"
    break;

  case 92: /* pname: ESP  */
#line 667 "grammar.y"
                                { (yyval.i) = Q_ESP; }
#line 2533 "grammar.c"
    break;

  case 93: /* pname: ISO  */
#line 668 "grammar.y"
                                { (yyval.i) = Q_ISO; }
#line 2539 "grammar.c"
    break;

  case 94: /* pname: ESIS  */
#line 669 "grammar.y"
                                { (yyval.i) = Q_ESIS; }
#line 2545 "grammar.c"
    break;

  case 95: /* pname: ISIS  */
#line 670 "grammar.y"
                                { (yyval.i) = Q_ISIS; }
#line 2551 "grammar.c"
    break;

  case 96: /* pname: L1  */
#line 671 "grammar.y"
                                { (yyval.i) = Q_ISIS_L1; }
#line 2557 "grammar.c"
    break;

  case 97: /* pname: L2  */
#line 672 "grammar.y"
                                { (yyval.i) = Q_ISIS_L2; }
#line 2563 "grammar.c"
    break;

  case 98: /* pname: IIH  */
#line 673 "grammar.y"
                                { (yyval.i) = Q_ISIS_IIH; }
#line 2569 "grammar.c"
    break;

  case 99: /* pname: LSP  */
#line 674 "grammar.y"
                                { (yyval.i) = Q_ISIS_LSP; }
#line 2575 "grammar.c"
    break;

  case 100: /* pname: SNP  */
#line 675 "grammar.y"
                                { (yyval.i) = Q_ISIS_SNP; }
#line 2581 "grammar.c"
    break;

  case 101: /* pname: PSNP  */
#line 676 "grammar.y"
                                { (yyval.i) = Q_ISIS_PSNP; }
#line 2587 "grammar.c"
    break;

  case 102: /* pname: CSNP  */
#line 677 "grammar.y"
                                { (yyval.i) = Q_ISIS_CSNP; }
#line 2593 "grammar.c"
    break;

  case 103: /* pname: CLNP  */
#line 678 "grammar.y"
                                { (yyval.i) = Q_CLNP; }
#line 2599 "grammar.c"
    break;

  case 104: /* pname: STP  */
#line 679 "grammar.y"
                                { (yyval.i) = Q_STP; }
#line 2605 "grammar.c"
    break;

  case 105: /* pname: IPX  */
#line 680 "grammar.y"
                                { (yyval.i) = Q_IPX; }
#line 2611 "grammar.c"
    break;

  case 106: /* pname: NETBEUI  */
#line 681 "grammar.y"
                                { (yyval.i) = Q_NETBEUI; }
#line 2617 "grammar.c"
    break;

  case 107: /* pname: RADIO  */
#line 682 "grammar.y"
                                { (yyval.i) = Q_RADIO; }
#line 2623 "grammar.c"
    break;

  case 108: /* other: pqual TK_BROADCAST  */
#line 684 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_broadcast(cstate, (yyvsp[-1].i)))); }
#line 2629 "grammar.c"
    break;

  case 109: /* other: pqual TK_MULTICAST  */
#line 685 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_multicast(cstate, (yyvsp[-1].i)))); }
#line 2635 "grammar.c"
    break;

  case 110: /* other: LESS NUM  */
#line 686 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_less(cstate, (yyvsp[0].h)))); }
#line 2641 "grammar.c"
    break;

  case 111: /* other: GREATER NUM  */
#line 687 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_greater(cstate, (yyvsp[0].h)))); }
#line 2647 "grammar.c"
    break;

  case 112: /* other: CBYTE NUM byteop NUM  */
#line 688 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_byteop(cstate, (yyvsp[-1].i), (yyvsp[-2].h), (yyvsp[0].h)))); }
#line 2653 "grammar.c"
    break;

  case 113: /* other: INBOUND  */
#line 689 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_inbound(cstate, 0))); }
#line 2659 "grammar.c"
    break;

  case 114: /* other: OUTBOUND  */
#line 690 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_inbound(cstate, 1))); }
#line 2665 "grammar.c"
    break;

  case 115: /* other: IFINDEX NUM  */
#line 691 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_ifindex(cstate, (yyvsp[0].h)))); }
#line 2671 "grammar.c"
    break;

  case 116: /* other: VLAN pnum  */
#line 692 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_vlan(cstate, (yyvsp[0].h), 1))); }
#line 2677 "grammar.c"
    break;

  case 117: /* other: VLAN  */
#line 693 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_vlan(cstate, 0, 0))); }
#line 2683 "grammar.c"
    break;

  case 118: /* other: MPLS pnum  */
#line 694 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_mpls(cstate, (yyvsp[0].h), 1))); }
#line 2689 "grammar.c"
    break;

  case 119: /* other: MPLS  */
#line 695 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_mpls(cstate, 0, 0))); }
#line 2695 "grammar.c"
    break;

  case 120: /* other: PPPOED  */
#line 696 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_pppoed(cstate))); }
#line 2701 "grammar.c"
    break;

  case 121: /* other: PPPOES pnum  */
#line 697 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_pppoes(cstate, (yyvsp[0].h), 1))); }
#line 2707 "grammar.c"
    break;

  case 122: /* other: PPPOES  */
#line 698 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_pppoes(cstate, 0, 0))); }
#line 2713 "grammar.c"
    break;

  case 123: /* other: GENEVE pnum  */
#line 699 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_geneve(cstate, (yyvsp[0].h), 1))); }
#line 2719 "grammar.c"
    break;

  case 124: /* other: GENEVE  */
#line 700 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_geneve(cstate, 0, 0))); }
#line 2725 "grammar.c"
    break;

  case 125: /* other: pfvar  */
#line 701 "grammar.y"
                                { (yyval.rblk) = (yyvsp[0].rblk); }
#line 2731 "grammar.c"
    break;

  case 126: /* other: pqual p80211  */
#line 702 "grammar.y"
                                { (yyval.rblk) = (yyvsp[0].rblk); }
#line 2737 "grammar.c"
    break;

  case 127: /* other: pllc  */
#line 703 "grammar.y"
                                { (yyval.rblk) = (yyvsp[0].rblk); }
#line 2743 "grammar.c"
    break;

  case 128: /* pfvar: PF_IFNAME ID  */
#line 706 "grammar.y"
                                { CHECK_PTR_VAL((yyvsp[0].s)); CHECK_PTR_VAL(((yyval.rblk) = gen_pf_ifname(cstate, (yyvsp[0].s)))); }
#line 2749 "grammar.c"
    break;

  case 129: /* pfvar: PF_RSET ID  */
#line 707 "grammar.y"
                                { CHECK_PTR_VAL((yyvsp[0].s)); CHECK_PTR_VAL(((yyval.rblk) = gen_pf_ruleset(cstate, (yyvsp[0].s)))); }
#line 2755 "grammar.c"
    break;

  case 130: /* pfvar: PF_RNR NUM  */
#line 708 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_pf_rnr(cstate, (yyvsp[0].h)))); }
#line 2761 "grammar.c"
    break;

  case 131: /* pfvar: PF_SRNR NUM  */
#line 709 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_pf_srnr(cstate, (yyvsp[0].h)))); }
#line 2767 "grammar.c"
    break;

  case 132: /* pfvar: PF_REASON reason  */
#line 710 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_pf_reason(cstate, (yyvsp[0].i)))); }
#line 2773 "grammar.c"
    break;

  case 133: /* pfvar: PF_ACTION action  */
#line 711 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_pf_action(cstate, (yyvsp[0].i)))); }
#line 2779 "grammar.c"
    break;

  case 134: /* p80211: TYPE type SUBTYPE subtype  */
#line 715 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_p80211_type(cstate, (yyvsp[-2].i) | (yyvsp[0].i),
					IEEE80211_FC0_TYPE_MASK |
					IEEE80211_FC0_SUBTYPE_MASK)));
				}
#line 2788 "grammar.c"
    break;

  case 135: /* p80211: TYPE type  */
#line 719 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_p80211_type(cstate, (yyvsp[0].i),
					IEEE80211_FC0_TYPE_MASK)));
				}
#line 2796 "grammar.c"
    break;

  case 136: /* p80211: SUBTYPE type_subtype  */
#line 722 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_p80211_type(cstate, (yyvsp[0].i),
					IEEE80211_FC0_TYPE_MASK |
					IEEE80211_FC0_SUBTYPE_MASK)));
				}
#line 2805 "grammar.c"
    break;

  case 137: /* p80211: DIR dir  */
#line 726 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_p80211_fcdir(cstate, (yyvsp[0].i)))); }
#line 2811 "grammar.c"
    break;

  case 138: /* type: NUM  */
#line 729 "grammar.y"
                                { if (((yyvsp[0].h) & (~IEEE80211_FC0_TYPE_MASK)) != 0) {
					bpf_set_error(cstate, "invalid 802.11 type value 0x%02x", (yyvsp[0].h));
					YYABORT;
				  }
				  (yyval.i) = (int)(yyvsp[0].h);
				}
#line 2822 "grammar.c"
    break;

  case 139: /* type: ID  */
#line 735 "grammar.y"
                                { CHECK_PTR_VAL((yyvsp[0].s));
				  (yyval.i) = str2tok((yyvsp[0].s), ieee80211_types);
				  if ((yyval.i) == -1) {
					bpf_set_error(cstate, "unknown 802.11 type name \"%s\"", (yyvsp[0].s));
					YYABORT;
				  }
				}
#line 2834 "grammar.c"
    break;

  case 140: /* subtype: NUM  */
#line 744 "grammar.y"
                                { if (((yyvsp[0].h) & (~IEEE80211_FC0_SUBTYPE_MASK)) != 0) {
					bpf_set_error(cstate, "invalid 802.11 subtype value 0x%02x", (yyvsp[0].h));
					YYABORT;
				  }
				  (yyval.i) = (int)(yyvsp[0].h);
				}
#line 2845 "grammar.c"
    break;

  case 141: /* subtype: ID  */
#line 750 "grammar.y"
                                { const struct tok *types = NULL;
				  int i;
				  CHECK_PTR_VAL((yyvsp[0].s));
				  for (i = 0;; i++) {
					if (ieee80211_type_subtypes[i].tok == NULL) {
						/* Ran out of types */
						bpf_set_error(cstate, "unknown 802.11 type");
						YYABORT;
					}
					if ((yyvsp[(-1) - (1)].i) == ieee80211_type_subtypes[i].type) {
						types = ieee80211_type_subtypes[i].tok;
						break;
					}
				  }

				  (yyval.i) = str2tok((yyvsp[0].s), types);
				  if ((yyval.i) == -1) {
					bpf_set_error(cstate, "unknown 802.11 subtype name \"%s\"", (yyvsp[0].s));
					YYABORT;
				  }
				}
#line 2871 "grammar.c"
    break;

  case 142: /* type_subtype: ID  */
#line 773 "grammar.y"
                                { int i;
				  CHECK_PTR_VAL((yyvsp[0].s));
				  for (i = 0;; i++) {
					if (ieee80211_type_subtypes[i].tok == NULL) {
						/* Ran out of types */
						bpf_set_error(cstate, "unknown 802.11 type name");
						YYABORT;
					}
					(yyval.i) = str2tok((yyvsp[0].s), ieee80211_type_subtypes[i].tok);
					if ((yyval.i) != -1) {
						(yyval.i) |= ieee80211_type_subtypes[i].type;
						break;
					}
				  }
				}
#line 2891 "grammar.c"
    break;

  case 143: /* pllc: LLC  */
#line 790 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_llc(cstate))); }
#line 2897 "grammar.c"
    break;

  case 144: /* pllc: LLC ID  */
#line 791 "grammar.y"
                                { CHECK_PTR_VAL((yyvsp[0].s));
				  if (pcapint_strcasecmp((yyvsp[0].s), "i") == 0) {
					CHECK_PTR_VAL(((yyval.rblk) = gen_llc_i(cstate)));
				  } else if (pcapint_strcasecmp((yyvsp[0].s), "s") == 0) {
					CHECK_PTR_VAL(((yyval.rblk) = gen_llc_s(cstate)));
				  } else if (pcapint_strcasecmp((yyvsp[0].s), "u") == 0) {
					CHECK_PTR_VAL(((yyval.rblk) = gen_llc_u(cstate)));
				  } else {
					int subtype;

					subtype = str2tok((yyvsp[0].s), llc_s_subtypes);
					if (subtype != -1) {
						CHECK_PTR_VAL(((yyval.rblk) = gen_llc_s_subtype(cstate, subtype)));
					} else {
						subtype = str2tok((yyvsp[0].s), llc_u_subtypes);
						if (subtype == -1) {
							bpf_set_error(cstate, "unknown LLC type name \"%s\"", (yyvsp[0].s));
							YYABORT;
						}
						CHECK_PTR_VAL(((yyval.rblk) = gen_llc_u_subtype(cstate, subtype)));
					}
				  }
				}
#line 2925 "grammar.c"
    break;

  case 145: /* pllc: LLC PF_RNR  */
#line 815 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.rblk) = gen_llc_s_subtype(cstate, LLC_RNR))); }
#line 2931 "grammar.c"
    break;

  case 146: /* dir: NUM  */
#line 818 "grammar.y"
                                { (yyval.i) = (int)(yyvsp[0].h); }
#line 2937 "grammar.c"
    break;

  case 147: /* dir: ID  */
#line 819 "grammar.y"
                                { CHECK_PTR_VAL((yyvsp[0].s));
				  if (pcapint_strcasecmp((yyvsp[0].s), "nods") == 0)
					(yyval.i) = IEEE80211_FC1_DIR_NODS;
				  else if (pcapint_strcasecmp((yyvsp[0].s), "tods") == 0)
					(yyval.i) = IEEE80211_FC1_DIR_TODS;
				  else if (pcapint_strcasecmp((yyvsp[0].s), "fromds") == 0)
					(yyval.i) = IEEE80211_FC1_DIR_FROMDS;
				  else if (pcapint_strcasecmp((yyvsp[0].s), "dstods") == 0)
					(yyval.i) = IEEE80211_FC1_DIR_DSTODS;
				  else {
					bpf_set_error(cstate, "unknown 802.11 direction");
					YYABORT;
				  }
				}
#line 2956 "grammar.c"
    break;

  case 148: /* reason: NUM  */
#line 835 "grammar.y"
                                { (yyval.i) = (yyvsp[0].h); }
#line 2962 "grammar.c"
    break;

  case 149: /* reason: ID  */
#line 836 "grammar.y"
                                { CHECK_PTR_VAL((yyvsp[0].s)); CHECK_INT_VAL(((yyval.i) = pfreason_to_num(cstate, (yyvsp[0].s)))); }
#line 2968 "grammar.c"
    break;

  case 150: /* action: ID  */
#line 839 "grammar.y"
                                { CHECK_PTR_VAL((yyvsp[0].s)); CHECK_INT_VAL(((yyval.i) = pfaction_to_num(cstate, (yyvsp[0].s)))); }
#line 2974 "grammar.c"
    break;

  case 151: /* relop: '>'  */
#line 842 "grammar.y"
                                { (yyval.i) = BPF_JGT; }
#line 2980 "grammar.c"
    break;

  case 152: /* relop: GEQ  */
#line 843 "grammar.y"
                                { (yyval.i) = BPF_JGE; }
#line 2986 "grammar.c"
    break;

  case 153: /* relop: '='  */
#line 844 "grammar.y"
                                { (yyval.i) = BPF_JEQ; }
#line 2992 "grammar.c"
    break;

  case 154: /* irelop: LEQ  */
#line 846 "grammar.y"
                                { (yyval.i) = BPF_JGT; }
#line 2998 "grammar.c"
    break;

  case 155: /* irelop: '<'  */
#line 847 "grammar.y"
                                { (yyval.i) = BPF_JGE; }
#line 3004 "grammar.c"
    break;

  case 156: /* irelop: NEQ  */
#line 848 "grammar.y"
                                { (yyval.i) = BPF_JEQ; }
#line 3010 "grammar.c"
    break;

  case 157: /* arth: pnum  */
#line 850 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.a) = gen_loadi(cstate, (yyvsp[0].h)))); }
#line 3016 "grammar.c"
    break;

  case 159: /* narth: pname '[' arth ']'  */
#line 853 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_load(cstate, (yyvsp[-3].i), (yyvsp[-1].a), 1))); }
#line 3022 "grammar.c"
    break;

  case 160: /* narth: pname '[' arth ':' NUM ']'  */
#line 854 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_load(cstate, (yyvsp[-5].i), (yyvsp[-3].a), (yyvsp[-1].h)))); }
#line 3028 "grammar.c"
    break;

  case 161: /* narth: arth '+' arth  */
#line 855 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_arth(cstate, BPF_ADD, (yyvsp[-2].a), (yyvsp[0].a)))); }
#line 3034 "grammar.c"
    break;

  case 162: /* narth: arth '-' arth  */
#line 856 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_arth(cstate, BPF_SUB, (yyvsp[-2].a), (yyvsp[0].a)))); }
#line 3040 "grammar.c"
    break;

  case 163: /* narth: arth '*' arth  */
#line 857 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_arth(cstate, BPF_MUL, (yyvsp[-2].a), (yyvsp[0].a)))); }
#line 3046 "grammar.c"
    break;

  case 164: /* narth: arth '/' arth  */
#line 858 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_arth(cstate, BPF_DIV, (yyvsp[-2].a), (yyvsp[0].a)))); }
#line 3052 "grammar.c"
    break;

  case 165: /* narth: arth '%' arth  */
#line 859 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_arth(cstate, BPF_MOD, (yyvsp[-2].a), (yyvsp[0].a)))); }
#line 3058 "grammar.c"
    break;

  case 166: /* narth: arth '&' arth  */
#line 860 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_arth(cstate, BPF_AND, (yyvsp[-2].a), (yyvsp[0].a)))); }
#line 3064 "grammar.c"
    break;

  case 167: /* narth: arth '|' arth  */
#line 861 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_arth(cstate, BPF_OR, (yyvsp[-2].a), (yyvsp[0].a)))); }
#line 3070 "grammar.c"
    break;

  case 168: /* narth: arth '^' arth  */
#line 862 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_arth(cstate, BPF_XOR, (yyvsp[-2].a), (yyvsp[0].a)))); }
#line 3076 "grammar.c"
    break;

  case 169: /* narth: arth LSH arth  */
#line 863 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_arth(cstate, BPF_LSH, (yyvsp[-2].a), (yyvsp[0].a)))); }
#line 3082 "grammar.c"
    break;

  case 170: /* narth: arth RSH arth  */
#line 864 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_arth(cstate, BPF_RSH, (yyvsp[-2].a), (yyvsp[0].a)))); }
#line 3088 "grammar.c"
    break;

  case 171: /* narth: '-' arth  */
#line 865 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_neg(cstate, (yyvsp[0].a)))); }
#line 3094 "grammar.c"
    break;

  case 172: /* narth: paren narth ')'  */
#line 866 "grammar.y"
                                        { (yyval.a) = (yyvsp[-1].a); }
#line 3100 "grammar.c"
    break;

  case 173: /* narth: LEN  */
#line 867 "grammar.y"
                                        { CHECK_PTR_VAL(((yyval.a) = gen_loadlen(cstate))); }
#line 3106 "grammar.c"
    break;

  case 174: /* byteop: '&'  */
#line 869 "grammar.y"
                                { (yyval.i) = '&'; }
#line 3112 "grammar.c"
    break;

  case 175: /* byteop: '|'  */
#line 870 "grammar.y"
                                { (yyval.i) = '|'; }
#line 3118 "grammar.c"
    break;

  case 176: /* byteop: '<'  */
#line 871 "grammar.y"
                                { (yyval.i) = '<'; }
#line 3124 "grammar.c"
    break;

  case 177: /* byteop: '>'  */
#line 872 "grammar.y"
                                { (yyval.i) = '>'; }
#line 3130 "grammar.c"
    break;

  case 178: /* byteop: '='  */
#line 873 "grammar.y"
                                { (yyval.i) = '='; }
#line 3136 "grammar.c"
    break;

  case 180: /* pnum: paren pnum ')'  */
#line 876 "grammar.y"
                                { (yyval.h) = (yyvsp[-1].h); }
#line 3142 "grammar.c"
    break;

  case 181: /* atmtype: LANE  */
#line 878 "grammar.y"
                                { (yyval.i) = A_LANE; }
#line 3148 "grammar.c"
    break;

  case 182: /* atmtype: METAC  */
#line 879 "grammar.y"
                                { (yyval.i) = A_METAC;	}
#line 3154 "grammar.c"
    break;

  case 183: /* atmtype: BCC  */
#line 880 "grammar.y"
                                { (yyval.i) = A_BCC; }
#line 3160 "grammar.c"
    break;

  case 184: /* atmtype: OAMF4EC  */
#line 881 "grammar.y"
                                { (yyval.i) = A_OAMF4EC; }
#line 3166 "grammar.c"
    break;

  case 185: /* atmtype: OAMF4SC  */
#line 882 "grammar.y"
                                { (yyval.i) = A_OAMF4SC; }
#line 3172 "grammar.c"
    break;

  case 186: /* atmtype: SC  */
#line 883 "grammar.y"
                                { (yyval.i) = A_SC; }
#line 3178 "grammar.c"
    break;

  case 187: /* atmtype: ILMIC  */
#line 884 "grammar.y"
                                { (yyval.i) = A_ILMIC; }
#line 3184 "grammar.c"
    break;

  case 188: /* atmmultitype: OAM  */
#line 886 "grammar.y"
                                { (yyval.i) = A_OAM; }
#line 3190 "grammar.c"
    break;

  case 189: /* atmmultitype: OAMF4  */
#line 887 "grammar.y"
                                { (yyval.i) = A_OAMF4; }
#line 3196 "grammar.c"
    break;

  case 190: /* atmmultitype: CONNECTMSG  */
#line 888 "grammar.y"
                                { (yyval.i) = A_CONNECTMSG; }
#line 3202 "grammar.c"
    break;

  case 191: /* atmmultitype: METACONNECT  */
#line 889 "grammar.y"
                                { (yyval.i) = A_METACONNECT; }
#line 3208 "grammar.c"
    break;

  case 192: /* atmfield: VPI  */
#line 892 "grammar.y"
                                { (yyval.blk).atmfieldtype = A_VPI; }
#line 3214 "grammar.c"
    break;

  case 193: /* atmfield: VCI  */
#line 893 "grammar.y"
                                { (yyval.blk).atmfieldtype = A_VCI; }
#line 3220 "grammar.c"
    break;

  case 195: /* atmvalue: relop NUM  */
#line 896 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_atmfield_code(cstate, (yyvsp[-2].blk).atmfieldtype, (yyvsp[0].h), (yyvsp[-1].i), 0))); }
#line 3226 "grammar.c"
    break;

  case 196: /* atmvalue: irelop NUM  */
#line 897 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_atmfield_code(cstate, (yyvsp[-2].blk).atmfieldtype, (yyvsp[0].h), (yyvsp[-1].i), 1))); }
#line 3232 "grammar.c"
    break;

  case 197: /* atmvalue: paren atmlistvalue ')'  */
#line 898 "grammar.y"
                                 { (yyval.blk).b = (yyvsp[-1].blk).b; (yyval.blk).q = qerr; }
#line 3238 "grammar.c"
    break;

  case 198: /* atmfieldvalue: NUM  */
#line 900 "grammar.y"
                   {
	(yyval.blk).atmfieldtype = (yyvsp[-1].blk).atmfieldtype;
	if ((yyval.blk).atmfieldtype == A_VPI ||
	    (yyval.blk).atmfieldtype == A_VCI)
		CHECK_PTR_VAL(((yyval.blk).b = gen_atmfield_code(cstate, (yyval.blk).atmfieldtype, (yyvsp[0].h), BPF_JEQ, 0)));
	}
#line 3249 "grammar.c"
    break;

  case 200: /* atmlistvalue: atmlistvalue or atmfieldvalue  */
#line 908 "grammar.y"
                                        { gen_or((yyvsp[-2].blk).b, (yyvsp[0].blk).b); (yyval.blk) = (yyvsp[0].blk); }
#line 3255 "grammar.c"
    break;

  case 201: /* mtp2type: FISU  */
#line 911 "grammar.y"
                                { (yyval.i) = M_FISU; }
#line 3261 "grammar.c"
    break;

  case 202: /* mtp2type: LSSU  */
#line 912 "grammar.y"
                                { (yyval.i) = M_LSSU; }
#line 3267 "grammar.c"
    break;

  case 203: /* mtp2type: MSU  */
#line 913 "grammar.y"
                                { (yyval.i) = M_MSU; }
#line 3273 "grammar.c"
    break;

  case 204: /* mtp2type: HFISU  */
#line 914 "grammar.y"
                                { (yyval.i) = MH_FISU; }
#line 3279 "grammar.c"
    break;

  case 205: /* mtp2type: HLSSU  */
#line 915 "grammar.y"
                                { (yyval.i) = MH_LSSU; }
#line 3285 "grammar.c"
    break;

  case 206: /* mtp2type: HMSU  */
#line 916 "grammar.y"
                                { (yyval.i) = MH_MSU; }
#line 3291 "grammar.c"
    break;

  case 207: /* mtp3field: SIO  */
#line 919 "grammar.y"
                                { (yyval.blk).mtp3fieldtype = M_SIO; }
#line 3297 "grammar.c"
    break;

  case 208: /* mtp3field: OPC  */
#line 920 "grammar.y"
                                { (yyval.blk).mtp3fieldtype = M_OPC; }
#line 3303 "grammar.c"
    break;

  case 209: /* mtp3field: DPC  */
#line 921 "grammar.y"
                                { (yyval.blk).mtp3fieldtype = M_DPC; }
#line 3309 "grammar.c"
    break;

  case 210: /* mtp3field: SLS  */
#line 922 "grammar.y"
                                { (yyval.blk).mtp3fieldtype = M_SLS; }
#line 3315 "grammar.c"
    break;

  case 211: /* mtp3field: HSIO  */
#line 923 "grammar.y"
                                { (yyval.blk).mtp3fieldtype = MH_SIO; }
#line 3321 "grammar.c"
    break;

  case 212: /* mtp3field: HOPC  */
#line 924 "grammar.y"
                                { (yyval.blk).mtp3fieldtype = MH_OPC; }
#line 3327 "grammar.c"
    break;

  case 213: /* mtp3field: HDPC  */
#line 925 "grammar.y"
                                { (yyval.blk).mtp3fieldtype = MH_DPC; }
#line 3333 "grammar.c"
    break;

  case 214: /* mtp3field: HSLS  */
#line 926 "grammar.y"
                                { (yyval.blk).mtp3fieldtype = MH_SLS; }
#line 3339 "grammar.c"
    break;

  case 216: /* mtp3value: relop NUM  */
#line 929 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_mtp3field_code(cstate, (yyvsp[-2].blk).mtp3fieldtype, (yyvsp[0].h), (yyvsp[-1].i), 0))); }
#line 3345 "grammar.c"
    break;

  case 217: /* mtp3value: irelop NUM  */
#line 930 "grammar.y"
                                { CHECK_PTR_VAL(((yyval.blk).b = gen_mtp3field_code(cstate, (yyvsp[-2].blk).mtp3fieldtype, (yyvsp[0].h), (yyvsp[-1].i), 1))); }
#line 3351 "grammar.c"
    break;

  case 218: /* mtp3value: paren mtp3listvalue ')'  */
#line 931 "grammar.y"
                                  { (yyval.blk).b = (yyvsp[-1].blk).b; (yyval.blk).q = qerr; }
#line 3357 "grammar.c"
    break;

  case 219: /* mtp3fieldvalue: NUM  */
#line 933 "grammar.y"
                    {
	(yyval.blk).mtp3fieldtype = (yyvsp[-1].blk).mtp3fieldtype;
	if ((yyval.blk).mtp3fieldtype == M_SIO ||
	    (yyval.blk).mtp3fieldtype == M_OPC ||
	    (yyval.blk).mtp3fieldtype == M_DPC ||
	    (yyval.blk).mtp3fieldtype == M_SLS ||
	    (yyval.blk).mtp3fieldtype == MH_SIO ||
	    (yyval.blk).mtp3fieldtype == MH_OPC ||
	    (yyval.blk).mtp3fieldtype == MH_DPC ||
	    (yyval.blk).mtp3fieldtype == MH_SLS)
		CHECK_PTR_VAL(((yyval.blk).b = gen_mtp3field_code(cstate, (yyval.blk).mtp3fieldtype, (yyvsp[0].h), BPF_JEQ, 0)));
	}
#line 3374 "grammar.c"
    break;

  case 221: /* mtp3listvalue: mtp3listvalue or mtp3fieldvalue  */
#line 947 "grammar.y"
                                          { gen_or((yyvsp[-2].blk).b, (yyvsp[0].blk).b); (yyval.blk) = (yyvsp[0].blk); }
#line 3380 "grammar.c"
    break;


#line 3384 "grammar.c"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      yyerror (yyscanner, cstate, YY_("syntax error"));
    }

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, yyscanner, cstate);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, yyscanner, cstate);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (yyscanner, cstate, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, yyscanner, cstate);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, yyscanner, cstate);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif

  return yyresult;
}

#line 949 "grammar.y"

