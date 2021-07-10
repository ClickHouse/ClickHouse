/*
 * Copyright (c) 2010, Oracle America, Inc.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 *       copyright notice, this list of conditions and the following
 *       disclaimer in the documentation and/or other materials
 *       provided with the distribution.
 *     * Neither the name of the "Oracle America, Inc." nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 *   FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 *   COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *   INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 *   DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 *   GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *   INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *   WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
%/*
% * Find out about remote users
% */

const RUSERS_MAXUSERLEN = 32;
const RUSERS_MAXLINELEN = 32;
const RUSERS_MAXHOSTLEN = 257;

struct rusers_utmp {
	string ut_user<RUSERS_MAXUSERLEN>;	/* aka ut_name */
	string ut_line<RUSERS_MAXLINELEN>;	/* device */
	string ut_host<RUSERS_MAXHOSTLEN>;	/* host user logged on from */
	int ut_type;				/* type of entry */
	int ut_time;				/* time entry was made */
	unsigned int ut_idle;			/* minutes idle */
};

typedef rusers_utmp utmp_array<>;

#ifdef RPC_HDR
%
%/*
% * Values for ut_type field above.
% */
#endif
const	RUSERS_EMPTY = 0;
const	RUSERS_RUN_LVL = 1;
const	RUSERS_BOOT_TIME = 2;
const	RUSERS_OLD_TIME = 3;
const	RUSERS_NEW_TIME = 4;
const	RUSERS_INIT_PROCESS = 5;
const	RUSERS_LOGIN_PROCESS = 6;
const	RUSERS_USER_PROCESS = 7;
const	RUSERS_DEAD_PROCESS = 8;
const	RUSERS_ACCOUNTING = 9;

program RUSERSPROG {

	version RUSERSVERS_3 {
		int
		RUSERSPROC_NUM(void) = 1;

		utmp_array
		RUSERSPROC_NAMES(void) = 2;

		utmp_array
		RUSERSPROC_ALLNAMES(void) = 3;
	} = 3;

} = 100002;

#ifdef RPC_HDR
%
%
%#ifdef	__cplusplus
%extern "C" {
%#endif
%
%#include <rpc/xdr.h>
%
%/*
% * The following structures are used by version 2 of the rusersd protocol.
% * They were not developed with rpcgen, so they do not appear as RPCL.
% */
%
%#define	RUSERSVERS_IDLE 2
%#define	RUSERSVERS 3		/* current version */
%#define	MAXUSERS 100
%
%/*
% * This is the structure used in version 2 of the rusersd RPC service.
% * It corresponds to the utmp structure for BSD systems.
% */
%struct ru_utmp {
%	char	ut_line[8];		/* tty name */
%	char	ut_name[8];		/* user id */
%	char	ut_host[16];		/* host name, if remote */
%	long int ut_time;		/* time on */
%};
%
%struct utmparr {
%       struct ru_utmp **uta_arr;
%       int uta_cnt;
%};
%typedef struct utmparr utmparr;
%
%extern bool_t xdr_utmparr (XDR *xdrs, struct utmparr *objp) __THROW;
%
%struct utmpidle {
%	struct ru_utmp ui_utmp;
%	unsigned int ui_idle;
%};
%
%struct utmpidlearr {
%	struct utmpidle **uia_arr;
%	int uia_cnt;
%};
%
%extern bool_t xdr_utmpidlearr (XDR *xdrs, struct utmpidlearr *objp) __THROW;
%
%#ifdef	__cplusplus
%}
%#endif
#endif


#ifdef	RPC_XDR
%bool_t xdr_utmp (XDR *xdrs, struct ru_utmp *objp);
%
%bool_t
%xdr_utmp (XDR *xdrs, struct ru_utmp *objp)
%{
%	/* Since the fields are char foo [xxx], we should not free them. */
%	if (xdrs->x_op != XDR_FREE)
%	{
%		char *ptr;
%		unsigned int size;
%		ptr = objp->ut_line;
%		size = sizeof (objp->ut_line);
%		if (!xdr_bytes (xdrs, &ptr, &size, size)) {
%			return (FALSE);
%		}
%		ptr = objp->ut_name;
%		size = sizeof (objp->ut_name);
%		if (!xdr_bytes (xdrs, &ptr, &size, size)) {
%			return (FALSE);
%		}
%		ptr = objp->ut_host;
%		size = sizeof (objp->ut_host);
%		if (!xdr_bytes (xdrs, &ptr, &size, size)) {
%			return (FALSE);
%		}
%	}
%	if (!xdr_long(xdrs, &objp->ut_time)) {
%		return (FALSE);
%	}
%	return (TRUE);
%}
%
%bool_t xdr_utmpptr(XDR *xdrs, struct ru_utmp **objpp);
%
%bool_t
%xdr_utmpptr (XDR *xdrs, struct ru_utmp **objpp)
%{
%	if (!xdr_reference(xdrs, (char **) objpp, sizeof (struct ru_utmp),
%			   (xdrproc_t) xdr_utmp)) {
%		return (FALSE);
%	}
%	return (TRUE);
%}
%
%bool_t
%xdr_utmparr (XDR *xdrs, struct utmparr *objp)
%{
%	if (!xdr_array(xdrs, (char **)&objp->uta_arr, (u_int *)&objp->uta_cnt,
%		       MAXUSERS, sizeof(struct ru_utmp *),
%		       (xdrproc_t) xdr_utmpptr)) {
%		return (FALSE);
%	}
%	return (TRUE);
%}
%
%bool_t xdr_utmpidle(XDR *xdrs, struct utmpidle *objp);
%
%bool_t
%xdr_utmpidle (XDR *xdrs, struct utmpidle *objp)
%{
%	if (!xdr_utmp(xdrs, &objp->ui_utmp)) {
%		return (FALSE);
%	}
%	if (!xdr_u_int(xdrs, &objp->ui_idle)) {
%		return (FALSE);
%	}
%	return (TRUE);
%}
%
%bool_t xdr_utmpidleptr(XDR *xdrs, struct utmpidle **objp);
%
%bool_t
%xdr_utmpidleptr (XDR *xdrs, struct utmpidle **objpp)
%{
%	if (!xdr_reference(xdrs, (char **) objpp, sizeof (struct utmpidle),
%			   (xdrproc_t) xdr_utmpidle)) {
%		return (FALSE);
%	}
%	return (TRUE);
%}
%
%bool_t
%xdr_utmpidlearr (XDR *xdrs, struct utmpidlearr *objp)
%{
%	if (!xdr_array(xdrs, (char **)&objp->uia_arr, (u_int *)&objp->uia_cnt,
%		       MAXUSERS, sizeof(struct utmpidle *),
%		       (xdrproc_t) xdr_utmpidleptr)) {
%		return (FALSE);
%	}
%	return (TRUE);
%}
#endif
