/* @(#)nlm_prot.x	2.1 88/08/01 4.0 RPCSRC */

/*
 * Network lock manager protocol definition
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
 *
 * protocol used between local lock manager and remote lock manager
 */

#ifdef RPC_HDR
%#define LM_MAXSTRLEN	1024
%#define MAXNAMELEN	LM_MAXSTRLEN+1
#endif

/*
 * status of a call to the lock manager
 */
enum nlm_stats {
	nlm_granted = 0,
	nlm_denied = 1,
	nlm_denied_nolocks = 2,
	nlm_blocked = 3,
	nlm_denied_grace_period = 4
};

struct nlm_holder {
	bool exclusive;
	int svid;
	netobj oh;
	unsigned l_offset;
	unsigned l_len;
};

union nlm_testrply switch (nlm_stats stat) {
	case nlm_denied:
		struct nlm_holder holder;
	default:
		void;
};

struct nlm_stat {
	nlm_stats stat;
};

struct nlm_res {
	netobj cookie;
	nlm_stat stat;
};

struct nlm_testres {
	netobj cookie;
	nlm_testrply stat;
};

struct nlm_lock {
	string caller_name<LM_MAXSTRLEN>;
	netobj fh;		/* identify a file */
	netobj oh;		/* identify owner of a lock */
	int svid;		/* generated from pid for svid */
	unsigned l_offset;
	unsigned l_len;
};

struct nlm_lockargs {
	netobj cookie;
	bool block;
	bool exclusive;
	struct nlm_lock alock;
	bool reclaim;		/* used for recovering locks */
	int state;		/* specify local status monitor state */
};

struct nlm_cancargs {
	netobj cookie;
	bool block;
	bool exclusive;
	struct nlm_lock alock;
};

struct nlm_testargs {
	netobj cookie;
	bool exclusive;
	struct nlm_lock alock;
};

struct nlm_unlockargs {
	netobj cookie;
	struct nlm_lock alock;
};


#ifdef RPC_HDR
%/*
% * The following enums are actually bit encoded for efficient
% * boolean algebra.... DON'T change them.....
% */
#endif
enum	fsh_mode {
	fsm_DN  = 0,	/* deny none */
	fsm_DR  = 1,	/* deny read */
	fsm_DW  = 2,	/* deny write */
	fsm_DRW = 3	/* deny read/write */
};

enum	fsh_access {
	fsa_NONE = 0,	/* for completeness */
	fsa_R    = 1,	/* read only */
	fsa_W    = 2,	/* write only */
	fsa_RW   = 3	/* read/write */
};

struct	nlm_share {
	string caller_name<LM_MAXSTRLEN>;
	netobj	fh;
	netobj	oh;
	fsh_mode	mode;
	fsh_access	access;
};

struct	nlm_shareargs {
	netobj	cookie;
	nlm_share	share;
	bool	reclaim;
};

struct	nlm_shareres {
	netobj	cookie;
	nlm_stats	stat;
	int	sequence;
};

struct	nlm_notify {
	string name<MAXNAMELEN>;
	long state;
};

/*
 * Over-the-wire protocol used between the network lock managers
 */

program NLM_PROG {
	version NLM_VERS {

		nlm_testres	NLM_TEST(struct nlm_testargs) =	1;

		nlm_res		NLM_LOCK(struct nlm_lockargs) =	2;

		nlm_res		NLM_CANCEL(struct nlm_cancargs) = 3;
		nlm_res		NLM_UNLOCK(struct nlm_unlockargs) =	4;

		/*
		 * remote lock manager call-back to grant lock
		 */
		nlm_res		NLM_GRANTED(struct nlm_testargs)= 5;
		/*
		 * message passing style of requesting lock
		 */
		void		NLM_TEST_MSG(struct nlm_testargs) = 6;
		void		NLM_LOCK_MSG(struct nlm_lockargs) = 7;
		void		NLM_CANCEL_MSG(struct nlm_cancargs) =8;
		void		NLM_UNLOCK_MSG(struct nlm_unlockargs) = 9;
		void		NLM_GRANTED_MSG(struct nlm_testargs) = 10;
		void		NLM_TEST_RES(nlm_testres) = 11;
		void		NLM_LOCK_RES(nlm_res) = 12;
		void		NLM_CANCEL_RES(nlm_res) = 13;
		void		NLM_UNLOCK_RES(nlm_res) = 14;
		void		NLM_GRANTED_RES(nlm_res) = 15;
	} = 1;

	version NLM_VERSX {
		nlm_shareres	NLM_SHARE(nlm_shareargs) = 20;
		nlm_shareres	NLM_UNSHARE(nlm_shareargs) = 21;
		nlm_res		NLM_NM_LOCK(nlm_lockargs) = 22;
		void		NLM_FREE_ALL(nlm_notify) = 23;
	} = 3;

} = 100021;
