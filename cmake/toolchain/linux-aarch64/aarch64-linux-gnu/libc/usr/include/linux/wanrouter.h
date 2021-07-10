/* SPDX-License-Identifier: GPL-2.0 WITH Linux-syscall-note */
/*
 * wanrouter.h	Legacy declarations kept around until X25 is removed
 */

#ifndef _ROUTER_H
#define _ROUTER_H

/* 'state' defines */
enum wan_states
{
	WAN_UNCONFIGURED,	/* link/channel is not configured */
	WAN_DISCONNECTED,	/* link/channel is disconnected */
	WAN_CONNECTING,		/* connection is in progress */
	WAN_CONNECTED		/* link/channel is operational */
};

#endif /* _ROUTER_H */
