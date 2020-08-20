/*-
 * SPDX-License-Identifier: BSD-2-Clause-FreeBSD
 *
 * Copyright (c) 2007 Ulf Lilleengen
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * $FreeBSD$
 */

#include <sys/param.h>
#define _KERNEL
#include <sys/mount.h>
#undef _KERNEL
#include <sys/queue.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <sys/time.h>
#include <sys/vnode.h>

#include <netinet/in.h>

#include <err.h>
#include <kvm.h>
#include <stdio.h>
#include <stdlib.h>

#define ZFS
#include "libprocstat.h"
#include "common_kvm.h"
#include "zfs_defs.h"

int
zfs_filestat(kvm_t *kd, struct vnode *vp, struct vnstat *vn)
{

	struct mount mount, *mountptr;
	void *znodeptr;
	char *dataptr;
	size_t len;
	int size;

	len = sizeof(size);
	if (sysctlbyname("debug.sizeof.znode", &size, &len, NULL, 0) == -1) {
		warnx("error getting sysctl");
		return (1);
	}
	dataptr = malloc(size);
	if (dataptr == NULL) {
		warnx("error allocating memory for znode storage");
		return (1);
	}

	if ((size_t)size < offsetof_z_id + sizeof(uint64_t) ||
	    (size_t)size < offsetof_z_mode + sizeof(mode_t) ||
	    (size_t)size < offsetof_z_size + sizeof(uint64_t)) {
		warnx("znode_t size is too small");
		goto bad;
	}

	if ((size_t)size != sizeof_znode_t)
		warnx("znode_t size mismatch, data could be wrong");

	/* Since we have problems including vnode.h, we'll use the wrappers. */
	znodeptr = getvnodedata(vp);
	if (!kvm_read_all(kd, (unsigned long)znodeptr, dataptr,
	    (size_t)size)) {
		warnx("can't read znode at %p", (void *)znodeptr);
		goto bad;
	}

	/* Get the mount pointer, and read from the address. */
	mountptr = getvnodemount(vp);
	if (!kvm_read_all(kd, (unsigned long)mountptr, &mount, sizeof(mount))) {
		warnx("can't read mount at %p", (void *)mountptr);
		goto bad;
	}

	/*
	 * XXX Assume that this is a znode, but it can be a special node
	 * under .zfs/.
	 */
	vn->vn_fsid = mount.mnt_stat.f_fsid.val[0];
	vn->vn_fileid = *(uint64_t *)(void *)(dataptr + offsetof_z_id);
	vn->vn_mode = *(mode_t *)(void *)(dataptr + offsetof_z_mode);
	vn->vn_size = *(uint64_t *)(void *)(dataptr + offsetof_z_size);
	free(dataptr);
	return (0);
bad:
	free(dataptr);
	return (1);
}
