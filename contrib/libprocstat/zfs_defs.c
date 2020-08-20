/*-
 * SPDX-License-Identifier: BSD-2-Clause-FreeBSD
 *
 * Copyright (c) 2020 Andriy Gapon <avg@FreeBSD.org>
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
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <sys/cdefs.h>
__FBSDID("$FreeBSD$");

/* Pretend we are kernel to get the same binary layout. */
#define _KERNEL

/* A hack to deal with kpilite.h. */
#define KLD_MODULE

/*
 * Prevent some headers from getting included and fake some types
 * in order to allow this file to compile without bringing in
 * too many kernel build dependencies.
 */
#define _OPENSOLARIS_SYS_PATHNAME_H_
#define _OPENSOLARIS_SYS_POLICY_H_
#define _OPENSOLARIS_SYS_VNODE_H_
#define _VNODE_PAGER_

typedef struct vnode vnode_t;
typedef struct vattr vattr_t;
typedef struct xvattr xvattr_t;
typedef struct vsecattr vsecattr_t;
typedef enum vtype vtype_t;

#include <sys/zfs_context.h>
#include <sys/zfs_znode.h>

size_t sizeof_znode_t = sizeof(znode_t);
size_t offsetof_z_id = offsetof(znode_t, z_id);
size_t offsetof_z_size = offsetof(znode_t, z_size);
size_t offsetof_z_mode = offsetof(znode_t, z_mode);

/* Keep pcpu.h satisfied. */
uintptr_t *__start_set_pcpu;
uintptr_t *__stop_set_pcpu;
