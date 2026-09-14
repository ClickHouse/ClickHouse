#ifndef _SPAWN_H
#define _SPAWN_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>
#include <sys/types.h>
#include <bits/types/sigset_t.h>

#define POSIX_SPAWN_SETSIGMASK		0x08

typedef struct {
	int __flags;
	pid_t __pgrp;
	sigset_t __sd;
	sigset_t __ss;
	int __prio, __pol;
	void *__fn;
	char __pad[64-sizeof(void *)];
} posix_spawnattr_t;

typedef struct {
	int __pad0[2];
	void *__actions;
	int __pad[16];
} posix_spawn_file_actions_t;

int posix_spawn(pid_t *__restrict, const char *__restrict, const posix_spawn_file_actions_t *,
	const posix_spawnattr_t *__restrict, char *const *__restrict, char *const *__restrict);

#ifdef __cplusplus
}
#endif

#endif
