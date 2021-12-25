#ifndef _SPAWN_H
#define _SPAWN_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#define __NEED_mode_t
#define __NEED_pid_t
#define __NEED_sigset_t

#include <bits/alltypes.h>

struct sched_param;

#define POSIX_SPAWN_RESETIDS 1
#define POSIX_SPAWN_SETPGROUP 2
#define POSIX_SPAWN_SETSIGDEF 4
#define POSIX_SPAWN_SETSIGMASK 8
#define POSIX_SPAWN_SETSCHEDPARAM 16
#define POSIX_SPAWN_SETSCHEDULER 32
#define POSIX_SPAWN_USEVFORK 64
#define POSIX_SPAWN_SETSID 128

typedef struct {
	int __flags;
	pid_t __pgrp;
	sigset_t __def, __mask;
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
int posix_spawnp(pid_t *__restrict, const char *__restrict, const posix_spawn_file_actions_t *,
	const posix_spawnattr_t *__restrict, char *const *__restrict, char *const *__restrict);

int posix_spawnattr_init(posix_spawnattr_t *);
int posix_spawnattr_destroy(posix_spawnattr_t *);

int posix_spawnattr_setflags(posix_spawnattr_t *, short);
int posix_spawnattr_getflags(const posix_spawnattr_t *__restrict, short *__restrict);

int posix_spawnattr_setpgroup(posix_spawnattr_t *, pid_t);
int posix_spawnattr_getpgroup(const posix_spawnattr_t *__restrict, pid_t *__restrict);

int posix_spawnattr_setsigmask(posix_spawnattr_t *__restrict, const sigset_t *__restrict);
int posix_spawnattr_getsigmask(const posix_spawnattr_t *__restrict, sigset_t *__restrict);

int posix_spawnattr_setsigdefault(posix_spawnattr_t *__restrict, const sigset_t *__restrict);
int posix_spawnattr_getsigdefault(const posix_spawnattr_t *__restrict, sigset_t *__restrict);

int posix_spawnattr_setschedparam(posix_spawnattr_t *__restrict, const struct sched_param *__restrict);
int posix_spawnattr_getschedparam(const posix_spawnattr_t *__restrict, struct sched_param *__restrict);
int posix_spawnattr_setschedpolicy(posix_spawnattr_t *, int);
int posix_spawnattr_getschedpolicy(const posix_spawnattr_t *__restrict, int *__restrict);

int posix_spawn_file_actions_init(posix_spawn_file_actions_t *);
int posix_spawn_file_actions_destroy(posix_spawn_file_actions_t *);

int posix_spawn_file_actions_addopen(posix_spawn_file_actions_t *__restrict, int, const char *__restrict, int, mode_t);
int posix_spawn_file_actions_addclose(posix_spawn_file_actions_t *, int);
int posix_spawn_file_actions_adddup2(posix_spawn_file_actions_t *, int, int);

#if defined(_BSD_SOURCE) || defined(_GNU_SOURCE)
int posix_spawn_file_actions_addchdir_np(posix_spawn_file_actions_t *__restrict, const char *__restrict);
int posix_spawn_file_actions_addfchdir_np(posix_spawn_file_actions_t *, int);
#endif

#ifdef __cplusplus
}
#endif

#endif
