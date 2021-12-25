#ifndef _SYS_PROCFS_H
#define _SYS_PROCFS_H
#ifdef __cplusplus
extern "C" {
#endif

#include <sys/time.h>
#include <sys/types.h>
#include <sys/user.h>

struct elf_siginfo {
	int si_signo;
	int si_code;
	int si_errno;
};

struct elf_prstatus {
	struct elf_siginfo pr_info;
	short int pr_cursig;
	unsigned long int pr_sigpend;
	unsigned long int pr_sighold;
	pid_t pr_pid;
	pid_t pr_ppid;
	pid_t pr_pgrp;
	pid_t pr_sid;
	struct {
		long tv_sec, tv_usec;
	} pr_utime, pr_stime, pr_cutime, pr_cstime;
	elf_gregset_t pr_reg;
	int pr_fpvalid;
};

#define ELF_PRARGSZ 80

struct elf_prpsinfo {
	char pr_state;
	char pr_sname;
	char pr_zomb;
	char pr_nice;
	unsigned long int pr_flag;
#if UINTPTR_MAX == 0xffffffff
	unsigned short int pr_uid;
	unsigned short int pr_gid;
#else
	unsigned int pr_uid;
	unsigned int pr_gid;
#endif
	int pr_pid, pr_ppid, pr_pgrp, pr_sid;
	char pr_fname[16];
	char pr_psargs[ELF_PRARGSZ];
};

typedef void *psaddr_t;
typedef elf_gregset_t prgregset_t;
typedef elf_fpregset_t prfpregset_t;
typedef pid_t lwpid_t;
typedef struct elf_prstatus prstatus_t;
typedef struct elf_prpsinfo prpsinfo_t;

#ifdef __cplusplus
}
#endif
#endif
