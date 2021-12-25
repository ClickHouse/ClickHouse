#ifndef _SYS_SIGNALFD_H
#define _SYS_SIGNALFD_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <fcntl.h>

#define __NEED_sigset_t

#include <bits/alltypes.h>

#define SFD_CLOEXEC O_CLOEXEC
#define SFD_NONBLOCK O_NONBLOCK

int signalfd(int, const sigset_t *, int);

struct signalfd_siginfo {
	uint32_t  ssi_signo;
	int32_t   ssi_errno;
	int32_t   ssi_code;
	uint32_t  ssi_pid;
	uint32_t  ssi_uid;
	int32_t   ssi_fd;
	uint32_t  ssi_tid;
	uint32_t  ssi_band;
	uint32_t  ssi_overrun;
	uint32_t  ssi_trapno;
	int32_t   ssi_status;
	int32_t   ssi_int;
	uint64_t  ssi_ptr;
	uint64_t  ssi_utime;
	uint64_t  ssi_stime;
	uint64_t  ssi_addr;
	uint16_t  ssi_addr_lsb;
	uint16_t  __pad2;
	int32_t   ssi_syscall;
	uint64_t  ssi_call_addr;
	uint32_t  ssi_arch;
	uint8_t   __pad[128-14*4-5*8-2*2];
};

#ifdef __cplusplus
}
#endif

#endif
