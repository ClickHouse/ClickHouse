#ifndef _SYS_ACCT_H
#define _SYS_ACCT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>
#include <time.h>
#include <stdint.h>

#define ACCT_COMM 16

typedef uint16_t comp_t;

struct acct {
	char ac_flag;
	uint16_t ac_uid;
	uint16_t ac_gid;
	uint16_t ac_tty;
	uint32_t ac_btime;
	comp_t ac_utime;
	comp_t ac_stime;
	comp_t ac_etime;
	comp_t ac_mem;
	comp_t ac_io;
	comp_t ac_rw;
	comp_t ac_minflt;
	comp_t ac_majflt;
	comp_t ac_swaps;
	uint32_t ac_exitcode;
	char ac_comm[ACCT_COMM+1];
	char ac_pad[10];
};


struct acct_v3 {
	char ac_flag;
	char ac_version;
	uint16_t ac_tty;
	uint32_t ac_exitcode;
	uint32_t ac_uid;
	uint32_t ac_gid;
	uint32_t ac_pid;
	uint32_t ac_ppid;
	uint32_t ac_btime;
	float ac_etime;
	comp_t ac_utime;
	comp_t ac_stime;
	comp_t ac_mem;
	comp_t ac_io;
	comp_t ac_rw;
	comp_t ac_minflt;
	comp_t ac_majflt;
	comp_t ac_swaps;
	char ac_comm[ACCT_COMM];
};

#define AFORK 1
#define ASU 2
#define ACORE 8
#define AXSIG 16
#define ACCT_BYTEORDER (128*(__BYTE_ORDER==__BIG_ENDIAN))
#define AHZ 100

int acct(const char *);

#ifdef __cplusplus
}
#endif

#endif
