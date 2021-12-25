#ifndef _SYS_FILE_H
#define _SYS_FILE_H
#ifdef __cplusplus
extern "C" {
#endif

#define LOCK_SH	1
#define LOCK_EX	2
#define LOCK_NB	4
#define LOCK_UN	8

#define L_SET 0
#define L_INCR 1
#define L_XTND 2

int flock(int, int);

#ifdef __cplusplus
}
#endif
#endif
