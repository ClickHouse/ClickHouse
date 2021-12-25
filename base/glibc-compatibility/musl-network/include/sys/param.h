#ifndef _SYS_PARAM_H
#define _SYS_PARAM_H

#define MAXSYMLINKS 20
#define MAXHOSTNAMELEN 64
#define MAXNAMLEN 255
#define MAXPATHLEN 4096
#define NBBY 8
#define NGROUPS 32
#define CANBSIZ 255
#define NOFILE 256
#define NCARGS 131072
#define DEV_BSIZE 512
#define NOGROUP (-1)

#undef MIN
#undef MAX
#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

#define __bitop(x,i,o) ((x)[(i)/8] o (1<<(i)%8))
#define setbit(x,i) __bitop(x,i,|=)
#define clrbit(x,i) __bitop(x,i,&=~)
#define isset(x,i) __bitop(x,i,&)
#define isclr(x,i) !isset(x,i)

#define howmany(n,d) (((n)+((d)-1))/(d))
#define roundup(n,d) (howmany(n,d)*(d))
#define powerof2(n) !(((n)-1) & (n))

#include <sys/resource.h>
#include <endian.h>
#include <limits.h>

#endif
