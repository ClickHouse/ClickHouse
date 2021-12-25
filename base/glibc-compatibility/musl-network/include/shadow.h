#ifndef _SHADOW_H
#define _SHADOW_H

#ifdef __cplusplus
extern "C" {
#endif

#define	__NEED_FILE
#define __NEED_size_t

#include <bits/alltypes.h>

#define	SHADOW "/etc/shadow"

struct spwd {
	char *sp_namp;
	char *sp_pwdp;
	long sp_lstchg;
	long sp_min;
	long sp_max;
	long sp_warn;
	long sp_inact;
	long sp_expire;
	unsigned long sp_flag;
};

void setspent(void);
void endspent(void);
struct spwd *getspent(void);
struct spwd *fgetspent(FILE *);
struct spwd *sgetspent(const char *);
int putspent(const struct spwd *, FILE *);

struct spwd *getspnam(const char *);
int getspnam_r(const char *, struct spwd *, char *, size_t, struct spwd **);

int lckpwdf(void);
int ulckpwdf(void);

#ifdef __cplusplus
}
#endif

#endif
