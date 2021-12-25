#ifndef _CRYPT_H
#define _CRYPT_H

#ifdef __cplusplus
extern "C" {
#endif

struct crypt_data {
	int initialized;
	char __buf[256];
};

char *crypt(const char *, const char *);
char *crypt_r(const char *, const char *, struct crypt_data *);

#ifdef __cplusplus
}
#endif

#endif
