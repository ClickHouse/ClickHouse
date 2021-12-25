#ifndef _SEARCH_H
#define _SEARCH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#define __NEED_size_t
#include <bits/alltypes.h>

typedef enum { FIND, ENTER } ACTION;
typedef enum { preorder, postorder, endorder, leaf } VISIT;

typedef struct entry {
	char *key;
	void *data;
} ENTRY;

int hcreate(size_t);
void hdestroy(void);
ENTRY *hsearch(ENTRY, ACTION);

#ifdef _GNU_SOURCE
struct hsearch_data {
	struct __tab *__tab;
	unsigned int __unused1;
	unsigned int __unused2;
};

int hcreate_r(size_t, struct hsearch_data *);
void hdestroy_r(struct hsearch_data *);
int hsearch_r(ENTRY, ACTION, ENTRY **, struct hsearch_data *);
#endif

void insque(void *, void *);
void remque(void *);

void *lsearch(const void *, void *, size_t *, size_t,
	int (*)(const void *, const void *));
void *lfind(const void *, const void *, size_t *, size_t,
	int (*)(const void *, const void *));

void *tdelete(const void *__restrict, void **__restrict, int(*)(const void *, const void *));
void *tfind(const void *, void *const *, int(*)(const void *, const void *));
void *tsearch(const void *, void **, int (*)(const void *, const void *));
void twalk(const void *, void (*)(const void *, VISIT, int));

#ifdef _GNU_SOURCE
struct qelem {
	struct qelem *q_forw, *q_back;
	char q_data[1];
};

void tdestroy(void *, void (*)(void *));
#endif

#ifdef __cplusplus
}
#endif

#endif
