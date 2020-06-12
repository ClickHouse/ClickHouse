/* $OpenBSD: o_names.c,v 1.22 2017/01/29 17:49:23 beck Exp $ */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <openssl/opensslconf.h>

#include <openssl/err.h>
#include <openssl/lhash.h>
#include <openssl/objects.h>
#include <openssl/safestack.h>

/* I use the ex_data stuff to manage the identifiers for the obj_name_types
 * that applications may define.  I only really use the free function field.
 */
DECLARE_LHASH_OF(OBJ_NAME);
static LHASH_OF(OBJ_NAME) *names_lh = NULL;
static int names_type_num = OBJ_NAME_TYPE_NUM;

typedef struct name_funcs_st {
	unsigned long (*hash_func)(const char *name);
	int (*cmp_func)(const char *a, const char *b);
	void (*free_func)(const char *, int, const char *);
} NAME_FUNCS;

DECLARE_STACK_OF(NAME_FUNCS)

static STACK_OF(NAME_FUNCS) *name_funcs_stack;

/* The LHASH callbacks now use the raw "void *" prototypes and do per-variable
 * casting in the functions. This prevents function pointer casting without the
 * need for macro-generated wrapper functions. */

/* static unsigned long obj_name_hash(OBJ_NAME *a); */
static unsigned long obj_name_hash(const void *a_void);
/* static int obj_name_cmp(OBJ_NAME *a,OBJ_NAME *b); */
static int obj_name_cmp(const void *a_void, const void *b_void);

static IMPLEMENT_LHASH_HASH_FN(obj_name, OBJ_NAME)
static IMPLEMENT_LHASH_COMP_FN(obj_name, OBJ_NAME)

int
OBJ_NAME_init(void)
{
	if (names_lh != NULL)
		return (1);
	names_lh = lh_OBJ_NAME_new();
	return (names_lh != NULL);
}

int
OBJ_NAME_new_index(unsigned long (*hash_func)(const char *),
    int (*cmp_func)(const char *, const char *),
    void (*free_func)(const char *, int, const char *))
{
	int ret;
	int i;
	NAME_FUNCS *name_funcs;

	if (name_funcs_stack == NULL)
		name_funcs_stack = sk_NAME_FUNCS_new_null();
	if (name_funcs_stack == NULL)
		return (0);

	ret = names_type_num;
	names_type_num++;
	for (i = sk_NAME_FUNCS_num(name_funcs_stack); i < names_type_num; i++) {
		name_funcs = malloc(sizeof(NAME_FUNCS));
		if (!name_funcs) {
			OBJerror(ERR_R_MALLOC_FAILURE);
			return (0);
		}
		name_funcs->hash_func = lh_strhash;
		name_funcs->cmp_func = strcmp;
		name_funcs->free_func = NULL;
		if (sk_NAME_FUNCS_push(name_funcs_stack, name_funcs) == 0) {
			free(name_funcs);
			OBJerror(ERR_R_MALLOC_FAILURE);
			return (0);
		}
	}
	name_funcs = sk_NAME_FUNCS_value(name_funcs_stack, ret);
	if (hash_func != NULL)
		name_funcs->hash_func = hash_func;
	if (cmp_func != NULL)
		name_funcs->cmp_func = cmp_func;
	if (free_func != NULL)
		name_funcs->free_func = free_func;
	return (ret);
}

/* static int obj_name_cmp(OBJ_NAME *a, OBJ_NAME *b) */
static int
obj_name_cmp(const void *a_void, const void *b_void)
{
	int ret;
	const OBJ_NAME *a = (const OBJ_NAME *)a_void;
	const OBJ_NAME *b = (const OBJ_NAME *)b_void;

	ret = a->type - b->type;
	if (ret == 0) {
		if ((name_funcs_stack != NULL) &&
		    (sk_NAME_FUNCS_num(name_funcs_stack) > a->type)) {
			ret = sk_NAME_FUNCS_value(name_funcs_stack,
			    a->type)->cmp_func(a->name, b->name);
		} else
			ret = strcmp(a->name, b->name);
	}
	return (ret);
}

/* static unsigned long obj_name_hash(OBJ_NAME *a) */
static unsigned long
obj_name_hash(const void *a_void)
{
	unsigned long ret;
	const OBJ_NAME *a = (const OBJ_NAME *)a_void;

	if ((name_funcs_stack != NULL) &&
	    (sk_NAME_FUNCS_num(name_funcs_stack) > a->type)) {
		ret = sk_NAME_FUNCS_value(name_funcs_stack,
		    a->type)->hash_func(a->name);
	} else {
		ret = lh_strhash(a->name);
	}
	ret ^= a->type;
	return (ret);
}

const char *
OBJ_NAME_get(const char *name, int type)
{
	OBJ_NAME on, *ret;
	int num = 0, alias;

	if (name == NULL)
		return (NULL);
	if ((names_lh == NULL) && !OBJ_NAME_init())
		return (NULL);

	alias = type&OBJ_NAME_ALIAS;
	type&= ~OBJ_NAME_ALIAS;

	on.name = name;
	on.type = type;

	for (;;) {
		ret = lh_OBJ_NAME_retrieve(names_lh, &on);
		if (ret == NULL)
			return (NULL);
		if ((ret->alias) && !alias) {
			if (++num > 10)
				return (NULL);
			on.name = ret->data;
		} else {
			return (ret->data);
		}
	}
}

int
OBJ_NAME_add(const char *name, int type, const char *data)
{
	OBJ_NAME *onp, *ret;
	int alias;

	if ((names_lh == NULL) && !OBJ_NAME_init())
		return (0);

	alias = type & OBJ_NAME_ALIAS;
	type &= ~OBJ_NAME_ALIAS;

	onp = malloc(sizeof(OBJ_NAME));
	if (onp == NULL) {
		/* ERROR */
		return (0);
	}

	onp->name = name;
	onp->alias = alias;
	onp->type = type;
	onp->data = data;

	ret = lh_OBJ_NAME_insert(names_lh, onp);
	if (ret != NULL) {
		/* free things */
		if ((name_funcs_stack != NULL) &&
		    (sk_NAME_FUNCS_num(name_funcs_stack) > ret->type)) {
			/* XXX: I'm not sure I understand why the free
			 * function should get three arguments...
			 * -- Richard Levitte
			 */
			sk_NAME_FUNCS_value(
			    name_funcs_stack, ret->type)->free_func(
			    ret->name, ret->type, ret->data);
		}
		free(ret);
	} else {
		if (lh_OBJ_NAME_error(names_lh)) {
			/* ERROR */
			return (0);
		}
	}
	return (1);
}

int
OBJ_NAME_remove(const char *name, int type)
{
	OBJ_NAME on, *ret;

	if (names_lh == NULL)
		return (0);

	type &= ~OBJ_NAME_ALIAS;
	on.name = name;
	on.type = type;
	ret = lh_OBJ_NAME_delete(names_lh, &on);
	if (ret != NULL) {
		/* free things */
		if ((name_funcs_stack != NULL) &&
		    (sk_NAME_FUNCS_num(name_funcs_stack) > ret->type)) {
			/* XXX: I'm not sure I understand why the free
			 * function should get three arguments...
			 * -- Richard Levitte
			 */
			sk_NAME_FUNCS_value(
			    name_funcs_stack, ret->type)->free_func(
			    ret->name, ret->type, ret->data);
		}
		free(ret);
		return (1);
	} else
		return (0);
}

struct doall {
	int type;
	void (*fn)(const OBJ_NAME *, void *arg);
	void *arg;
};

static void
do_all_fn_doall_arg(const OBJ_NAME *name, struct doall *d)
{
	if (name->type == d->type)
		d->fn(name, d->arg);
}

static IMPLEMENT_LHASH_DOALL_ARG_FN(do_all_fn, const OBJ_NAME, struct doall)

void
OBJ_NAME_do_all(int type, void (*fn)(const OBJ_NAME *, void *arg), void *arg)
{
	struct doall d;

	d.type = type;
	d.fn = fn;
	d.arg = arg;

	lh_OBJ_NAME_doall_arg(names_lh, LHASH_DOALL_ARG_FN(do_all_fn),
	    struct doall, &d);
}

struct doall_sorted {
	int type;
	int n;
	const OBJ_NAME **names;
};

static void
do_all_sorted_fn(const OBJ_NAME *name, void *d_)
{
	struct doall_sorted *d = d_;

	if (name->type != d->type)
		return;

	d->names[d->n++] = name;
}

static int
do_all_sorted_cmp(const void *n1_, const void *n2_)
{
	const OBJ_NAME * const *n1 = n1_;
	const OBJ_NAME * const *n2 = n2_;

	return strcmp((*n1)->name, (*n2)->name);
}

void
OBJ_NAME_do_all_sorted(int type, void (*fn)(const OBJ_NAME *, void *arg),
    void *arg)
{
	struct doall_sorted d;
	int n;

	d.type = type;
	d.names = reallocarray(NULL, lh_OBJ_NAME_num_items(names_lh),
	    sizeof *d.names);
	d.n = 0;
	if (d.names != NULL) {
		OBJ_NAME_do_all(type, do_all_sorted_fn, &d);

		qsort((void *)d.names, d.n, sizeof *d.names, do_all_sorted_cmp);

		for (n = 0; n < d.n; ++n)
			fn(d.names[n], arg);

		free(d.names);
	}
}

static int free_type;

static void
names_lh_free_doall(OBJ_NAME *onp)
{
	if (onp == NULL)
		return;

	if (free_type < 0 || free_type == onp->type)
		OBJ_NAME_remove(onp->name, onp->type);
}

static IMPLEMENT_LHASH_DOALL_FN(names_lh_free, OBJ_NAME)

static void
name_funcs_free(NAME_FUNCS *ptr)
{
	free(ptr);
}

void
OBJ_NAME_cleanup(int type)
{
	unsigned long down_load;

	if (names_lh == NULL)
		return;

	free_type = type;
	down_load = lh_OBJ_NAME_down_load(names_lh);
	lh_OBJ_NAME_down_load(names_lh) = 0;

	lh_OBJ_NAME_doall(names_lh, LHASH_DOALL_FN(names_lh_free));
	if (type < 0) {
		lh_OBJ_NAME_free(names_lh);
		sk_NAME_FUNCS_pop_free(name_funcs_stack, name_funcs_free);
		names_lh = NULL;
		name_funcs_stack = NULL;
	} else
		lh_OBJ_NAME_down_load(names_lh) = down_load;
}
