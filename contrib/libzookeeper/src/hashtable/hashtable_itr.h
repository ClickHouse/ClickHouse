/* Copyright (C) 2002, 2004 Christopher Clark <firstname.lastname@cl.cam.ac.uk> */

#ifndef __HASHTABLE_ITR_CWC22__
#define __HASHTABLE_ITR_CWC22__
#include "hashtable.h"
#include "hashtable_private.h" /* needed to enable inlining */

#ifdef __cplusplus
extern "C" {
#endif

/*****************************************************************************/
/* This struct is only concrete here to allow the inlining of two of the
 * accessor functions. */
struct hashtable_itr
{
    struct hashtable *h;
    struct entry *e;
    struct entry *parent;
    unsigned int index;
};


/*****************************************************************************/
/* hashtable_iterator
 */

struct hashtable_itr *
hashtable_iterator(struct hashtable *h);

/*****************************************************************************/
/* hashtable_iterator_key
 * - return the value of the (key,value) pair at the current position */

static inline void *
hashtable_iterator_key(struct hashtable_itr *i)
{
    return i->e->k;
}

/*****************************************************************************/
/* value - return the value of the (key,value) pair at the current position */

static inline void *
hashtable_iterator_value(struct hashtable_itr *i)
{
    return i->e->v;
}

/*****************************************************************************/
/* advance - advance the iterator to the next element
 *           returns zero if advanced to end of table */

int
hashtable_iterator_advance(struct hashtable_itr *itr);

/*****************************************************************************/
/* remove - remove current element and advance the iterator to the next element
 *          NB: if you need the value to free it, read it before
 *          removing. ie: beware memory leaks!
 *          returns zero if advanced to end of table */

int
hashtable_iterator_remove(struct hashtable_itr *itr);

/*****************************************************************************/
/* search - overwrite the supplied iterator, to point to the entry
 *          matching the supplied key.
            h points to the hashtable to be searched.
 *          returns zero if not found. */
int
hashtable_iterator_search(struct hashtable_itr *itr,
                          struct hashtable *h, void *k);

#define DEFINE_HASHTABLE_ITERATOR_SEARCH(fnname, keytype) \
int fnname (struct hashtable_itr *i, struct hashtable *h, keytype *k) \
{ \
    return (hashtable_iterator_search(i,h,k)); \
}


#ifdef __cplusplus
}
#endif

#endif /* __HASHTABLE_ITR_CWC22__*/

/*
 * Copyright (c) 2002, 2004, Christopher Clark
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 
 * * Neither the name of the original author; nor the names of any contributors
 * may be used to endorse or promote products derived from this software
 * without specific prior written permission.
 * 
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
