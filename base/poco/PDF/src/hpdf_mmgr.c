/*
 * << Haru Free PDF Library >> -- hpdf_mmgr.c
 *
 * URL: http://libharu.org
 *
 * Copyright (c) 1999-2006 Takeshi Kanno <takeshi_kanno@est.hi-ho.ne.jp>
 * Copyright (c) 2007-2009 Antony Dovgal <tony@daylessday.org>
 *
 * Permission to use, copy, modify, distribute and sell this software
 * and its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appear in all copies and
 * that both that copyright notice and this permission notice appear
 * in supporting documentation.
 * It is provided "as is" without express or implied warranty.
 *
 */

#include "hpdf_conf.h"
#include "hpdf_consts.h"
#include "hpdf_mmgr.h"
#include "hpdf_utils.h"

#ifndef HPDF_STDCALL
#ifdef HPDF_DLL_MAKE
#define HPDF_STDCALL __stdcall
#else
#ifdef HPDF_DLL
#define HPDF_STDCALL __stdcall
#else
#define HPDF_STDCALL
#endif
#endif
#endif

static void * HPDF_STDCALL
InternalGetMem  (HPDF_UINT  size);

static void HPDF_STDCALL
InternalFreeMem  (void*  aptr);


HPDF_MMgr
HPDF_MMgr_New  (HPDF_Error       error,
                HPDF_UINT        buf_size,
                HPDF_Alloc_Func  alloc_fn,
                HPDF_Free_Func   free_fn)
{
    HPDF_MMgr mmgr;

    HPDF_PTRACE((" HPDF_MMgr_New\n"));

    if (alloc_fn)
        mmgr = (HPDF_MMgr)alloc_fn (sizeof(HPDF_MMgr_Rec));
    else
        mmgr = (HPDF_MMgr)InternalGetMem (sizeof(HPDF_MMgr_Rec));

    HPDF_PTRACE(("+%p mmgr-new\n", mmgr));

    if (mmgr != NULL) {
        /* initialize mmgr object */
        mmgr->error = error;


#ifdef HPDF_MEM_DEBUG
        mmgr->alloc_cnt = 0;
        mmgr->free_cnt = 0;
#endif
        /*
         *  if alloc_fn and free_fn are specified, these function is
         *  used. if not, default function (maybe these will be "malloc" and
         *  "free") is used.
         */
        if (alloc_fn && free_fn) {
            mmgr->alloc_fn = alloc_fn;
            mmgr->free_fn = free_fn;
        } else {
            mmgr->alloc_fn = InternalGetMem;
            mmgr->free_fn = InternalFreeMem;
        }

        /*
         *  if buf_size parameter is specified, this object is configured
         *  to be using memory-pool.
         *
         */
        if (!buf_size)
            mmgr->mpool = NULL;
        else {
            HPDF_MPool_Node node;

            node = (HPDF_MPool_Node)mmgr->alloc_fn (sizeof(HPDF_MPool_Node_Rec) +
                    buf_size);

            HPDF_PTRACE(("+%p mmgr-node-new\n", node));

            if (node == NULL) {
                HPDF_SetError (error, HPDF_FAILD_TO_ALLOC_MEM, HPDF_NOERROR);

                mmgr->free_fn(mmgr);
                mmgr = NULL;
            } else {
                mmgr->mpool = node;
                node->buf = (HPDF_BYTE *)node + sizeof(HPDF_MPool_Node_Rec);
                node->size = buf_size;
                node->used_size = 0;
                node->next_node = NULL;
            }

#ifdef HPDF_MEM_DEBUG
            if (mmgr) {
                mmgr->alloc_cnt += 1;
            }
#endif
        }

        if (mmgr) {
            mmgr->buf_size = buf_size;
        }
    } else
        HPDF_SetError(error, HPDF_FAILD_TO_ALLOC_MEM, HPDF_NOERROR);

    return mmgr;
}

void
HPDF_MMgr_Free  (HPDF_MMgr  mmgr)
{
    HPDF_MPool_Node node;

    HPDF_PTRACE((" HPDF_MMgr_Free\n"));

    if (mmgr == NULL)
        return;

    node = mmgr->mpool;

    /* delete all nodes recursively */
    while (node != NULL) {
        HPDF_MPool_Node tmp = node;
        node = tmp->next_node;

        HPDF_PTRACE(("-%p mmgr-node-free\n", tmp));
        mmgr->free_fn (tmp);

#ifdef HPDF_MEM_DEBUG
        mmgr->free_cnt++;
#endif

    }

#ifdef HPDF_MEM_DEBUG
    HPDF_PRINTF ("# HPDF_MMgr alloc-cnt=%u, free-cnt=%u\n",
            mmgr->alloc_cnt, mmgr->free_cnt);

    if (mmgr->alloc_cnt != mmgr->free_cnt)
        HPDF_PRINTF ("# ERROR #\n");
#endif

    HPDF_PTRACE(("-%p mmgr-free\n", mmgr));
    mmgr->free_fn (mmgr);
}

void*
HPDF_GetMem  (HPDF_MMgr  mmgr,
              HPDF_UINT  size)
{
    void * ptr;

    if (mmgr->mpool) {
        HPDF_MPool_Node node = mmgr->mpool;

#ifdef HPDF_ALINMENT_SIZ
        size = (size + (HPDF_ALINMENT_SIZ - 1)) / HPDF_ALINMENT_SIZ;
        size *= HPDF_ALINMENT_SIZ;
#endif

        if (node->size - node->used_size >= size) {
            ptr = (HPDF_BYTE*)node->buf + node->used_size;
            node->used_size += size;
            return ptr;
        } else {
            HPDF_UINT tmp_buf_siz = (mmgr->buf_size < size) ?  size :
                mmgr->buf_size;

            node = (HPDF_MPool_Node)mmgr->alloc_fn (sizeof(HPDF_MPool_Node_Rec)
                    + tmp_buf_siz);
            HPDF_PTRACE(("+%p mmgr-new-node\n", node));

            if (!node) {
                HPDF_SetError (mmgr->error, HPDF_FAILD_TO_ALLOC_MEM,
                        HPDF_NOERROR);
                return NULL;
            }

            node->size = tmp_buf_siz;
        }

        node->next_node = mmgr->mpool;
        mmgr->mpool = node;
        node->used_size = size;
        node->buf = (HPDF_BYTE*)node + sizeof(HPDF_MPool_Node_Rec);
        ptr = node->buf;
    } else {
        ptr = mmgr->alloc_fn (size);
        HPDF_PTRACE(("+%p mmgr-alloc_fn size=%u\n", ptr, size));

        if (ptr == NULL)
            HPDF_SetError (mmgr->error, HPDF_FAILD_TO_ALLOC_MEM, HPDF_NOERROR);
    }

#ifdef HPDF_MEM_DEBUG
    if (ptr)
        mmgr->alloc_cnt++;
#endif

    return ptr;
}

void
HPDF_FreeMem  (HPDF_MMgr  mmgr,
               void       *aptr)
{
    if (!aptr)
        return;

    if (!mmgr->mpool) {
        HPDF_PTRACE(("-%p mmgr-free-mem\n", aptr));
        mmgr->free_fn(aptr);

#ifdef HPDF_MEM_DEBUG
        mmgr->free_cnt++;
#endif
    }

    return;
}

static void * HPDF_STDCALL
InternalGetMem  (HPDF_UINT  size)
{
    return HPDF_MALLOC (size);
}

static void HPDF_STDCALL
InternalFreeMem  (void*  aptr)
{
    HPDF_FREE (aptr);
}


