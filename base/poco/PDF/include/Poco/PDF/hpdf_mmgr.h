/*
 * << Haru Free PDF Library >> -- hpdf_mmgr.h
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

#ifndef _HPDF_MMGR_H
#define _HPDF_MMGR_H

#include "hpdf_types.h"
#include "hpdf_error.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct  _HPDF_MPool_Node_Rec  *HPDF_MPool_Node;

typedef struct  _HPDF_MPool_Node_Rec {
    HPDF_BYTE*       buf;
    HPDF_UINT        size;
    HPDF_UINT        used_size;
    HPDF_MPool_Node  next_node;
} HPDF_MPool_Node_Rec;


typedef struct  _HPDF_MMgr_Rec  *HPDF_MMgr;

typedef struct  _HPDF_MMgr_Rec {
    HPDF_Error        error;
    HPDF_Alloc_Func   alloc_fn;
    HPDF_Free_Func    free_fn;
    HPDF_MPool_Node   mpool;
    HPDF_UINT         buf_size;

#ifdef HPDF_MEM_DEBUG
    HPDF_UINT         alloc_cnt;
    HPDF_UINT         free_cnt;
#endif
} HPDF_MMgr_Rec;


/*  HPDF_mpool_new
 *
 *  create new HPDF_mpool object. when memory allocation goes wrong,
 *  it returns NULL and error handling function will be called.
 *  if buf_size is non-zero, mmgr is configured to be using memory-pool
 */
HPDF_MMgr
HPDF_MMgr_New  (HPDF_Error       error,
                HPDF_UINT        buf_size,
                HPDF_Alloc_Func  alloc_fn,
                HPDF_Free_Func   free_fn);


void
HPDF_MMgr_Free  (HPDF_MMgr  mmgr);


void*
HPDF_GetMem  (HPDF_MMgr  mmgr,
              HPDF_UINT  size);


void
HPDF_FreeMem  (HPDF_MMgr  mmgr,
               void       *aptr);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_MMGR_H */

