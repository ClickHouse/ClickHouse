/*
 * << Haru Free PDF Library >> -- hpdf_list.h
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

#ifndef _HPDF_LIST_H
#define _HPDF_LIST_H

#include "hpdf_error.h"
#include "hpdf_mmgr.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _HPDF_List_Rec  *HPDF_List;

typedef struct _HPDF_List_Rec {
      HPDF_MMgr       mmgr;
      HPDF_Error      error;
      HPDF_UINT       block_siz;
      HPDF_UINT       items_per_block;
      HPDF_UINT       count;
      void            **obj;
} HPDF_List_Rec;


HPDF_List
HPDF_List_New  (HPDF_MMgr  mmgr,
                HPDF_UINT  items_per_block);


void
HPDF_List_Free  (HPDF_List  list);


HPDF_STATUS
HPDF_List_Add  (HPDF_List  list,
                void       *item);


HPDF_STATUS
HPDF_List_Insert  (HPDF_List  list,
                   void       *target,
                   void       *item);


HPDF_STATUS
HPDF_List_Remove  (HPDF_List  list,
                   void       *item);


void*
HPDF_List_RemoveByIndex  (HPDF_List  list,
                          HPDF_UINT  index);


void*
HPDF_List_ItemAt  (HPDF_List  list,
                   HPDF_UINT  index);


HPDF_INT32
HPDF_List_Find  (HPDF_List  list,
                 void       *item);


void
HPDF_List_Clear  (HPDF_List  list);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_LIST_H */

