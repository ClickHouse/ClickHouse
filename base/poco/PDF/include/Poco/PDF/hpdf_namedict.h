/*
 * << Haru Free PDF Library >> -- hpdf_namedict.h
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

#ifndef _HPDF_NAMEDICT_H
#define _HPDF_NAMEDICT_H

#include "hpdf_objects.h"

#ifdef __cplusplus
extern "C" {
#endif


HPDF_NameDict
HPDF_NameDict_New  (HPDF_MMgr  mmgr,
                    HPDF_Xref  xref);

HPDF_NameTree
HPDF_NameDict_GetNameTree  (HPDF_NameDict     namedict,
                            HPDF_NameDictKey  key);

HPDF_STATUS
HPDF_NameDict_SetNameTree  (HPDF_NameDict     namedict,
                            HPDF_NameDictKey  key,
                            HPDF_NameTree     tree);

HPDF_BOOL
HPDF_NameDict_Validate  (HPDF_NameDict  namedict);


/*------- NameTree -------*/

HPDF_NameTree
HPDF_NameTree_New  (HPDF_MMgr  mmgr,
                    HPDF_Xref  xref);

HPDF_STATUS
HPDF_NameTree_Add  (HPDF_NameTree  tree,
                    HPDF_String    name,
                    void          *obj);

HPDF_BOOL
HPDF_NameTree_Validate  (HPDF_NameTree  tree);


/*------- EmbeddedFile -------*/

HPDF_EmbeddedFile
HPDF_EmbeddedFile_New  (HPDF_MMgr  mmgr,
                        HPDF_Xref  xref,
                        const char *file);

HPDF_BOOL
HPDF_EmbeddedFile_Validate  (HPDF_EmbeddedFile  emfile);


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_NAMEDICT_H */

