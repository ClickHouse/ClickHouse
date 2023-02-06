/*
 * << Haru Free PDF Library >> -- hpdf_catalog.h
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

#ifndef _HPDF_CATALOG_H
#define _HPDF_CATALOG_H

#include "hpdf_objects.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef HPDF_Dict HPDF_Catalog;

HPDF_Catalog
HPDF_Catalog_New  (HPDF_MMgr  mmgr,
                   HPDF_Xref  xref);


HPDF_NameDict
HPDF_Catalog_GetNames  (HPDF_Catalog  catalog);


HPDF_STATUS
HPDF_Catalog_SetNames  (HPDF_Catalog  catalog,
                        HPDF_NameDict dict);


HPDF_Pages
HPDF_Catalog_GetRoot  (HPDF_Catalog  catalog);


HPDF_PageLayout
HPDF_Catalog_GetPageLayout  (HPDF_Catalog  catalog);


HPDF_STATUS
HPDF_Catalog_SetPageLayout  (HPDF_Catalog      catalog,
                             HPDF_PageLayout   layout);


HPDF_PageMode
HPDF_Catalog_GetPageMode  (HPDF_Catalog  catalog);


HPDF_STATUS
HPDF_Catalog_SetPageMode  (HPDF_Catalog   catalog,
                           HPDF_PageMode  mode);


HPDF_STATUS
HPDF_Catalog_SetOpenAction  (HPDF_Catalog       catalog,
                             HPDF_Destination   open_action);


HPDF_STATUS
HPDF_Catalog_AddPageLabel  (HPDF_Catalog   catalog,
                            HPDF_UINT      page_num,
                            HPDF_Dict      page_label);


HPDF_UINT
HPDF_Catalog_GetViewerPreference  (HPDF_Catalog   catalog);


HPDF_STATUS
HPDF_Catalog_SetViewerPreference  (HPDF_Catalog   catalog,
                                   HPDF_UINT      value);


HPDF_BOOL
HPDF_Catalog_Validate  (HPDF_Catalog  catalog);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_CATALOG_H */

