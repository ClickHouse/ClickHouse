/*
 * << Haru Free PDF Library >> -- hpdf_page_label.h
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

#ifndef _HPDF_PAGE_LABEL_H
#define _HPDF_PAGE_LABEL_H

#include "hpdf.h"

#ifdef __cplusplus
extern "C" {
#endif

HPDF_Dict
HPDF_PageLabel_New  (HPDF_Doc             pdf,
                     HPDF_PageNumStyle    style,
                     HPDF_INT             first_page,
                     const char          *prefix);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif

