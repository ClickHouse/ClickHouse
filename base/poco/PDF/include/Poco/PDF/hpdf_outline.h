/*
 * << Haru Free PDF Library >> -- hpdf_outline.h
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

#ifndef _HPDF_OUTLINE_H
#define _HPDF_OUTLINE_H

#include "hpdf_objects.h"

#ifdef __cplusplus
extern "C" {
#endif


/*----------------------------------------------------------------------------*/
/*----- HPDF_Outline ---------------------------------------------------------*/

HPDF_Outline
HPDF_OutlineRoot_New  (HPDF_MMgr   mmgr,
                       HPDF_Xref   xref);


HPDF_Outline
HPDF_Outline_New  (HPDF_MMgr          mmgr,
                   HPDF_Outline       parent,
                   const char   *title,
                   HPDF_Encoder       encoder,
                   HPDF_Xref          xref);


HPDF_Outline
HPDF_Outline_GetFirst (HPDF_Outline outline);


HPDF_Outline
HPDF_Outline_GetLast (HPDF_Outline outline);


HPDF_Outline
HPDF_Outline_GetPrev(HPDF_Outline outline);


HPDF_Outline
HPDF_Outline_GetNext (HPDF_Outline outline);


HPDF_Outline
HPDF_Outline_GetParent (HPDF_Outline outline);


HPDF_BOOL
HPDF_Outline_GetOpened  (HPDF_Outline  outline);



HPDF_BOOL
HPDF_Outline_Validate (HPDF_Outline  obj);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_OUTLINE_H */

