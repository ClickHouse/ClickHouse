/*
 * << Haru Free PDF Library >> -- hpdf_ext_gstate.h
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

#ifndef _HPDF_EXT_GSTATE_H
#define _HPDF_EXT_GSTATE_H

#include "hpdf_objects.h"

#ifdef __cplusplus
extern "C" {
#endif

HPDF_Dict
HPDF_ExtGState_New  (HPDF_MMgr   mmgr, 
                     HPDF_Xref   xref);


HPDF_BOOL
HPDF_ExtGState_Validate  (HPDF_ExtGState  ext_gstate);


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_EXT_GSTATE_H */

