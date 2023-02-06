/*
 * << Haru Free PDF Library >> -- hpdf_gstate.h
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

#ifndef _HPDF_GSTATE_H
#define _HPDF_GSTATE_H

#include "hpdf_font.h"

#ifdef __cplusplus
extern "C" {
#endif


/*----------------------------------------------------------------------------*/
/*------ graphic state stack -------------------------------------------------*/

typedef struct _HPDF_GState_Rec  *HPDF_GState;

typedef struct _HPDF_GState_Rec {
    HPDF_TransMatrix        trans_matrix;
    HPDF_REAL               line_width;
    HPDF_LineCap            line_cap;
    HPDF_LineJoin           line_join;
    HPDF_REAL               miter_limit;
    HPDF_DashMode           dash_mode;
    HPDF_REAL               flatness;

    HPDF_REAL               char_space;
    HPDF_REAL               word_space;
    HPDF_REAL               h_scalling;
    HPDF_REAL               text_leading;
    HPDF_TextRenderingMode  rendering_mode;
    HPDF_REAL               text_rise;

    HPDF_ColorSpace         cs_fill;
    HPDF_ColorSpace         cs_stroke;
    HPDF_RGBColor           rgb_fill;
    HPDF_RGBColor           rgb_stroke;
    HPDF_CMYKColor          cmyk_fill;
    HPDF_CMYKColor          cmyk_stroke;
    HPDF_REAL               gray_fill;
    HPDF_REAL               gray_stroke;

    HPDF_Font               font;
    HPDF_REAL               font_size;
    HPDF_WritingMode        writing_mode;

    HPDF_GState             prev;
    HPDF_UINT               depth;
} HPDF_GState_Rec;

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

HPDF_GState
HPDF_GState_New  (HPDF_MMgr    mmgr,
                  HPDF_GState  current);


HPDF_GState
HPDF_GState_Free  (HPDF_MMgr    mmgr,
                   HPDF_GState  gstate);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_GSTATE_H */

