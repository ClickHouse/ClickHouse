/*
 * << Haru Free PDF Library >> -- hpdf_fontdef_cnt.c
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
#include "hpdf_utils.h"
#include "hpdf.h"

/*----------------------------------------------------------------------------*/

static const HPDF_CID_Width MING_LIU_W_ARRAY[] = {
    {668, 500},
    {669, 500},
    {670, 500},
    {671, 500},
    {672, 500},
    {673, 500},
    {674, 500},
    {675, 500},
    {676, 500},
    {677, 500},
    {678, 500},
    {679, 500},
    {680, 500},
    {681, 500},
    {682, 500},
    {683, 500},
    {684, 500},
    {685, 500},
    {686, 500},
    {687, 500},
    {688, 500},
    {689, 500},
    {690, 500},
    {691, 500},
    {692, 500},
    {693, 500},
    {694, 500},
    {696, 500},
    {697, 500},
    {698, 500},
    {699, 500},
    {814, 500},
    {815, 500},
    {816, 500},
    {817, 500},
    {818, 500},
    {819, 500},
    {820, 500},
    {821, 500},
    {822, 500},
    {823, 500},
    {824, 500},
    {825, 500},
    {826, 500},
    {827, 500},
    {828, 500},
    {829, 500},
    {830, 500},
    {831, 500},
    {832, 500},
    {833, 500},
    {834, 500},
    {835, 500},
    {836, 500},
    {837, 500},
    {838, 500},
    {839, 500},
    {840, 500},
    {841, 500},
    {842, 500},
    {843, 500},
    {844, 500},
    {845, 500},
    {846, 500},
    {847, 500},
    {848, 500},
    {849, 500},
    {850, 500},
    {851, 500},
    {852, 500},
    {853, 500},
    {854, 500},
    {855, 500},
    {856, 500},
    {857, 500},
    {858, 500},
    {859, 500},
    {860, 500},
    {861, 500},
    {862, 500},
    {863, 500},
    {864, 500},
    {865, 500},
    {866, 500},
    {867, 500},
    {868, 500},
    {869, 500},
    {870, 500},
    {871, 500},
    {872, 500},
    {873, 500},
    {874, 500},
    {875, 500},
    {876, 500},
    {877, 500},
    {878, 500},
    {879, 500},
    {880, 500},
    {881, 500},
    {882, 500},
    {883, 500},
    {884, 500},
    {885, 500},
    {886, 500},
    {887, 500},
    {888, 500},
    {889, 500},
    {890, 500},
    {891, 500},
    {892, 500},
    {893, 500},
    {894, 500},
    {895, 500},
    {896, 500},
    {897, 500},
    {898, 500},
    {899, 500},
    {900, 500},
    {901, 500},
    {902, 500},
    {903, 500},
    {904, 500},
    {905, 500},
    {906, 500},
    {907, 500},
    {7716, 500},
    {0xFFFF, 0}
};


static HPDF_STATUS
MingLiU_Init  (HPDF_FontDef   fontdef)
{
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_FontDef_MingLiU_Init\n"));

    fontdef->ascent = 800;
    fontdef->descent = -199;
    fontdef->cap_height = 769;
    fontdef->font_bbox = HPDF_ToBox(0, -199, 1000, 800);
    fontdef->flags = HPDF_FONT_SYMBOLIC + HPDF_FONT_FIXED_WIDTH +
                HPDF_FONT_SERIF;
    fontdef->italic_angle = 0;
    fontdef->stemv = 78;
    if ((ret = HPDF_CIDFontDef_AddWidth (fontdef, MING_LIU_W_ARRAY)) !=
                HPDF_OK) {
        return ret;
    }

    fontdef->type = HPDF_FONTDEF_TYPE_CID;
    fontdef->valid = HPDF_TRUE;

    return HPDF_OK;
}


static HPDF_STATUS
MingLiU_Bold_Init  (HPDF_FontDef   fontdef)
{
    HPDF_STATUS ret = MingLiU_Init (fontdef);

    if (ret != HPDF_OK)
        return ret;

    return HPDF_CIDFontDef_ChangeStyle (fontdef, HPDF_TRUE, HPDF_FALSE);
}


static HPDF_STATUS
MingLiU_Italic_Init  (HPDF_FontDef   fontdef)
{
    HPDF_STATUS ret = MingLiU_Init (fontdef);

    if (ret != HPDF_OK)
        return ret;

    return HPDF_CIDFontDef_ChangeStyle (fontdef, HPDF_FALSE, HPDF_TRUE);
}

static HPDF_STATUS
MingLiU_BoldItalic_Init  (HPDF_FontDef   fontdef)
{
    HPDF_STATUS ret = MingLiU_Init (fontdef);

    if (ret != HPDF_OK)
        return ret;

    return HPDF_CIDFontDef_ChangeStyle (fontdef, HPDF_TRUE, HPDF_TRUE);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_UseCNTFonts   (HPDF_Doc   pdf)
{
    HPDF_FontDef fontdef;
    HPDF_STATUS ret;

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    /* MingLiU */
    fontdef = HPDF_CIDFontDef_New (pdf->mmgr,  "MingLiU",
                MingLiU_Init);

    if ((ret = HPDF_Doc_RegisterFontDef (pdf, fontdef)) != HPDF_OK)
        return ret;

    fontdef = HPDF_CIDFontDef_New (pdf->mmgr,  "MingLiU,Bold",
                MingLiU_Bold_Init);

    if ((ret = HPDF_Doc_RegisterFontDef (pdf, fontdef)) != HPDF_OK)
        return ret;

    fontdef = HPDF_CIDFontDef_New (pdf->mmgr,  "MingLiU,Italic",
                MingLiU_Italic_Init);

    if ((ret = HPDF_Doc_RegisterFontDef (pdf, fontdef)) != HPDF_OK)
        return ret;

    fontdef = HPDF_CIDFontDef_New (pdf->mmgr,  "MingLiU,BoldItalic",
                MingLiU_BoldItalic_Init);

    if ((ret = HPDF_Doc_RegisterFontDef (pdf, fontdef)) != HPDF_OK)
        return ret;

    return HPDF_OK;
}

