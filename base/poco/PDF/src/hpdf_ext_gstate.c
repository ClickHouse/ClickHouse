/*
 * << Haru Free PDF Library >> -- hpdf_ext_gstate.c
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
#include "hpdf_ext_gstate.h"
#include "hpdf.h"

static const char * const HPDF_BM_NAMES[] = {
                                      "Normal",
                                      "Multiply",
                                      "Screen",
                                      "Overlay",
                                      "Darken",
                                      "Lighten",
                                      "ColorDodge",
                                      "ColorBurn",
                                      "HardLight",
                                      "SoftLight",
                                      "Difference",
                                      "Exclusion"
                                      };


HPDF_BOOL
HPDF_ExtGState_Validate  (HPDF_ExtGState  ext_gstate)
{
    if (!ext_gstate || (ext_gstate->header.obj_class != 
                (HPDF_OSUBCLASS_EXT_GSTATE | HPDF_OCLASS_DICT) &&
                ext_gstate->header.obj_class !=
                 (HPDF_OSUBCLASS_EXT_GSTATE_R | HPDF_OCLASS_DICT)))
        return HPDF_FALSE;

    return HPDF_TRUE;
}


HPDF_STATUS
ExtGState_Check  (HPDF_ExtGState  ext_gstate)
{
    if (!HPDF_ExtGState_Validate (ext_gstate))
        return HPDF_INVALID_OBJECT;
    
    if (ext_gstate->header.obj_class == 
            (HPDF_OSUBCLASS_EXT_GSTATE_R | HPDF_OCLASS_DICT))
        return HPDF_RaiseError (ext_gstate->error, HPDF_EXT_GSTATE_READ_ONLY,
                0);

    return HPDF_OK;
}


HPDF_Dict
HPDF_ExtGState_New  (HPDF_MMgr   mmgr, 
                     HPDF_Xref   xref)
{
    HPDF_Dict obj = HPDF_Dict_New (mmgr);

    HPDF_PTRACE ((" HPDF_ExtGState_New\n"));

    if (!obj)
        return NULL;

    if (HPDF_Xref_Add (xref, obj) != HPDF_OK)
        return NULL;

    if (HPDF_Dict_AddName (obj, "Type", "ExtGState") != HPDF_OK)
        return NULL;

    obj->header.obj_class |= HPDF_OSUBCLASS_EXT_GSTATE;

    return obj;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_ExtGState_SetAlphaStroke  (HPDF_ExtGState   ext_gstate,
                                HPDF_REAL        value)
{
    HPDF_STATUS ret = ExtGState_Check (ext_gstate);
    
    if (ret != HPDF_OK)
        return ret;
    
    if (value < 0 || value > 1.0f)
        return HPDF_RaiseError (ext_gstate->error, 
                HPDF_EXT_GSTATE_OUT_OF_RANGE, 0);

    return HPDF_Dict_AddReal (ext_gstate, "CA", value);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_ExtGState_SetAlphaFill  (HPDF_ExtGState   ext_gstate,
                              HPDF_REAL        value)
{
    HPDF_STATUS ret = ExtGState_Check (ext_gstate);
    
    if (ret != HPDF_OK)
        return ret;

    if (value < 0 || value > 1.0f)
        return HPDF_RaiseError (ext_gstate->error, 
                HPDF_EXT_GSTATE_OUT_OF_RANGE, 0);

    return HPDF_Dict_AddReal (ext_gstate, "ca", value);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_ExtGState_SetBlendMode  (HPDF_ExtGState   ext_gstate,
                              HPDF_BlendMode   bmode)
{
    HPDF_STATUS ret = ExtGState_Check (ext_gstate);
    
    if (ret != HPDF_OK)
        return ret;

    if ((int)bmode < 0 || (int)bmode > (int)HPDF_BM_EOF)
        return HPDF_RaiseError (ext_gstate->error, 
                HPDF_EXT_GSTATE_OUT_OF_RANGE, 0);

    return HPDF_Dict_AddName (ext_gstate, "BM", HPDF_BM_NAMES[(int)bmode]);
}

/*
HPDF_STATUS
HPDF_ExtGState_SetStrokeAdjustment  (HPDF_ExtGState   ext_gstate,
                                     HPDF_BOOL        value)
{
    HPDF_STATUS ret = ExtGState_Check (ext_gstate);
    
    if (ret != HPDF_OK)
        return ret;

    return HPDF_Dict_AddBoolean (ext_gstate, "SA", value);
}
*/

