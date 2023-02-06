/*
 * << Haru Free PDF Library >> -- hpdf_name.c
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
#include "hpdf_objects.h"


HPDF_Name
HPDF_Name_New  (HPDF_MMgr        mmgr,
                const char  *value)
{
    HPDF_Name obj;

    obj  = HPDF_GetMem (mmgr, sizeof(HPDF_Name_Rec));

    if (obj) {
        HPDF_MemSet (&obj->header, 0, sizeof(HPDF_Obj_Header));
        obj->header.obj_class = HPDF_OCLASS_NAME;
        obj->error = mmgr->error;
        if (HPDF_Name_SetValue (obj, value) == HPDF_NAME_INVALID_VALUE) {
            HPDF_FreeMem (mmgr, obj);
            return NULL;
        }
    }

    return obj;
}


HPDF_STATUS
HPDF_Name_Write  (HPDF_Name    obj,
                  HPDF_Stream  stream)
{
    return HPDF_Stream_WriteEscapeName (stream, obj->value);
}


HPDF_STATUS
HPDF_Name_SetValue  (HPDF_Name        obj,
                     const char  *value)
{
    if (!value || value[0] == 0)
        return HPDF_SetError (obj->error, HPDF_NAME_INVALID_VALUE, 0);

    if (HPDF_StrLen (value, HPDF_LIMIT_MAX_NAME_LEN + 1) >
            HPDF_LIMIT_MAX_NAME_LEN)
        return HPDF_SetError (obj->error, HPDF_NAME_OUT_OF_RANGE, 0);

    HPDF_StrCpy (obj->value, value, obj->value + HPDF_LIMIT_MAX_NAME_LEN);

    return HPDF_OK;
}

const char*
HPDF_Name_GetValue (HPDF_Name  obj)
{
    return (const char *)obj->value;
}

