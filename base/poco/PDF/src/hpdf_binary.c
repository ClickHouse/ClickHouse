/*
 * << Haru Free PDF Library >> -- hpdf_binary.c
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


HPDF_Binary
HPDF_Binary_New  (HPDF_MMgr  mmgr,
                  HPDF_BYTE  *value,
                  HPDF_UINT  len)
{
    HPDF_Binary obj;

    obj  = HPDF_GetMem (mmgr, sizeof(HPDF_Binary_Rec));

    if (obj) {
        HPDF_MemSet(&obj->header, 0, sizeof(HPDF_Obj_Header));
        obj->header.obj_class = HPDF_OCLASS_BINARY;

        obj->mmgr = mmgr;
        obj->error = mmgr->error;
        obj->value = NULL;
        obj->len = 0;
        if (HPDF_Binary_SetValue (obj, value, len) != HPDF_OK) {
            HPDF_FreeMem (mmgr, obj);
            return NULL;
        }
    }

    return obj;
}

HPDF_STATUS
HPDF_Binary_Write  (HPDF_Binary   obj,
                    HPDF_Stream   stream,
                    HPDF_Encrypt  e)
{
    HPDF_STATUS ret;

    if (obj->len == 0)
        return HPDF_Stream_WriteStr (stream, "<>");

    if ((ret = HPDF_Stream_WriteChar (stream, '<')) != HPDF_OK)
        return ret;

    if (e)
        HPDF_Encrypt_Reset (e);

    if ((ret = HPDF_Stream_WriteBinary (stream, obj->value, obj->len, e)) !=
                    HPDF_OK)
        return ret;

    return HPDF_Stream_WriteChar (stream, '>');
}


HPDF_STATUS
HPDF_Binary_SetValue  (HPDF_Binary  obj,
                       HPDF_BYTE    *value,
                       HPDF_UINT    len)
{
    if (len > HPDF_LIMIT_MAX_STRING_LEN)
        return HPDF_SetError (obj->error, HPDF_BINARY_LENGTH_ERR, 0);

    if (obj->value) {
        HPDF_FreeMem (obj->mmgr, obj->value);
        obj->len = 0;
    }

    obj->value = HPDF_GetMem (obj->mmgr, len);
    if (!obj->value)
        return HPDF_Error_GetCode (obj->error);

    HPDF_MemCpy (obj->value, value, len);
    obj->len = len;

    return HPDF_OK;
}


void
HPDF_Binary_Free  (HPDF_Binary  obj)
{
    if (!obj)
        return;

    if (obj->value)
        HPDF_FreeMem (obj->mmgr, obj->value);

    HPDF_FreeMem (obj->mmgr, obj);
}

HPDF_UINT
HPDF_Binary_GetLen  (HPDF_Binary  obj)
{
    return obj->len;
}

HPDF_BYTE*
HPDF_Binary_GetValue  (HPDF_Binary  obj)
{
    return obj->value;
}

