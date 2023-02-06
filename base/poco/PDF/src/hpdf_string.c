/*
 * << Haru Free PDF Library >> -- hpdf_string.c
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

#include <string.h>
#include "hpdf_conf.h"
#include "hpdf_utils.h"
#include "hpdf_objects.h"

static const HPDF_BYTE UNICODE_HEADER[] = {
    0xFE, 0xFF
};


HPDF_String
HPDF_String_New  (HPDF_MMgr        mmgr,
                  const char  *value,
                  HPDF_Encoder     encoder)
{
    HPDF_String obj;

    HPDF_PTRACE((" HPDF_String_New\n"));

    obj = (HPDF_String)HPDF_GetMem (mmgr, sizeof(HPDF_String_Rec));
    if (obj) {
        HPDF_MemSet (&obj->header, 0, sizeof(HPDF_Obj_Header));
        obj->header.obj_class = HPDF_OCLASS_STRING;

        obj->mmgr = mmgr;
        obj->error = mmgr->error;
        obj->encoder = encoder;
        obj->value = NULL;
        obj->len = 0;

        if (HPDF_String_SetValue (obj, value) != HPDF_OK) {
            HPDF_FreeMem (obj->mmgr, obj);
            return NULL;
        }
    }

    return obj;
}


HPDF_STATUS
HPDF_String_SetValue  (HPDF_String      obj,
                       const char  *value)
{
    HPDF_UINT len;
    HPDF_STATUS ret = HPDF_OK;

    HPDF_PTRACE((" HPDF_String_SetValue\n"));

    if (obj->value) {
        HPDF_FreeMem (obj->mmgr, obj->value);
        obj->len = 0;
    }

    len = HPDF_StrLen(value, HPDF_LIMIT_MAX_STRING_LEN + 1);

    if (len > HPDF_LIMIT_MAX_STRING_LEN)
        return HPDF_SetError (obj->error, HPDF_STRING_OUT_OF_RANGE, 0);

    obj->value = HPDF_GetMem (obj->mmgr, len + 1);
    if (!obj->value)
        return HPDF_Error_GetCode (obj->error);

    HPDF_StrCpy ((char *)obj->value, value, (char *)obj->value + len);
    obj->len = len;

    return ret;
}

void
HPDF_String_Free  (HPDF_String  obj)
{
    if (!obj)
        return;

    HPDF_PTRACE((" HPDF_String_Free\n"));

    HPDF_FreeMem (obj->mmgr, obj->value);
    HPDF_FreeMem (obj->mmgr, obj);
}


HPDF_STATUS
HPDF_String_Write  (HPDF_String   obj,
                    HPDF_Stream   stream,
                    HPDF_Encrypt  e)
{
    HPDF_STATUS ret;

    /*
     *  When encoder is not NULL, text is changed to unicode using encoder,
     *  and it outputs by HPDF_write_binary method.
     */

    HPDF_PTRACE((" HPDF_String_Write\n"));

    if (e)
        HPDF_Encrypt_Reset (e);

    if (obj->encoder == NULL) {
        if (e) {
            if ((ret = HPDF_Stream_WriteChar (stream, '<')) != HPDF_OK)
                return ret;

            if ((ret = HPDF_Stream_WriteBinary (stream, obj->value,
                    HPDF_StrLen ((char *)obj->value, -1), e)) != HPDF_OK)
                return ret;

            return HPDF_Stream_WriteChar (stream, '>');
        } else {
            return HPDF_Stream_WriteEscapeText (stream, (char *)obj->value);
        }
    } else {
        HPDF_BYTE* src = obj->value;
        HPDF_BYTE buf[HPDF_TEXT_DEFAULT_LEN * 2];
        HPDF_UINT tmp_len = 0;
        HPDF_BYTE* pbuf = buf;
        HPDF_INT32 len = obj->len;
        HPDF_ParseText_Rec  parse_state;
        HPDF_UINT i;

        if ((ret = HPDF_Stream_WriteChar (stream, '<')) != HPDF_OK)
           return ret;

        if ((ret = HPDF_Stream_WriteBinary (stream, UNICODE_HEADER, 2, e))
                        != HPDF_OK)
            return ret;

        HPDF_Encoder_SetParseText (obj->encoder, &parse_state, src, len);

        for (i = 0; (HPDF_INT32)i < len; i++) {
            HPDF_BYTE b = src[i];
            HPDF_UNICODE tmp_unicode;
            HPDF_ByteType btype = HPDF_Encoder_ByteType (obj->encoder,
                    &parse_state);

            if (tmp_len >= HPDF_TEXT_DEFAULT_LEN - 1) {
                if ((ret = HPDF_Stream_WriteBinary (stream, buf,
                            tmp_len * 2, e)) != HPDF_OK)
                    return ret;

                tmp_len = 0;
                pbuf = buf;
            }

            if (btype != HPDF_BYTE_TYPE_TRIAL) {
                if (btype == HPDF_BYTE_TYPE_LEAD) {
                    HPDF_BYTE b2 = src[i + 1];
                    HPDF_UINT16 char_code = (HPDF_UINT16)((HPDF_UINT) b * 256 + b2);

                    tmp_unicode = HPDF_Encoder_ToUnicode (obj->encoder,
                                char_code);
                } else {
                    tmp_unicode = HPDF_Encoder_ToUnicode (obj->encoder, b);
                }

                HPDF_UInt16Swap (&tmp_unicode);
                HPDF_MemCpy (pbuf, (HPDF_BYTE*)&tmp_unicode, 2);
                pbuf += 2;
                tmp_len++;
            }
        }

        if (tmp_len > 0) {
            if ((ret = HPDF_Stream_WriteBinary (stream, buf, tmp_len * 2, e))
                            != HPDF_OK)
                return ret;
        }

        if ((ret = HPDF_Stream_WriteChar (stream, '>')) != HPDF_OK)
            return ret;
    }

    return HPDF_OK;
}


HPDF_INT32
HPDF_String_Cmp  (HPDF_String s1,
                  HPDF_String s2)
{
    if (s1->len < s2->len) return -1;
    if (s1->len > s2->len) return +1;
    return memcmp(s1->value, s2->value, s1->len);
}
