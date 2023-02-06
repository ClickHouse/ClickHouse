/*
 * << Haru Free PDF Library >> -- hpdf_objects.c
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

void
HPDF_Obj_Free  (HPDF_MMgr    mmgr,
                void         *obj)
{
    HPDF_Obj_Header *header;

    HPDF_PTRACE((" HPDF_Obj_Free\n"));

    if (!obj)
        return;

    header = (HPDF_Obj_Header *)obj;

    if (!(header->obj_id & HPDF_OTYPE_INDIRECT))
        HPDF_Obj_ForceFree (mmgr, obj);
}


void
HPDF_Obj_ForceFree  (HPDF_MMgr    mmgr,
                     void         *obj)
{
    HPDF_Obj_Header *header;

    HPDF_PTRACE((" HPDF_Obj_ForceFree\n"));

    if (!obj)
        return;

    header = (HPDF_Obj_Header *)obj;

    HPDF_PTRACE((" HPDF_Obj_ForceFree obj=0x%08X obj_id=0x%08X "
                    "obj_class=0x%08X\n",
                    (HPDF_UINT)obj, (HPDF_UINT)(header->obj_id),
                    (HPDF_UINT)(header->obj_class)));

    switch (header->obj_class & HPDF_OCLASS_ANY) {
        case HPDF_OCLASS_STRING:
            HPDF_String_Free (obj);
            break;
        case HPDF_OCLASS_BINARY:
            HPDF_Binary_Free (obj);
            break;
        case HPDF_OCLASS_ARRAY:
            HPDF_Array_Free (obj);
            break;
        case HPDF_OCLASS_DICT:
            HPDF_Dict_Free (obj);
            break;
        default:
            HPDF_FreeMem (mmgr, obj);
    }
}

HPDF_STATUS
HPDF_Obj_Write  (void          *obj,
                 HPDF_Stream   stream,
                 HPDF_Encrypt  e)
{
    HPDF_Obj_Header *header = (HPDF_Obj_Header *)obj;

    HPDF_PTRACE((" HPDF_Obj_Write\n"));

    if (header->obj_id & HPDF_OTYPE_HIDDEN) {
         HPDF_PTRACE(("#HPDF_Obj_Write obj=0x%08X skipped\n", (HPDF_UINT)obj));
         return HPDF_OK;
    }

    if (header->obj_class == HPDF_OCLASS_PROXY) {
        char buf[HPDF_SHORT_BUF_SIZ];
        char *pbuf = buf;
        char *eptr = buf + HPDF_SHORT_BUF_SIZ - 1;
        HPDF_Proxy p = obj;

        header = (HPDF_Obj_Header*)p->obj;

        pbuf = HPDF_IToA (pbuf, header->obj_id & 0x00FFFFFF, eptr);
        *pbuf++ = ' ';
        pbuf = HPDF_IToA (pbuf, header->gen_no, eptr);
        HPDF_StrCpy(pbuf, " R", eptr);

        return HPDF_Stream_WriteStr(stream, buf);
    }

    return HPDF_Obj_WriteValue(obj, stream, e);
}

HPDF_STATUS
HPDF_Obj_WriteValue  (void          *obj,
                      HPDF_Stream   stream,
                      HPDF_Encrypt  e)
{
    HPDF_Obj_Header *header;
    HPDF_STATUS ret;

    HPDF_PTRACE((" HPDF_Obj_WriteValue\n"));

    header = (HPDF_Obj_Header *)obj;

    HPDF_PTRACE((" HPDF_Obj_WriteValue obj=0x%08X obj_class=0x%04X\n",
            (HPDF_UINT)obj, (HPDF_UINT)header->obj_class));

    switch (header->obj_class & HPDF_OCLASS_ANY) {
        case HPDF_OCLASS_NAME:
            ret = HPDF_Name_Write (obj, stream);
            break;
        case HPDF_OCLASS_NUMBER:
            ret = HPDF_Number_Write (obj, stream);
            break;
        case HPDF_OCLASS_REAL:
            ret = HPDF_Real_Write (obj, stream);
            break;
        case HPDF_OCLASS_STRING:
            ret = HPDF_String_Write (obj, stream, e);
            break;
        case HPDF_OCLASS_BINARY:
            ret = HPDF_Binary_Write (obj, stream, e);
            break;
        case HPDF_OCLASS_ARRAY:
            ret = HPDF_Array_Write (obj, stream, e);
            break;
        case HPDF_OCLASS_DICT:
            ret = HPDF_Dict_Write (obj, stream, e);
            break;
        case HPDF_OCLASS_BOOLEAN:
            ret = HPDF_Boolean_Write (obj, stream);
            break;
        case HPDF_OCLASS_NULL:
            ret = HPDF_Stream_WriteStr (stream, "null");
            break;
        default:
            ret = HPDF_ERR_UNKNOWN_CLASS;
    }

    return ret;
}

HPDF_Proxy
HPDF_Proxy_New  (HPDF_MMgr  mmgr,
                 void       *obj)
{
    HPDF_Proxy p = HPDF_GetMem (mmgr, sizeof(HPDF_Proxy_Rec));

    HPDF_PTRACE((" HPDF_Proxy_New\n"));

    if (p) {
        HPDF_MemSet (&p->header, 0, sizeof(HPDF_Obj_Header));
        p->header.obj_class = HPDF_OCLASS_PROXY;
        p->obj = obj;
    }

    return p;
}

