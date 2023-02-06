/*
 * << Haru Free PDF Library >> -- hpdf_dict.c
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

HPDF_DictElement
GetElement  (HPDF_Dict      dict,
             const char    *key);

/*--------------------------------------------------------------------------*/

HPDF_Dict
HPDF_Dict_New  (HPDF_MMgr  mmgr)
{
    HPDF_Dict obj;

    obj = (HPDF_Dict)HPDF_GetMem (mmgr, sizeof(HPDF_Dict_Rec));
    if (obj) {
        HPDF_MemSet (obj, 0, sizeof(HPDF_Dict_Rec));
        obj->header.obj_class = HPDF_OCLASS_DICT;
        obj->mmgr = mmgr;
        obj->error = mmgr->error;
        obj->list = HPDF_List_New (mmgr, HPDF_DEF_ITEMS_PER_BLOCK);
        obj->filter = HPDF_STREAM_FILTER_NONE;
        if (!obj->list) {
            HPDF_FreeMem (mmgr, obj);
            obj = NULL;
        }
    }

    return obj;
}


HPDF_Dict
HPDF_DictStream_New  (HPDF_MMgr  mmgr,
                      HPDF_Xref  xref)
{
    HPDF_Dict  obj;
    HPDF_Number length;
    HPDF_STATUS ret = 0;

    obj = HPDF_Dict_New (mmgr);
    if (!obj)
        return NULL;

    /* only stream object is added to xref automatically */
    ret += HPDF_Xref_Add (xref, obj);
    if (ret != HPDF_OK)
        return NULL;

    length = HPDF_Number_New (mmgr, 0);
    if (!length)
        return NULL;

    ret = HPDF_Xref_Add (xref, length);
    if (ret != HPDF_OK)
        return NULL;

    ret = HPDF_Dict_Add (obj, "Length", length);
    if (ret != HPDF_OK)
        return NULL;

    obj->stream = HPDF_MemStream_New (mmgr, HPDF_STREAM_BUF_SIZ);
    if (!obj->stream)
        return NULL;

    return obj;
}


void
HPDF_Dict_Free  (HPDF_Dict  dict)
{
    HPDF_UINT i;

    if (!dict)
        return;

    if (dict->free_fn)
        dict->free_fn (dict);

    for (i = 0; i < dict->list->count; i++) {
        HPDF_DictElement element =
                (HPDF_DictElement)HPDF_List_ItemAt (dict->list, i);

        if (element) {
            HPDF_Obj_Free (dict->mmgr, element->value);
            HPDF_FreeMem (dict->mmgr, element);
        }
    }

    if (dict->stream)
        HPDF_Stream_Free (dict->stream);

    HPDF_List_Free (dict->list);

    dict->header.obj_class = 0;

    HPDF_FreeMem (dict->mmgr, dict);
}

HPDF_STATUS
HPDF_Dict_Add_FilterParams(HPDF_Dict    dict, HPDF_Dict filterParam)
{
    HPDF_Array paramArray;
    /* prepare params object */
    paramArray = HPDF_Dict_GetItem (dict, "DecodeParms",
                                              HPDF_OCLASS_ARRAY);
    if(paramArray==NULL) {
        paramArray = HPDF_Array_New (dict->mmgr);
       if (!paramArray)
            return HPDF_Error_GetCode (dict->error);

        /* add parameters */
        HPDF_Dict_Add(dict, "DecodeParms", paramArray);
    }
    HPDF_Array_Add(paramArray, filterParam);
    return HPDF_OK;
}


HPDF_STATUS
HPDF_Dict_Write  (HPDF_Dict     dict,
                  HPDF_Stream   stream,
                  HPDF_Encrypt  e)
{
    HPDF_UINT i;
    HPDF_STATUS ret;

    ret = HPDF_Stream_WriteStr (stream, "<<\012");
    if (ret != HPDF_OK)
        return ret;

    if (dict->before_write_fn) {
        if ((ret = dict->before_write_fn (dict)) != HPDF_OK)
            return ret;
    }

    /* encrypt-dict must not be encrypted. */
    if (dict->header.obj_class == (HPDF_OCLASS_DICT | HPDF_OSUBCLASS_ENCRYPT))
        e = NULL;

    if (dict->stream) {
        /* set filter element */
        if (dict->filter == HPDF_STREAM_FILTER_NONE)
            HPDF_Dict_RemoveElement (dict, "Filter");
        else {
            HPDF_Array array = HPDF_Dict_GetItem (dict, "Filter",
                        HPDF_OCLASS_ARRAY);

            if (!array) {
                array = HPDF_Array_New (dict->mmgr);
                if (!array)
                    return HPDF_Error_GetCode (dict->error);

                ret = HPDF_Dict_Add (dict, "Filter", array);
                if (ret != HPDF_OK)
                    return ret;
            }

            HPDF_Array_Clear (array);

#ifndef LIBHPDF_HAVE_NOZLIB
            if (dict->filter & HPDF_STREAM_FILTER_FLATE_DECODE)
                HPDF_Array_AddName (array, "FlateDecode");
#endif /* LIBHPDF_HAVE_NOZLIB */

            if (dict->filter & HPDF_STREAM_FILTER_DCT_DECODE)
                HPDF_Array_AddName (array, "DCTDecode");

            if(dict->filter & HPDF_STREAM_FILTER_CCITT_DECODE)
                HPDF_Array_AddName (array, "CCITTFaxDecode");

            if(dict->filterParams!=NULL)
            {
                HPDF_Dict_Add_FilterParams(dict, dict->filterParams);
            }
        }
    }

    for (i = 0; i < dict->list->count; i++) {
        HPDF_DictElement element =
                (HPDF_DictElement)HPDF_List_ItemAt (dict->list, i);
        HPDF_Obj_Header *header = (HPDF_Obj_Header *)(element->value);

        if (!element->value)
            return HPDF_SetError (dict->error, HPDF_INVALID_OBJECT, 0);

        if  (header->obj_id & HPDF_OTYPE_HIDDEN) {
            HPDF_PTRACE((" HPDF_Dict_Write obj=%p skipped obj_id=0x%08X\n",
                    element->value, (HPDF_UINT)header->obj_id));
        } else {
            ret = HPDF_Stream_WriteEscapeName (stream, element->key);
            if (ret != HPDF_OK)
                return ret;

            ret = HPDF_Stream_WriteChar (stream, ' ');
            if (ret != HPDF_OK)
                return ret;

            ret = HPDF_Obj_Write (element->value, stream, e);
            if (ret != HPDF_OK)
                return ret;

            ret = HPDF_Stream_WriteStr (stream, "\012");
            if (ret != HPDF_OK)
                return ret;
        }
    }

    if (dict->write_fn) {
        if ((ret = dict->write_fn (dict, stream)) != HPDF_OK)
            return ret;
    }

    if ((ret = HPDF_Stream_WriteStr (stream, ">>")) != HPDF_OK)
        return ret;

    if (dict->stream) {
        HPDF_UINT32 strptr;
        HPDF_Number length;

        /* get "length" element */
        length = (HPDF_Number)HPDF_Dict_GetItem (dict, "Length",
                HPDF_OCLASS_NUMBER);
        if (!length)
            return HPDF_SetError (dict->error,
                    HPDF_DICT_STREAM_LENGTH_NOT_FOUND, 0);

        /* "length" element must be indirect-object */
        if (!(length->header.obj_id & HPDF_OTYPE_INDIRECT)) {
            return HPDF_SetError (dict->error, HPDF_DICT_ITEM_UNEXPECTED_TYPE,
                    0);
        }

        if ((ret = HPDF_Stream_WriteStr (stream, "\012stream\015\012")) /* Acrobat 8.15 requires both \r and \n here */
                != HPDF_OK)
            return ret;

        strptr = stream->size;

        if (e)
            HPDF_Encrypt_Reset (e);

        if ((ret = HPDF_Stream_WriteToStream (dict->stream, stream,
                        dict->filter, e)) != HPDF_OK)
            return ret;

        HPDF_Number_SetValue (length, stream->size - strptr);

        ret = HPDF_Stream_WriteStr (stream, "\012endstream");
    }

    /* 2006.08.13 add. */
    if (dict->after_write_fn) {
        if ((ret = dict->after_write_fn (dict)) != HPDF_OK)
            return ret;
    }

    return ret;
}

HPDF_STATUS
HPDF_Dict_Add  (HPDF_Dict        dict,
                const char  *key,
                void             *obj)
{
    HPDF_Obj_Header *header;
    HPDF_STATUS ret = HPDF_OK;
    HPDF_DictElement element;

    if (!obj) {
        if (HPDF_Error_GetCode (dict->error) == HPDF_OK)
            return HPDF_SetError (dict->error, HPDF_INVALID_OBJECT, 0);
        else
            return HPDF_INVALID_OBJECT;
    }

    header = (HPDF_Obj_Header *)obj;

    if (header->obj_id & HPDF_OTYPE_DIRECT)
        return HPDF_SetError (dict->error, HPDF_INVALID_OBJECT, 0);

    if (!key) {
        HPDF_Obj_Free (dict->mmgr, obj);
        return HPDF_SetError (dict->error, HPDF_INVALID_OBJECT, 0);
    }

    if (dict->list->count >= HPDF_LIMIT_MAX_DICT_ELEMENT) {
        HPDF_PTRACE((" HPDF_Dict_Add exceed limitatin of dict count(%d)\n",
                    HPDF_LIMIT_MAX_DICT_ELEMENT));

        HPDF_Obj_Free (dict->mmgr, obj);
        return HPDF_SetError (dict->error, HPDF_DICT_COUNT_ERR, 0);
    }

    /* check whether there is an object which has same name */
    element = GetElement (dict, key);

    if (element) {
        HPDF_Obj_Free (dict->mmgr, element->value);
        element->value = NULL;
    } else {
        element = (HPDF_DictElement)HPDF_GetMem (dict->mmgr,
                sizeof(HPDF_DictElement_Rec));

        if (!element) {
            /* cannot create element object */
            if (!(header->obj_id & HPDF_OTYPE_INDIRECT))
                HPDF_Obj_Free (dict->mmgr, obj);

            return HPDF_Error_GetCode (dict->error);
        }

        HPDF_StrCpy (element->key, key, element->key +
                HPDF_LIMIT_MAX_NAME_LEN + 1);
        element->value = NULL;

        ret = HPDF_List_Add (dict->list, element);
        if (ret != HPDF_OK) {
            if (!(header->obj_id & HPDF_OTYPE_INDIRECT))
                HPDF_Obj_Free (dict->mmgr, obj);

            HPDF_FreeMem (dict->mmgr, element);

            return HPDF_Error_GetCode (dict->error);
        }
    }

    if (header->obj_id & HPDF_OTYPE_INDIRECT) {
        HPDF_Proxy proxy = HPDF_Proxy_New (dict->mmgr, obj);

        if (!proxy)
            return HPDF_Error_GetCode (dict->error);

        element->value = proxy;
        proxy->header.obj_id |= HPDF_OTYPE_DIRECT;
    } else {
        element->value = obj;
        header->obj_id |= HPDF_OTYPE_DIRECT;
    }

    return ret;
}


HPDF_STATUS
HPDF_Dict_AddName (HPDF_Dict        dict,
                   const char  *key,
                   const char  *value)
{
    HPDF_Name name = HPDF_Name_New (dict->mmgr, value);
    if (!name)
        return HPDF_Error_GetCode (dict->error);

    return HPDF_Dict_Add (dict, key, name);
}


HPDF_STATUS
HPDF_Dict_AddNumber  (HPDF_Dict        dict,
                      const char  *key,
                      HPDF_INT32       value)
{
    HPDF_Number number = HPDF_Number_New (dict->mmgr, value);

    if (!number)
        return HPDF_Error_GetCode (dict->error);

    return HPDF_Dict_Add (dict, key, number);
}


HPDF_STATUS
HPDF_Dict_AddReal  (HPDF_Dict        dict,
                    const char  *key,
                    HPDF_REAL        value)
{
    HPDF_Real real = HPDF_Real_New (dict->mmgr, value);

    if (!real)
        return HPDF_Error_GetCode (dict->error);

    return HPDF_Dict_Add (dict, key, real);
}


HPDF_STATUS
HPDF_Dict_AddBoolean  (HPDF_Dict      dict,
                      const char    *key,
                      HPDF_BOOL      value)
{
    HPDF_Boolean obj = HPDF_Boolean_New (dict->mmgr, value);

    if (!obj)
        return HPDF_Error_GetCode (dict->error);

    return HPDF_Dict_Add (dict, key, obj);
}


void*
HPDF_Dict_GetItem  (HPDF_Dict        dict,
                    const char  *key,
                    HPDF_UINT16      obj_class)
{
    HPDF_DictElement element = GetElement (dict, key);
    void *obj;

    if (element && HPDF_StrCmp(key, element->key) == 0) {
        HPDF_Obj_Header *header = (HPDF_Obj_Header *)element->value;

        if (header->obj_class == HPDF_OCLASS_PROXY) {
            HPDF_Proxy p = element->value;
            header = (HPDF_Obj_Header *)p->obj;
            obj = p->obj;
        } else
            obj = element->value;

        if ((header->obj_class & HPDF_OCLASS_ANY) != obj_class) {
            HPDF_PTRACE((" HPDF_Dict_GetItem dict=%p key=%s obj_class=0x%08X\n",
                    dict, key, (HPDF_UINT)header->obj_class));
            HPDF_SetError (dict->error, HPDF_DICT_ITEM_UNEXPECTED_TYPE, 0);

            return NULL;
        }

        return obj;
    }

    return NULL;
}


HPDF_DictElement
GetElement  (HPDF_Dict        dict,
             const char  *key)
{
    HPDF_UINT i;

    for (i = 0; i < dict->list->count; i++) {
        HPDF_DictElement element =
                (HPDF_DictElement)HPDF_List_ItemAt (dict->list, i);

        if (HPDF_StrCmp (key, element->key) == 0)
            return element;
    }

    return NULL;
}


HPDF_STATUS
HPDF_Dict_RemoveElement  (HPDF_Dict        dict,
                          const char  *key)
{
    HPDF_UINT i;

    for (i = 0; i < dict->list->count; i++) {
        HPDF_DictElement element =
                (HPDF_DictElement)HPDF_List_ItemAt (dict->list, i);

        if (HPDF_StrCmp (key, element->key) == 0) {
            HPDF_List_Remove (dict->list, element);

            HPDF_Obj_Free (dict->mmgr, element->value);
            HPDF_FreeMem (dict->mmgr, element);

            return HPDF_OK;
        }
    }

    return HPDF_DICT_ITEM_NOT_FOUND;
}

const char*
HPDF_Dict_GetKeyByObj (HPDF_Dict  dict,
                       void       *obj)
{
    HPDF_UINT i;

    for (i = 0; i < dict->list->count; i++) {
        HPDF_Obj_Header *header;
        HPDF_DictElement element =
                (HPDF_DictElement)HPDF_List_ItemAt (dict->list, i);

        header = (HPDF_Obj_Header *)(element->value);
        if (header->obj_class == HPDF_OCLASS_PROXY) {
            HPDF_Proxy p = element->value;

            if (p->obj == obj)
                return element->key;
        } else {
            if (element->value == obj)
                return element->key;
        }
    }

    return NULL;
}

