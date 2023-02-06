/*
 * << Haru Free PDF Library >> -- hpdf_xref.c
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

static HPDF_STATUS
WriteTrailer  (HPDF_Xref     xref,
               HPDF_Stream   stream);


HPDF_Xref
HPDF_Xref_New  (HPDF_MMgr     mmgr,
                HPDF_UINT32   offset)
{
    HPDF_Xref xref;
    HPDF_XrefEntry new_entry;

    HPDF_PTRACE((" HPDF_Xref_New\n"));

    xref = (HPDF_Xref)HPDF_GetMem (mmgr, sizeof(HPDF_Xref_Rec));
    if (!xref)
        return NULL;

    HPDF_MemSet (xref, 0, sizeof(HPDF_Xref_Rec));
    xref->mmgr = mmgr;
    xref->error = mmgr->error;
    xref->start_offset = offset;

    xref->entries = HPDF_List_New (mmgr, HPDF_DEFALUT_XREF_ENTRY_NUM);
    if (!xref->entries)
        goto Fail;

    xref->addr = 0;

    if (xref->start_offset == 0) {
        new_entry = (HPDF_XrefEntry)HPDF_GetMem (mmgr,
                sizeof(HPDF_XrefEntry_Rec));
        if (!new_entry)
            goto Fail;

        if (HPDF_List_Add (xref->entries, new_entry) != HPDF_OK) {
            HPDF_FreeMem (mmgr, new_entry);
            goto Fail;
        }

        /* add first entry which is free entry and whose generation
         * number is 0
         */
        new_entry->entry_typ = HPDF_FREE_ENTRY;
        new_entry->byte_offset = 0;
        new_entry->gen_no = HPDF_MAX_GENERATION_NUM;
        new_entry->obj = NULL;
    }

    xref->trailer = HPDF_Dict_New (mmgr);
    if (!xref->trailer)
        goto Fail;

    return xref;

Fail:
    HPDF_PTRACE((" HPDF_Xref_New failed\n"));
    HPDF_Xref_Free (xref);
    return NULL;
}


void
HPDF_Xref_Free  (HPDF_Xref  xref)
{
    HPDF_UINT i;
    HPDF_XrefEntry entry;
    HPDF_Xref tmp_xref;

    HPDF_PTRACE((" HPDF_Xref_Free\n"));

    /* delete xref entries. where prev element is not NULL,
     * delete all xref entries recursively.
     */
    while (xref) {
        /* delete all objects belong to the xref. */

        if (xref->entries) {
            for (i = 0; i < xref->entries->count; i++) {
                entry = HPDF_Xref_GetEntry (xref, i);
                if (entry->obj)
                    HPDF_Obj_ForceFree (xref->mmgr, entry->obj);
                HPDF_FreeMem (xref->mmgr, entry);
            }

            HPDF_List_Free(xref->entries);
        }

        if (xref->trailer)
            HPDF_Dict_Free (xref->trailer);

        tmp_xref = xref->prev;
        HPDF_FreeMem (xref->mmgr, xref);
        xref = tmp_xref;
    }
}


HPDF_STATUS
HPDF_Xref_Add  (HPDF_Xref  xref,
                void       *obj)
{
    HPDF_XrefEntry entry;
    HPDF_Obj_Header *header;

    HPDF_PTRACE((" HPDF_Xref_Add\n"));

    if (!obj) {
        if (HPDF_Error_GetCode (xref->error) == HPDF_OK)
            return HPDF_SetError (xref->error, HPDF_INVALID_OBJECT, 0);
        else
            return HPDF_INVALID_OBJECT;
    }

    header = (HPDF_Obj_Header *)obj;

    if (header->obj_id & HPDF_OTYPE_DIRECT ||
            header->obj_id & HPDF_OTYPE_INDIRECT)
        return HPDF_SetError(xref->error, HPDF_INVALID_OBJECT, 0);

    if (xref->entries->count >= HPDF_LIMIT_MAX_XREF_ELEMENT) {
        HPDF_SetError(xref->error, HPDF_XREF_COUNT_ERR, 0);
        goto Fail;
    }

    /* in the following, we have to dispose the object when an error is
     * occured.
     */

    entry = (HPDF_XrefEntry)HPDF_GetMem (xref->mmgr,
            sizeof(HPDF_XrefEntry_Rec));
    if (entry == NULL)
        goto Fail;

    if (HPDF_List_Add(xref->entries, entry) != HPDF_OK) {
        HPDF_FreeMem (xref->mmgr, entry);
        goto Fail;
    }

    entry->entry_typ = HPDF_IN_USE_ENTRY;
    entry->byte_offset = 0;
    entry->gen_no = 0;
    entry->obj = obj;
    header->obj_id = xref->start_offset + xref->entries->count - 1 +
                    HPDF_OTYPE_INDIRECT;

    header->gen_no = entry->gen_no;

    return HPDF_OK;

Fail:
    HPDF_Obj_ForceFree(xref->mmgr, obj);
    return HPDF_Error_GetCode (xref->error);
}

HPDF_XrefEntry
HPDF_Xref_GetEntry  (HPDF_Xref  xref,
                     HPDF_UINT  index)
{
    HPDF_PTRACE((" HPDF_Xref_GetEntry\n"));

    return (HPDF_XrefEntry)HPDF_List_ItemAt(xref->entries, index);
}


HPDF_XrefEntry
HPDF_Xref_GetEntryByObjectId  (HPDF_Xref  xref,
                               HPDF_UINT  obj_id)
{
    HPDF_Xref tmp_xref = xref;

    HPDF_PTRACE((" HPDF_Xref_GetEntryByObjectId\n"));

    while (tmp_xref) {
        HPDF_UINT i;

        if (tmp_xref->entries->count + tmp_xref->start_offset > obj_id) {
            HPDF_SetError (xref->error, HPDF_INVALID_OBJ_ID, 0);
            return NULL;
        }

        if (tmp_xref->start_offset < obj_id) {
            for (i = 0; i < tmp_xref->entries->count; i++) {
                if (tmp_xref->start_offset + i == obj_id) {
                    HPDF_XrefEntry entry = HPDF_Xref_GetEntry(tmp_xref, i);

                    return entry;
                }
            }
        }

        tmp_xref = tmp_xref->prev;
    }

    return NULL;
}


HPDF_STATUS
HPDF_Xref_WriteToStream  (HPDF_Xref    xref,
                          HPDF_Stream  stream,
                          HPDF_Encrypt e)
{
    HPDF_STATUS ret;
    HPDF_UINT i;
    char buf[HPDF_SHORT_BUF_SIZ];
    char* pbuf;
    char* eptr = buf + HPDF_SHORT_BUF_SIZ - 1;
    HPDF_UINT str_idx;
    HPDF_Xref tmp_xref = xref;

    /* write each objects of xref to the specified stream */

    HPDF_PTRACE((" HPDF_Xref_WriteToStream\n"));

    while (tmp_xref) {
        if (tmp_xref->start_offset == 0)
            str_idx = 1;
        else
            str_idx = 0;

        for (i = str_idx; i < tmp_xref->entries->count; i++) {
            HPDF_XrefEntry  entry =
                        (HPDF_XrefEntry)HPDF_List_ItemAt (tmp_xref->entries, i);
            HPDF_UINT obj_id = tmp_xref->start_offset + i;
            HPDF_UINT16 gen_no = entry->gen_no;

            entry->byte_offset = stream->size;

            pbuf = buf;
            pbuf = HPDF_IToA (pbuf, obj_id, eptr);
            *pbuf++ = ' ';
            pbuf = HPDF_IToA (pbuf, gen_no, eptr);
            HPDF_StrCpy(pbuf, " obj\012", eptr);

            if ((ret = HPDF_Stream_WriteStr (stream, buf)) != HPDF_OK)
               return ret;

            if (e)
                HPDF_Encrypt_InitKey (e, obj_id, gen_no);

            if ((ret = HPDF_Obj_WriteValue (entry->obj, stream, e)) != HPDF_OK)
                return ret;

            if ((ret = HPDF_Stream_WriteStr (stream, "\012endobj\012"))
                    != HPDF_OK)
                return ret;
       }

       tmp_xref = tmp_xref->prev;
    }

    /* start to write cross-reference table */

    tmp_xref = xref;

    while (tmp_xref) {
        tmp_xref->addr = stream->size;

        pbuf = buf;
        pbuf = (char *)HPDF_StrCpy (pbuf, "xref\012", eptr);
        pbuf = HPDF_IToA (pbuf, tmp_xref->start_offset, eptr);
        *pbuf++ = ' ';
        pbuf = HPDF_IToA (pbuf, tmp_xref->entries->count, eptr);
        HPDF_StrCpy (pbuf, "\012", eptr);
        ret = HPDF_Stream_WriteStr (stream, buf);
        if (ret != HPDF_OK)
            return ret;

        for (i = 0; i < tmp_xref->entries->count; i++) {
            HPDF_XrefEntry entry = HPDF_Xref_GetEntry(tmp_xref, i);

            pbuf = buf;
            pbuf = HPDF_IToA2 (pbuf, entry->byte_offset, HPDF_BYTE_OFFSET_LEN +
                    1);
            *pbuf++ = ' ';
            pbuf = HPDF_IToA2 (pbuf, entry->gen_no, HPDF_GEN_NO_LEN + 1);
            *pbuf++ = ' ';
            *pbuf++ = entry->entry_typ;
            HPDF_StrCpy (pbuf, "\015\012", eptr); /* Acrobat 8.15 requires both \r and \n here */
            ret = HPDF_Stream_WriteStr (stream, buf);
            if (ret != HPDF_OK)
                return ret;
        }

        tmp_xref = tmp_xref->prev;
    }

    /* write trailer dictionary */
    ret = WriteTrailer (xref, stream);

    return ret;
}

static HPDF_STATUS
WriteTrailer  (HPDF_Xref     xref,
               HPDF_Stream   stream)
{
    HPDF_UINT max_obj_id = xref->entries->count + xref->start_offset;
    HPDF_STATUS ret;

    HPDF_PTRACE ((" WriteTrailer\n"));

    if ((ret = HPDF_Dict_AddNumber (xref->trailer, "Size", max_obj_id))
            != HPDF_OK)
        return ret;

    if (xref->prev)
        if ((ret = HPDF_Dict_AddNumber (xref->trailer, "Prev",
                xref->prev->addr)) != HPDF_OK)
            return ret;

    if ((ret = HPDF_Stream_WriteStr (stream, "trailer\012")) != HPDF_OK)
        return ret;

    if ((ret = HPDF_Dict_Write (xref->trailer, stream, NULL)) != HPDF_OK)
        return ret;

    if ((ret = HPDF_Stream_WriteStr (stream, "\012startxref\012")) != HPDF_OK)
        return ret;

    if ((ret = HPDF_Stream_WriteUInt (stream, xref->addr)) != HPDF_OK)
        return ret;

    if ((ret = HPDF_Stream_WriteStr (stream, "\012%%EOF\012")) != HPDF_OK)
        return ret;

    return HPDF_OK;
}

