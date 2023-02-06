/*
 * << Haru Free PDF Library >> -- hpdf_doc.c
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
#include "hpdf_encryptdict.h"
#include "hpdf_namedict.h"
#include "hpdf_destination.h"
#include "hpdf_info.h"
#include "hpdf_page_label.h"
#include "hpdf.h"


static const char * const HPDF_VERSION_STR[6] = {
                "%PDF-1.2\012%\267\276\255\252\012",
                "%PDF-1.3\012%\267\276\255\252\012",
                "%PDF-1.4\012%\267\276\255\252\012",
                "%PDF-1.5\012%\267\276\255\252\012",
                "%PDF-1.6\012%\267\276\255\252\012",
                "%PDF-1.7\012%\267\276\255\252\012"
};


static HPDF_STATUS
WriteHeader  (HPDF_Doc      pdf,
              HPDF_Stream   stream);


static HPDF_STATUS
PrepareTrailer  (HPDF_Doc   pdf);


static void
FreeEncoderList (HPDF_Doc  pdf);


static void
FreeFontDefList (HPDF_Doc  pdf);


static void
CleanupFontDefList (HPDF_Doc  pdf);


static HPDF_Dict
GetInfo  (HPDF_Doc  pdf);

static HPDF_STATUS
InternalSaveToStream  (HPDF_Doc      pdf,
                       HPDF_Stream   stream);

static const char*
LoadType1FontFromStream (HPDF_Doc     pdf,
                         HPDF_Stream  afmdata,
                         HPDF_Stream  pfmdata);


static const char*
LoadTTFontFromStream (HPDF_Doc         pdf,
                      HPDF_Stream      font_data,
                      HPDF_BOOL        embedding,
                       const char      *file_name);


static const char*
LoadTTFontFromStream2 (HPDF_Doc         pdf,
                       HPDF_Stream      font_data,
                       HPDF_UINT        index,
                       HPDF_BOOL        embedding,
                       const char      *file_name);


/*---------------------------------------------------------------------------*/

HPDF_EXPORT(const char *)
HPDF_GetVersion (void)
{
    return HPDF_VERSION_TEXT;
}


HPDF_BOOL
HPDF_Doc_Validate  (HPDF_Doc  pdf)
{
    HPDF_PTRACE ((" HPDF_Doc_Validate\n"));

    if (!pdf || pdf->sig_bytes != HPDF_SIG_BYTES)
        return HPDF_FALSE;
    else
        return HPDF_TRUE;
}


HPDF_EXPORT(HPDF_BOOL)
HPDF_HasDoc  (HPDF_Doc  pdf)
{
    HPDF_PTRACE ((" HPDF_HasDoc\n"));

    if (!pdf || pdf->sig_bytes != HPDF_SIG_BYTES)
        return HPDF_FALSE;

    if (!pdf->catalog || pdf->error.error_no != HPDF_NOERROR) {
        HPDF_RaiseError (&pdf->error, HPDF_INVALID_DOCUMENT, 0);
        return HPDF_FALSE;
    } else
        return HPDF_TRUE;
}


HPDF_EXPORT(HPDF_Doc)
HPDF_New  (HPDF_Error_Handler    user_error_fn,
           void                  *user_data)
{
    HPDF_PTRACE ((" HPDF_New\n"));

    return HPDF_NewEx (user_error_fn, NULL, NULL, 0, user_data);
}


HPDF_EXPORT(HPDF_Doc)
HPDF_NewEx  (HPDF_Error_Handler    user_error_fn,
             HPDF_Alloc_Func       user_alloc_fn,
             HPDF_Free_Func        user_free_fn,
             HPDF_UINT             mem_pool_buf_size,
             void                 *user_data)
{
    HPDF_Doc pdf;
    HPDF_MMgr mmgr;
    HPDF_Error_Rec tmp_error;

    HPDF_PTRACE ((" HPDF_NewEx\n"));

    /* initialize temporary-error object */
    HPDF_Error_Init (&tmp_error, user_data);

    /* create memory-manager object */
    mmgr = HPDF_MMgr_New (&tmp_error, mem_pool_buf_size, user_alloc_fn,
            user_free_fn);
    if (!mmgr) {
        HPDF_CheckError (&tmp_error);
        return NULL;
    }

    /* now create pdf_doc object */
    pdf = HPDF_GetMem (mmgr, sizeof (HPDF_Doc_Rec));
    if (!pdf) {
        HPDF_MMgr_Free (mmgr);
        HPDF_CheckError (&tmp_error);
        return NULL;
    }

    HPDF_MemSet (pdf, 0, sizeof (HPDF_Doc_Rec));
    pdf->sig_bytes = HPDF_SIG_BYTES;
    pdf->mmgr = mmgr;
    pdf->pdf_version = HPDF_VER_13;
    pdf->compression_mode = HPDF_COMP_NONE;

    /* copy the data of temporary-error object to the one which is
       included in pdf_doc object */
    pdf->error = tmp_error;

    /* switch the error-object of memory-manager */
    mmgr->error = &pdf->error;

    if (HPDF_NewDoc (pdf) != HPDF_OK) {
        HPDF_Free (pdf);
        HPDF_CheckError (&tmp_error);
        return NULL;
    }

    pdf->error.error_fn = user_error_fn;

    return pdf;
}


HPDF_EXPORT(void)
HPDF_Free  (HPDF_Doc  pdf)
{
    HPDF_PTRACE ((" HPDF_Free\n"));

    if (pdf) {
        HPDF_MMgr mmgr = pdf->mmgr;

        HPDF_FreeDocAll (pdf);

        pdf->sig_bytes = 0;

        HPDF_FreeMem (mmgr, pdf);
        HPDF_MMgr_Free (mmgr);
    }
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_NewDoc  (HPDF_Doc  pdf)
{
    char buf[HPDF_TMP_BUF_SIZ];
    char *ptr = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    const char *version;

    HPDF_PTRACE ((" HPDF_NewDoc\n"));

    if (!HPDF_Doc_Validate (pdf))
        return HPDF_DOC_INVALID_OBJECT;

    HPDF_FreeDoc (pdf);

    pdf->xref = HPDF_Xref_New (pdf->mmgr, 0);
    if (!pdf->xref)
        return HPDF_CheckError (&pdf->error);

    pdf->trailer = pdf->xref->trailer;

    pdf->font_mgr = HPDF_List_New (pdf->mmgr, HPDF_DEF_ITEMS_PER_BLOCK);
    if (!pdf->font_mgr)
        return HPDF_CheckError (&pdf->error);

    if (!pdf->fontdef_list) {
        pdf->fontdef_list = HPDF_List_New (pdf->mmgr,
                HPDF_DEF_ITEMS_PER_BLOCK);
        if (!pdf->fontdef_list)
            return HPDF_CheckError (&pdf->error);
    }

    if (!pdf->encoder_list) {
        pdf->encoder_list = HPDF_List_New (pdf->mmgr,
                HPDF_DEF_ITEMS_PER_BLOCK);
        if (!pdf->encoder_list)
            return HPDF_CheckError (&pdf->error);
    }

    pdf->catalog = HPDF_Catalog_New (pdf->mmgr, pdf->xref);
    if (!pdf->catalog)
        return HPDF_CheckError (&pdf->error);

    pdf->root_pages = HPDF_Catalog_GetRoot (pdf->catalog);
    if (!pdf->root_pages)
        return HPDF_CheckError (&pdf->error);

    pdf->page_list = HPDF_List_New (pdf->mmgr, HPDF_DEF_PAGE_LIST_NUM);
    if (!pdf->page_list)
        return HPDF_CheckError (&pdf->error);

    pdf->cur_pages = pdf->root_pages;

    ptr = (char *)HPDF_StrCpy (ptr, (const char *)"Haru Free PDF Library ", eptr);
    version = HPDF_GetVersion ();
    HPDF_StrCpy (ptr, version, eptr);

    if (HPDF_SetInfoAttr (pdf, HPDF_INFO_PRODUCER, buf) != HPDF_OK)
        return HPDF_CheckError (&pdf->error);

    return HPDF_OK;
}


HPDF_EXPORT(void)
HPDF_FreeDoc  (HPDF_Doc  pdf)
{
    HPDF_PTRACE ((" HPDF_FreeDoc\n"));

    if (HPDF_Doc_Validate (pdf)) {
        if (pdf->xref) {
           HPDF_Xref_Free (pdf->xref);
           pdf->xref = NULL;
        }

        if (pdf->font_mgr) {
            HPDF_List_Free (pdf->font_mgr);
            pdf->font_mgr = NULL;
        }

        if (pdf->fontdef_list)
            CleanupFontDefList (pdf);

        HPDF_MemSet(pdf->ttfont_tag, 0, 6);

        pdf->pdf_version = HPDF_VER_13;
        pdf->outlines = NULL;
        pdf->catalog = NULL;
        pdf->root_pages = NULL;
        pdf->cur_pages = NULL;
        pdf->cur_page = NULL;
        pdf->encrypt_on = HPDF_FALSE;
        pdf->cur_page_num = 0;
        pdf->cur_encoder = NULL;
        pdf->def_encoder = NULL;
        pdf->page_per_pages = 0;

        if (pdf->page_list) {
            HPDF_List_Free (pdf->page_list);
            pdf->page_list = NULL;
        }

        pdf->encrypt_dict = NULL;
        pdf->info = NULL;

        HPDF_Error_Reset (&pdf->error);

        if (pdf->stream) {
            HPDF_Stream_Free (pdf->stream);
            pdf->stream = NULL;
        }
    }
}


HPDF_EXPORT(void)
HPDF_FreeDocAll  (HPDF_Doc  pdf)
{
    HPDF_PTRACE ((" HPDF_FreeDocAll\n"));

    if (HPDF_Doc_Validate (pdf)) {
        HPDF_FreeDoc (pdf);

        if (pdf->fontdef_list)
            FreeFontDefList (pdf);

        if (pdf->encoder_list)
            FreeEncoderList (pdf);

        pdf->compression_mode = HPDF_COMP_NONE;

        HPDF_Error_Reset (&pdf->error);
    }
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetPagesConfiguration  (HPDF_Doc    pdf,
                             HPDF_UINT   page_per_pages)
{
    HPDF_PTRACE ((" HPDF_SetPagesConfiguration\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (pdf->cur_page)
        return HPDF_RaiseError (&pdf->error, HPDF_INVALID_DOCUMENT_STATE, 0);

    if (page_per_pages > HPDF_LIMIT_MAX_ARRAY)
        return HPDF_RaiseError (&pdf->error, HPDF_INVALID_PARAMETER, 0);

    if (pdf->cur_pages == pdf->root_pages) {
        pdf->cur_pages = HPDF_Doc_AddPagesTo (pdf, pdf->root_pages);
        if (!pdf->cur_pages)
            return pdf->error.error_no;
        pdf->cur_page_num = 0;
    }

    pdf->page_per_pages = page_per_pages;

    return HPDF_OK;
}


static HPDF_STATUS
WriteHeader  (HPDF_Doc      pdf,
              HPDF_Stream   stream)
{
    HPDF_UINT idx = (HPDF_INT)pdf->pdf_version;

    HPDF_PTRACE ((" WriteHeader\n"));

    if (HPDF_Stream_WriteStr (stream, HPDF_VERSION_STR[idx]) != HPDF_OK)
        return pdf->error.error_no;

    return HPDF_OK;
}


static HPDF_STATUS
PrepareTrailer  (HPDF_Doc    pdf)
{
    HPDF_PTRACE ((" PrepareTrailer\n"));

    if (HPDF_Dict_Add (pdf->trailer, "Root", pdf->catalog) != HPDF_OK)
        return pdf->error.error_no;

    if (HPDF_Dict_Add (pdf->trailer, "Info", pdf->info) != HPDF_OK)
        return pdf->error.error_no;

    return HPDF_OK;
}


HPDF_STATUS
HPDF_Doc_SetEncryptOn  (HPDF_Doc   pdf)
{
    HPDF_PTRACE ((" HPDF_Doc_SetEncryptOn\n"));

    if (pdf->encrypt_on)
        return HPDF_OK;

    if (!pdf->encrypt_dict)
        return HPDF_SetError (&pdf->error, HPDF_DOC_ENCRYPTDICT_NOT_FOUND,
                0);

    if (pdf->encrypt_dict->header.obj_id == HPDF_OTYPE_NONE)
        if (HPDF_Xref_Add (pdf->xref, pdf->encrypt_dict) != HPDF_OK)
            return pdf->error.error_no;

    if (HPDF_Dict_Add (pdf->trailer, "Encrypt", pdf->encrypt_dict) != HPDF_OK)
        return pdf->error.error_no;

    pdf->encrypt_on = HPDF_TRUE;

    return HPDF_OK;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_SetPassword  (HPDF_Doc          pdf,
                   const char  *owner_passwd,
                   const char  *user_passwd)
{
    HPDF_PTRACE ((" HPDF_SetPassword\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_DOC_INVALID_OBJECT;

    if (!pdf->encrypt_dict) {
        pdf->encrypt_dict = HPDF_EncryptDict_New (pdf->mmgr, pdf->xref);

        if (!pdf->encrypt_dict)
            return HPDF_CheckError (&pdf->error);
    }

    if (HPDF_EncryptDict_SetPassword (pdf->encrypt_dict, owner_passwd,
                user_passwd) != HPDF_OK)
        return HPDF_CheckError (&pdf->error);

    return HPDF_Doc_SetEncryptOn (pdf);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetPermission  (HPDF_Doc    pdf,
                     HPDF_UINT   permission)
{
    HPDF_Encrypt e;

    HPDF_PTRACE ((" HPDF_SetPermission\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_DOC_INVALID_OBJECT;

    e = HPDF_EncryptDict_GetAttr (pdf->encrypt_dict);

    if (!e)
        return HPDF_RaiseError (&pdf->error,
                HPDF_DOC_ENCRYPTDICT_NOT_FOUND, 0);
    else
        e->permission = permission;

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetEncryptionMode  (HPDF_Doc           pdf,
                         HPDF_EncryptMode   mode,
                         HPDF_UINT          key_len)
{
    HPDF_Encrypt e;

    HPDF_PTRACE ((" HPDF_SetEncryptionMode\n"));

    if (!HPDF_Doc_Validate (pdf))
        return HPDF_DOC_INVALID_OBJECT;

    e = HPDF_EncryptDict_GetAttr (pdf->encrypt_dict);

    if (!e)
        return HPDF_RaiseError (&pdf->error,
                HPDF_DOC_ENCRYPTDICT_NOT_FOUND, 0);
    else {
        if (mode == HPDF_ENCRYPT_R2)
            e->key_len = 5;
        else {
            /* if encryption mode is specified revision-3, the version of
             * pdf file is set to 1.4
             */
            pdf->pdf_version = HPDF_VER_14;

            if (key_len >= 5 && key_len <= 16)
                e->key_len = key_len;
            else if (key_len == 0)
                e->key_len = 16;
            else
                return HPDF_RaiseError (&pdf->error,
                        HPDF_INVALID_ENCRYPT_KEY_LEN, 0);
        }
        e->mode = mode;
    }

    return HPDF_OK;
}


HPDF_STATUS
HPDF_Doc_SetEncryptOff  (HPDF_Doc   pdf)
{
    HPDF_PTRACE ((" HPDF_Doc_SetEncryptOff\n"));

    if (!pdf->encrypt_on)
        return HPDF_OK;

    /* if encrypy-dict object is registered to cross-reference-table,
     * replace it to null-object.
     * additionally remove encrypt-dict object from trailer-object.
     */
    if (pdf->encrypt_dict) {
        HPDF_UINT obj_id = pdf->encrypt_dict->header.obj_id;

        if (obj_id & HPDF_OTYPE_INDIRECT) {
            HPDF_XrefEntry entry;
            HPDF_Null null_obj;

            HPDF_Dict_RemoveElement (pdf->trailer, "Encrypt");

            entry = HPDF_Xref_GetEntryByObjectId (pdf->xref,
                        obj_id & 0x00FFFFFF);

            if (!entry) {
                return HPDF_SetError (&pdf->error,
                        HPDF_DOC_ENCRYPTDICT_NOT_FOUND, 0);
            }

            null_obj = HPDF_Null_New (pdf->mmgr);
            if (!null_obj)
                return pdf->error.error_no;

            entry->obj = null_obj;
            null_obj->header.obj_id = obj_id | HPDF_OTYPE_INDIRECT;

            pdf->encrypt_dict->header.obj_id = HPDF_OTYPE_NONE;
        }
    }

    pdf->encrypt_on = HPDF_FALSE;
    return HPDF_OK;
}


HPDF_STATUS
HPDF_Doc_PrepareEncryption  (HPDF_Doc   pdf)
{
    HPDF_Encrypt e= HPDF_EncryptDict_GetAttr (pdf->encrypt_dict);
    HPDF_Dict info = GetInfo (pdf);
    HPDF_Array id;

    if (!e)
        return HPDF_DOC_ENCRYPTDICT_NOT_FOUND;

    if (!info)
        return pdf->error.error_no;

    if (HPDF_EncryptDict_Prepare (pdf->encrypt_dict, info, pdf->xref) !=
            HPDF_OK)
        return pdf->error.error_no;

    /* reset 'ID' to trailer-dictionary */
    id = HPDF_Dict_GetItem (pdf->trailer, "ID", HPDF_OCLASS_ARRAY);
    if (!id) {
        id = HPDF_Array_New (pdf->mmgr);

        if (!id || HPDF_Dict_Add (pdf->trailer, "ID", id) != HPDF_OK)
            return pdf->error.error_no;
    } else
        HPDF_Array_Clear (id);

    if (HPDF_Array_Add (id, HPDF_Binary_New (pdf->mmgr, e->encrypt_id,
                HPDF_ID_LEN)) != HPDF_OK)
        return pdf->error.error_no;

    if (HPDF_Array_Add (id, HPDF_Binary_New (pdf->mmgr, e->encrypt_id,
                HPDF_ID_LEN)) != HPDF_OK)
        return pdf->error.error_no;

    return HPDF_OK;
}


static HPDF_STATUS
InternalSaveToStream  (HPDF_Doc      pdf,
                       HPDF_Stream   stream)
{
    HPDF_STATUS ret;

    if ((ret = WriteHeader (pdf, stream)) != HPDF_OK)
        return ret;

    /* prepare trailer */
    if ((ret = PrepareTrailer (pdf)) != HPDF_OK)
        return ret;

    /* prepare encription */
    if (pdf->encrypt_on) {
        HPDF_Encrypt e= HPDF_EncryptDict_GetAttr (pdf->encrypt_dict);

        if ((ret = HPDF_Doc_PrepareEncryption (pdf)) != HPDF_OK)
            return ret;

        if ((ret = HPDF_Xref_WriteToStream (pdf->xref, stream, e)) != HPDF_OK)
            return ret;
    } else {
        if ((ret = HPDF_Xref_WriteToStream (pdf->xref, stream, NULL)) !=
                HPDF_OK)
            return ret;
    }

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SaveToStream  (HPDF_Doc   pdf)
{
    HPDF_PTRACE ((" HPDF_SaveToStream\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (!pdf->stream)
        pdf->stream = HPDF_MemStream_New (pdf->mmgr, HPDF_STREAM_BUF_SIZ);

    if (!HPDF_Stream_Validate (pdf->stream))
        return HPDF_RaiseError (&pdf->error, HPDF_INVALID_STREAM, 0);

    HPDF_MemStream_FreeData (pdf->stream);

    if (InternalSaveToStream (pdf, pdf->stream) != HPDF_OK)
        return HPDF_CheckError (&pdf->error);

    return HPDF_OK;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_GetContents   (HPDF_Doc   pdf,
                   HPDF_BYTE  *buf,
                 HPDF_UINT32  *size)
{
    HPDF_Stream stream;
    HPDF_UINT isize = *size;
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_GetContents\n"));

    if (!HPDF_HasDoc (pdf)) {
        return HPDF_INVALID_DOCUMENT;
    }

    stream = HPDF_MemStream_New (pdf->mmgr, HPDF_STREAM_BUF_SIZ);

    if (!stream) {
        return HPDF_CheckError (&pdf->error);
    }

    if (InternalSaveToStream (pdf, stream) != HPDF_OK) {
        HPDF_Stream_Free (stream);
        return HPDF_CheckError (&pdf->error);
    }

    ret = HPDF_Stream_Read (stream, buf, &isize);

    *size = isize;
    HPDF_Stream_Free (stream);

    return ret;
}

HPDF_EXPORT(HPDF_UINT32)
HPDF_GetStreamSize  (HPDF_Doc   pdf)
{
    HPDF_PTRACE ((" HPDF_GetStreamSize\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (!HPDF_Stream_Validate (pdf->stream))
        return 0;

    return HPDF_Stream_Size(pdf->stream);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_ReadFromStream  (HPDF_Doc       pdf,
                      HPDF_BYTE     *buf,
                      HPDF_UINT32   *size)
{
    HPDF_UINT isize = *size;
    HPDF_STATUS ret;

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (!HPDF_Stream_Validate (pdf->stream))
        return HPDF_RaiseError (&pdf->error, HPDF_INVALID_OPERATION, 0);

    if (*size == 0)
        return HPDF_RaiseError (&pdf->error, HPDF_INVALID_PARAMETER, 0);

    ret = HPDF_Stream_Read (pdf->stream, buf, &isize);

    *size = isize;

    if (ret != HPDF_OK)
        HPDF_CheckError (&pdf->error);

    return ret;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_ResetStream  (HPDF_Doc     pdf)
{
    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (!HPDF_Stream_Validate (pdf->stream))
        return HPDF_RaiseError (&pdf->error, HPDF_INVALID_OPERATION, 0);

    return HPDF_Stream_Seek (pdf->stream, 0, HPDF_SEEK_SET);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SaveToFile  (HPDF_Doc     pdf,
                  const char  *file_name)
{
    HPDF_Stream stream;

    HPDF_PTRACE ((" HPDF_SaveToFile\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    stream = HPDF_FileWriter_New (pdf->mmgr, file_name);
    if (!stream)
        return HPDF_CheckError (&pdf->error);

    InternalSaveToStream (pdf, stream);

    HPDF_Stream_Free (stream);

    return HPDF_CheckError (&pdf->error);
}


HPDF_EXPORT(HPDF_Page)
HPDF_GetCurrentPage  (HPDF_Doc   pdf)
{
    HPDF_PTRACE ((" HPDF_GetCurrentPage\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    return pdf->cur_page;
}


HPDF_EXPORT(HPDF_Page)
HPDF_GetPageByIndex  (HPDF_Doc    pdf,
                      HPDF_UINT   index)
{
    HPDF_Page ret;

    HPDF_PTRACE ((" HPDF_GetPageByIndex\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    ret = HPDF_List_ItemAt (pdf->page_list, index);
    if (!ret) {
        HPDF_RaiseError (&pdf->error, HPDF_INVALID_PAGE_INDEX, 0);
        return NULL;
    }

    return ret;
}


HPDF_Pages
HPDF_Doc_GetCurrentPages  (HPDF_Doc   pdf)
{
    HPDF_PTRACE ((" HPDF_GetCurrentPages\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    return pdf->cur_pages;
}


HPDF_STATUS
HPDF_Doc_SetCurrentPages  (HPDF_Doc     pdf,
                           HPDF_Pages   pages)
{
    HPDF_PTRACE ((" HPDF_Doc_SetCurrentPages\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (!HPDF_Pages_Validate (pages))
        return HPDF_SetError (&pdf->error, HPDF_INVALID_PAGES, 0);

    /* check whether the pages belong to the pdf */
    if (pdf->mmgr != pages->mmgr)
        return HPDF_SetError (&pdf->error, HPDF_INVALID_PAGES, 0);

    pdf->cur_pages = pages;

    return HPDF_OK;
}


HPDF_STATUS
HPDF_Doc_SetCurrentPage  (HPDF_Doc    pdf,
                          HPDF_Page   page)
{
    HPDF_PTRACE ((" HPDF_Doc_SetCurrentPage\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (!HPDF_Page_Validate (page))
        return HPDF_SetError (&pdf->error, HPDF_INVALID_PAGE, 0);

    /* check whether the page belong to the pdf */
    if (pdf->mmgr != page->mmgr)
        return HPDF_SetError (&pdf->error, HPDF_INVALID_PAGE, 0);

    pdf->cur_page = page;

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_Page)
HPDF_AddPage  (HPDF_Doc  pdf)
{
    HPDF_Page page;
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_AddPage\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    if (pdf->page_per_pages) {
        if (pdf->page_per_pages <= pdf->cur_page_num) {
            pdf->cur_pages = HPDF_Doc_AddPagesTo (pdf, pdf->root_pages);
            if (!pdf->cur_pages)
                return NULL;
            pdf->cur_page_num = 0;
        }
    }

    page = HPDF_Page_New (pdf->mmgr, pdf->xref);
    if (!page) {
        HPDF_CheckError (&pdf->error);
        return NULL;
    }

    if ((ret = HPDF_Pages_AddKids (pdf->cur_pages, page)) != HPDF_OK) {
        HPDF_RaiseError (&pdf->error, ret, 0);
        return NULL;
    }

    if ((ret = HPDF_List_Add (pdf->page_list, page)) != HPDF_OK) {
        HPDF_RaiseError (&pdf->error, ret, 0);
        return NULL;
    }

    pdf->cur_page = page;

    if (pdf->compression_mode & HPDF_COMP_TEXT)
        HPDF_Page_SetFilter (page, HPDF_STREAM_FILTER_FLATE_DECODE);

    pdf->cur_page_num++;

    return page;
}


HPDF_Pages
HPDF_Doc_AddPagesTo  (HPDF_Doc     pdf,
                      HPDF_Pages   parent)
{
    HPDF_Pages pages;

    HPDF_PTRACE ((" HPDF_AddPagesTo\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    if (!HPDF_Pages_Validate (parent)) {
        HPDF_RaiseError (&pdf->error, HPDF_INVALID_PAGES, 0);
        return NULL;
    }

    /* check whether the page belong to the pdf */
    if (pdf->mmgr != parent->mmgr) {
        HPDF_RaiseError (&pdf->error, HPDF_INVALID_PAGES, 0);
        return NULL;
    }

    pages = HPDF_Pages_New (pdf->mmgr, parent, pdf->xref);
    if (pages)
        pdf->cur_pages = pages;
    else
        HPDF_CheckError (&pdf->error);


    return  pages;
}


HPDF_EXPORT(HPDF_Page)
HPDF_InsertPage  (HPDF_Doc    pdf,
                  HPDF_Page   target)
{
    HPDF_Page page;
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_InsertPage\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    if (!HPDF_Page_Validate (target)) {
        HPDF_RaiseError (&pdf->error, HPDF_INVALID_PAGE, 0);
        return NULL;
    }

    /* check whether the page belong to the pdf */
    if (pdf->mmgr != target->mmgr) {
        HPDF_RaiseError (&pdf->error, HPDF_INVALID_PAGE, 0);
        return NULL;
    }

    page = HPDF_Page_New (pdf->mmgr, pdf->xref);
    if (!page) {
        HPDF_CheckError (&pdf->error);
        return NULL;
    }

    if ((ret = HPDF_Page_InsertBefore (page, target)) != HPDF_OK) {
        HPDF_RaiseError (&pdf->error, ret, 0);
        return NULL;
    }

    if ((ret = HPDF_List_Insert (pdf->page_list, target, page)) != HPDF_OK) {
        HPDF_RaiseError (&pdf->error, ret, 0);
        return NULL;
    }

    if (pdf->compression_mode & HPDF_COMP_TEXT)
        HPDF_Page_SetFilter (page, HPDF_STREAM_FILTER_FLATE_DECODE);

    return page;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetErrorHandler  (HPDF_Doc             pdf,
                       HPDF_Error_Handler   user_error_fn)
{
    if (!HPDF_Doc_Validate (pdf))
        return HPDF_INVALID_DOCUMENT;

    pdf->error.error_fn = user_error_fn;

    return HPDF_OK;
}


/*----- font handling -------------------------------------------------------*/


static void
FreeFontDefList  (HPDF_Doc   pdf)
{
    HPDF_List list = pdf->fontdef_list;
    HPDF_UINT i;

    HPDF_PTRACE ((" HPDF_Doc_FreeFontDefList\n"));

    for (i = 0; i < list->count; i++) {
        HPDF_FontDef def = (HPDF_FontDef)HPDF_List_ItemAt (list, i);

        HPDF_FontDef_Free (def);
    }

    HPDF_List_Free (list);

    pdf->fontdef_list = NULL;
}

static void
CleanupFontDefList  (HPDF_Doc  pdf)
{
    HPDF_List list = pdf->fontdef_list;
    HPDF_UINT i;

    HPDF_PTRACE ((" CleanupFontDefList\n"));

    for (i = 0; i < list->count; i++) {
        HPDF_FontDef def = (HPDF_FontDef)HPDF_List_ItemAt (list, i);

        HPDF_FontDef_Cleanup (def);
    }
}


HPDF_FontDef
HPDF_Doc_FindFontDef  (HPDF_Doc          pdf,
                       const char  *font_name)
{
    HPDF_List list = pdf->fontdef_list;
    HPDF_UINT i;

    HPDF_PTRACE ((" HPDF_Doc_FindFontDef\n"));

    for (i = 0; i < list->count; i++) {
        HPDF_FontDef def = (HPDF_FontDef)HPDF_List_ItemAt (list, i);

        if (HPDF_StrCmp (font_name, def->base_font) == 0) {
            if (def->type == HPDF_FONTDEF_TYPE_UNINITIALIZED) {
                if (!def->init_fn ||
                    def->init_fn (def) != HPDF_OK)
                    return NULL;
            }

            return def;
        }
    }

    return NULL;
}


HPDF_STATUS
HPDF_Doc_RegisterFontDef  (HPDF_Doc       pdf,
                           HPDF_FontDef   fontdef)
{
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_Doc_RegisterFontDef\n"));

    if (!fontdef)
        return HPDF_SetError (&pdf->error, HPDF_INVALID_OBJECT, 0);

    if (HPDF_Doc_FindFontDef (pdf, fontdef->base_font) != NULL) {
        HPDF_FontDef_Free (fontdef);
        return HPDF_SetError (&pdf->error, HPDF_DUPLICATE_REGISTRATION, 0);
    }

    if ((ret = HPDF_List_Add (pdf->fontdef_list, fontdef)) != HPDF_OK) {
        HPDF_FontDef_Free (fontdef);
        return HPDF_SetError (&pdf->error, ret, 0);
    }

    return HPDF_OK;
}



HPDF_FontDef
HPDF_GetFontDef  (HPDF_Doc          pdf,
                  const char  *font_name)
{
    HPDF_STATUS ret;
    HPDF_FontDef def;

    HPDF_PTRACE ((" HPDF_GetFontDef\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    def = HPDF_Doc_FindFontDef (pdf, font_name);

    if (!def) {
        def = HPDF_Base14FontDef_New (pdf->mmgr, font_name);

        if (!def)
            return NULL;

        if ((ret = HPDF_List_Add (pdf->fontdef_list, def)) != HPDF_OK) {
            HPDF_FontDef_Free (def);
            HPDF_RaiseError (&pdf->error, ret, 0);
            def = NULL;
        }
    }

    return def;
}


/*----- encoder handling ----------------------------------------------------*/


HPDF_Encoder
HPDF_Doc_FindEncoder  (HPDF_Doc         pdf,
                       const char  *encoding_name)
{
    HPDF_List list = pdf->encoder_list;
    HPDF_UINT i;

    HPDF_PTRACE ((" HPDF_Doc_FindEncoder\n"));

    for (i = 0; i < list->count; i++) {
        HPDF_Encoder encoder = (HPDF_Encoder)HPDF_List_ItemAt (list, i);

        if (HPDF_StrCmp (encoding_name, encoder->name) == 0) {

            /* if encoder is uninitialize, call init_fn() */
            if (encoder->type == HPDF_ENCODER_TYPE_UNINITIALIZED) {
                if (!encoder->init_fn ||
                    encoder->init_fn (encoder) != HPDF_OK)
                    return NULL;
            }

            return encoder;
        }
    }

    return NULL;
}



HPDF_STATUS
HPDF_Doc_RegisterEncoder  (HPDF_Doc       pdf,
                           HPDF_Encoder   encoder)
{
    HPDF_STATUS ret;

    if (!encoder)
        return HPDF_SetError (&pdf->error, HPDF_INVALID_OBJECT, 0);

    if (HPDF_Doc_FindEncoder (pdf, encoder->name) != NULL) {
        HPDF_Encoder_Free (encoder);
        return HPDF_SetError (&pdf->error, HPDF_DUPLICATE_REGISTRATION, 0);
    }

    if ((ret = HPDF_List_Add (pdf->encoder_list, encoder)) != HPDF_OK) {
        HPDF_Encoder_Free (encoder);
        return HPDF_SetError (&pdf->error, ret, 0);
    }

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_Encoder)
HPDF_GetEncoder  (HPDF_Doc         pdf,
                  const char  *encoding_name)
{
    HPDF_Encoder encoder;
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_GetEncoder\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    encoder = HPDF_Doc_FindEncoder (pdf, encoding_name);

    if (!encoder) {
        encoder = HPDF_BasicEncoder_New (pdf->mmgr, encoding_name);

        if (!encoder) {
            HPDF_CheckError (&pdf->error);
            return NULL;
        }

        if ((ret = HPDF_List_Add (pdf->encoder_list, encoder)) != HPDF_OK) {
            HPDF_Encoder_Free (encoder);
            HPDF_RaiseError (&pdf->error, ret, 0);
            return NULL;
        }
    }

    return encoder;
}


HPDF_EXPORT(HPDF_Encoder)
HPDF_GetCurrentEncoder  (HPDF_Doc    pdf)
{
    if (!HPDF_HasDoc (pdf))
        return NULL;

    return pdf->cur_encoder;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetCurrentEncoder  (HPDF_Doc    pdf,
                         const char  *encoding_name)
{
    HPDF_Encoder encoder;

    if (!HPDF_HasDoc (pdf))
        return HPDF_GetError (pdf);

    encoder = HPDF_GetEncoder (pdf, encoding_name);

    if (!encoder)
        return HPDF_GetError (pdf);

    pdf->cur_encoder = encoder;

    return HPDF_OK;
}


static void
FreeEncoderList  (HPDF_Doc  pdf)
{
    HPDF_List list = pdf->encoder_list;
    HPDF_UINT i;

    HPDF_PTRACE ((" FreeEncoderList\n"));

    for (i = 0; i < list->count; i++) {
        HPDF_Encoder encoder = (HPDF_Encoder)HPDF_List_ItemAt (list, i);

        HPDF_Encoder_Free (encoder);
    }

    HPDF_List_Free (list);

    pdf->encoder_list = NULL;
}


/*----- font handling -------------------------------------------------------*/


HPDF_Font
HPDF_Doc_FindFont  (HPDF_Doc          pdf,
                    const char  *font_name,
                    const char  *encoding_name)
{
    HPDF_UINT i;
    HPDF_Font font;

    HPDF_PTRACE ((" HPDF_Doc_FindFont\n"));

    for (i = 0; i < pdf->font_mgr->count; i++) {
        HPDF_FontAttr attr;

        font = (HPDF_Font)HPDF_List_ItemAt (pdf->font_mgr, i);
        attr = (HPDF_FontAttr) font->attr;

        if (HPDF_StrCmp (attr->fontdef->base_font, font_name) == 0 &&
                HPDF_StrCmp (attr->encoder->name, encoding_name) == 0)
            return font;
    }

    return NULL;
}


HPDF_EXPORT(HPDF_Font)
HPDF_GetFont  (HPDF_Doc          pdf,
               const char  *font_name,
               const char  *encoding_name)
{
    HPDF_FontDef fontdef = NULL;
    HPDF_Encoder encoder = NULL;
    HPDF_Font font;

    HPDF_PTRACE ((" HPDF_GetFont\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    if (!font_name) {
        HPDF_RaiseError (&pdf->error, HPDF_INVALID_FONT_NAME, 0);
        return NULL;
    }

    /* if encoding-name is not specified, find default-encoding of fontdef
     */
    if (!encoding_name) {
        fontdef = HPDF_GetFontDef (pdf, font_name);

        if (fontdef) {
            HPDF_Type1FontDefAttr attr = (HPDF_Type1FontDefAttr)fontdef->attr;

            if (fontdef->type == HPDF_FONTDEF_TYPE_TYPE1 &&
                HPDF_StrCmp (attr->encoding_scheme,
                            HPDF_ENCODING_FONT_SPECIFIC) == 0)
                encoder = HPDF_GetEncoder (pdf, HPDF_ENCODING_FONT_SPECIFIC);
            else
                encoder = HPDF_GetEncoder (pdf, HPDF_ENCODING_STANDARD);
        } else {
            HPDF_CheckError (&pdf->error);
            return NULL;
        }

        if (!encoder) {
            HPDF_CheckError (&pdf->error);
            return NULL;
        }

        font = HPDF_Doc_FindFont (pdf, font_name, encoder->name);
    } else {
        font = HPDF_Doc_FindFont (pdf, font_name, encoding_name);
    }

    if (font)
        return font;

    if (!fontdef) {
        fontdef = HPDF_GetFontDef (pdf, font_name);

        if (!fontdef) {
            HPDF_CheckError (&pdf->error);
            return NULL;
        }
    }

    if (!encoder) {
        encoder = HPDF_GetEncoder (pdf, encoding_name);

        if (!encoder)
            return NULL;
    }

    switch (fontdef->type) {
        case HPDF_FONTDEF_TYPE_TYPE1:
            font = HPDF_Type1Font_New (pdf->mmgr, fontdef, encoder, pdf->xref);

            if (font)
                HPDF_List_Add (pdf->font_mgr, font);

            break;
        case HPDF_FONTDEF_TYPE_TRUETYPE:
            if (encoder->type == HPDF_ENCODER_TYPE_DOUBLE_BYTE)
                font = HPDF_Type0Font_New (pdf->mmgr, fontdef, encoder,
                        pdf->xref);
            else
                font = HPDF_TTFont_New (pdf->mmgr, fontdef, encoder, pdf->xref);

            if (font)
                HPDF_List_Add (pdf->font_mgr, font);

            break;
        case HPDF_FONTDEF_TYPE_CID:
            font = HPDF_Type0Font_New (pdf->mmgr, fontdef, encoder, pdf->xref);

            if (font)
                HPDF_List_Add (pdf->font_mgr, font);

            break;
        default:
            HPDF_RaiseError (&pdf->error, HPDF_UNSUPPORTED_FONT_TYPE, 0);
            return NULL;
    }

    if (!font)
        HPDF_CheckError (&pdf->error);

    if (font && (pdf->compression_mode & HPDF_COMP_METADATA))
        font->filter = HPDF_STREAM_FILTER_FLATE_DECODE;

    return font;
}


HPDF_EXPORT(const char*)
HPDF_LoadType1FontFromFile  (HPDF_Doc     pdf,
                             const char  *afm_file_name,
                             const char  *data_file_name)
{
    HPDF_Stream afm;
    HPDF_Stream pfm = NULL;
    const char *ret;

    HPDF_PTRACE ((" HPDF_LoadType1FontFromFile\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    /* create file stream */
    afm = HPDF_FileReader_New (pdf->mmgr, afm_file_name);

    if (data_file_name)
        pfm = HPDF_FileReader_New (pdf->mmgr, data_file_name);

    if (HPDF_Stream_Validate (afm) &&
            (!data_file_name || HPDF_Stream_Validate (pfm))) {

        ret = LoadType1FontFromStream (pdf, afm, pfm);
    } else
        ret = NULL;

    /* destroy file stream */
    if (afm)
        HPDF_Stream_Free (afm);

    if (pfm)
        HPDF_Stream_Free (pfm);

    if (!ret)
        HPDF_CheckError (&pdf->error);

    return ret;
}


static const char*
LoadType1FontFromStream  (HPDF_Doc      pdf,
                          HPDF_Stream   afmdata,
                          HPDF_Stream   pfmdata)
{
    HPDF_FontDef def;

    HPDF_PTRACE ((" HPDF_LoadType1FontFromStream\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    def = HPDF_Type1FontDef_Load (pdf->mmgr, afmdata, pfmdata);
    if (def) {
        HPDF_FontDef  tmpdef = HPDF_Doc_FindFontDef (pdf, def->base_font);
        if (tmpdef) {
            HPDF_FontDef_Free (def);
            HPDF_SetError (&pdf->error, HPDF_FONT_EXISTS, 0);
            return NULL;
        }

        if (HPDF_List_Add (pdf->fontdef_list, def) != HPDF_OK) {
            HPDF_FontDef_Free (def);
            return NULL;
        }
        return def->base_font;
    }
    return NULL;
}

HPDF_EXPORT(HPDF_FontDef)
HPDF_GetTTFontDefFromFile (HPDF_Doc      pdf,
                           const char   *file_name,
                           HPDF_BOOL     embedding)
{
	HPDF_Stream font_data;
	HPDF_FontDef def;

	HPDF_PTRACE ((" HPDF_GetTTFontDefFromFile\n"));

	/* create file stream */
	font_data = HPDF_FileReader_New (pdf->mmgr, file_name);

	if (HPDF_Stream_Validate (font_data)) {
		def = HPDF_TTFontDef_Load (pdf->mmgr, font_data, embedding);
	} else {
		HPDF_CheckError (&pdf->error);
		return NULL;
	}

	return def;
}

HPDF_EXPORT(const char*)
HPDF_LoadTTFontFromFile (HPDF_Doc         pdf,
                         const char      *file_name,
                         HPDF_BOOL        embedding)
{
    HPDF_Stream font_data;
    const char *ret;

    HPDF_PTRACE ((" HPDF_LoadTTFontFromFile\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    /* create file stream */
    font_data = HPDF_FileReader_New (pdf->mmgr, file_name);

    if (HPDF_Stream_Validate (font_data)) {
        ret = LoadTTFontFromStream (pdf, font_data, embedding, file_name);
    } else
        ret = NULL;

    if (!ret)
        HPDF_CheckError (&pdf->error);

    return ret;
}


static const char*
LoadTTFontFromStream (HPDF_Doc         pdf,
                      HPDF_Stream      font_data,
                      HPDF_BOOL        embedding,
                      const char      *file_name)
{
    HPDF_FontDef def;

    HPDF_PTRACE ((" HPDF_LoadTTFontFromStream\n"));
    HPDF_UNUSED (file_name);

    def = HPDF_TTFontDef_Load (pdf->mmgr, font_data, embedding);
    if (def) {
        HPDF_FontDef  tmpdef = HPDF_Doc_FindFontDef (pdf, def->base_font);
        if (tmpdef) {
            HPDF_FontDef_Free (def);
            return tmpdef->base_font;
        }

        if (HPDF_List_Add (pdf->fontdef_list, def) != HPDF_OK) {
            HPDF_FontDef_Free (def);
            return NULL;
        }
    } else
        return NULL;

    if (embedding) {
        if (pdf->ttfont_tag[0] == 0) {
            HPDF_MemCpy (pdf->ttfont_tag, (HPDF_BYTE *)"HPDFAA", 6);
        } else {
            HPDF_INT i;

            for (i = 5; i >= 0; i--) {
                pdf->ttfont_tag[i] += 1;
                if (pdf->ttfont_tag[i] > 'Z')
                    pdf->ttfont_tag[i] = 'A';
                else
                    break;
            }
        }

        HPDF_TTFontDef_SetTagName (def, (char *)pdf->ttfont_tag);
    }

    return def->base_font;
}


HPDF_EXPORT(const char*)
HPDF_LoadTTFontFromFile2 (HPDF_Doc         pdf,
                          const char      *file_name,
                          HPDF_UINT        index,
                          HPDF_BOOL        embedding)
{
    HPDF_Stream font_data;
    const char *ret;

    HPDF_PTRACE ((" HPDF_LoadTTFontFromFile2\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    /* create file stream */
    font_data = HPDF_FileReader_New (pdf->mmgr, file_name);

    if (HPDF_Stream_Validate (font_data)) {
        ret = LoadTTFontFromStream2 (pdf, font_data, index, embedding, file_name);
    } else
        ret = NULL;

    if (!ret)
        HPDF_CheckError (&pdf->error);

    return ret;
}


static const char*
LoadTTFontFromStream2 (HPDF_Doc         pdf,
                       HPDF_Stream      font_data,
                       HPDF_UINT        index,
                       HPDF_BOOL        embedding,
                       const char      *file_name)
{
    HPDF_FontDef def;

    HPDF_PTRACE ((" HPDF_LoadTTFontFromStream2\n"));
    HPDF_UNUSED (file_name);

    def = HPDF_TTFontDef_Load2 (pdf->mmgr, font_data, index, embedding);
    if (def) {
        HPDF_FontDef  tmpdef = HPDF_Doc_FindFontDef (pdf, def->base_font);
        if (tmpdef) {
            HPDF_FontDef_Free (def);
            return tmpdef->base_font;
        }

        if (HPDF_List_Add (pdf->fontdef_list, def) != HPDF_OK) {
            HPDF_FontDef_Free (def);
            return NULL;
        }
    } else
        return NULL;

    if (embedding) {
        if (pdf->ttfont_tag[0] == 0) {
            HPDF_MemCpy (pdf->ttfont_tag, (HPDF_BYTE *)"HPDFAA", 6);
        } else {
            HPDF_INT i;

            for (i = 5; i >= 0; i--) {
                pdf->ttfont_tag[i] += 1;
                if (pdf->ttfont_tag[i] > 'Z')
                    pdf->ttfont_tag[i] = 'A';
                else
                    break;
            }
        }

        HPDF_TTFontDef_SetTagName (def, (char *)pdf->ttfont_tag);
    }

    return def->base_font;
}


HPDF_EXPORT(HPDF_Image)
HPDF_LoadRawImageFromFile  (HPDF_Doc          pdf,
                            const char       *filename,
                            HPDF_UINT         width,
                            HPDF_UINT         height,
                            HPDF_ColorSpace   color_space)
{
    HPDF_Stream imagedata;
    HPDF_Image image;

    HPDF_PTRACE ((" HPDF_LoadRawImageFromFile\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    /* create file stream */
    imagedata = HPDF_FileReader_New (pdf->mmgr, filename);

    if (HPDF_Stream_Validate (imagedata))
        image = HPDF_Image_LoadRawImage (pdf->mmgr, imagedata, pdf->xref, width,
                    height, color_space);
    else
        image = NULL;

    /* destroy file stream */
    HPDF_Stream_Free (imagedata);

    if (!image)
        HPDF_CheckError (&pdf->error);

    if (image && pdf->compression_mode & HPDF_COMP_IMAGE)
        image->filter = HPDF_STREAM_FILTER_FLATE_DECODE;

    return image;
}


HPDF_EXPORT(HPDF_Image)
HPDF_LoadRawImageFromMem  (HPDF_Doc           pdf,
                           const HPDF_BYTE   *buf,
                           HPDF_UINT          width,
                           HPDF_UINT          height,
                           HPDF_ColorSpace    color_space,
                           HPDF_UINT          bits_per_component)
{
    HPDF_Image image;

    HPDF_PTRACE ((" HPDF_LoadRawImageFromMem\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    /* Use directly HPDF_Image_LoadRaw1BitImageFromMem to save B/W images */
    if(color_space == HPDF_CS_DEVICE_GRAY && bits_per_component == 1) {
        return HPDF_Image_LoadRaw1BitImageFromMem (pdf, buf, width, height, (width+7)/8, HPDF_TRUE, HPDF_TRUE);
    }

    image = HPDF_Image_LoadRawImageFromMem (pdf->mmgr, buf, pdf->xref, width, height, color_space, bits_per_component);

    if (!image)
        HPDF_CheckError (&pdf->error);

    if (image && pdf->compression_mode & HPDF_COMP_IMAGE) {
        image->filter = HPDF_STREAM_FILTER_FLATE_DECODE;
    }

    return image;
}


HPDF_EXPORT(HPDF_Image)
HPDF_LoadJpegImageFromFile  (HPDF_Doc     pdf,
                             const char  *filename)
{
    HPDF_Stream imagedata;
    HPDF_Image image;

    HPDF_PTRACE ((" HPDF_LoadJpegImageFromFile\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    /* create file stream */
    imagedata = HPDF_FileReader_New (pdf->mmgr, filename);

    if (HPDF_Stream_Validate (imagedata))
        image = HPDF_Image_LoadJpegImage (pdf->mmgr, imagedata, pdf->xref);
    else
        image = NULL;

    /* destroy file stream */
    HPDF_Stream_Free (imagedata);

    if (!image)
        HPDF_CheckError (&pdf->error);

    return image;
}

HPDF_EXPORT(HPDF_Image)
HPDF_LoadJpegImageFromMem  (HPDF_Doc    pdf,
                     const HPDF_BYTE   *buffer,
                           HPDF_UINT    size)
{
	HPDF_Image image;

	HPDF_PTRACE ((" HPDF_LoadJpegImageFromMem\n"));

	if (!HPDF_HasDoc (pdf)) {
		return NULL;
	}

	image = HPDF_Image_LoadJpegImageFromMem (pdf->mmgr, buffer, size , pdf->xref);

	if (!image) {
		HPDF_CheckError (&pdf->error);
	}

	return image;
}

/*----- Catalog ------------------------------------------------------------*/

HPDF_EXPORT(HPDF_PageLayout)
HPDF_GetPageLayout  (HPDF_Doc   pdf)
{
    HPDF_PTRACE ((" HPDF_GetPageLayout\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_PAGE_LAYOUT_SINGLE;

    return HPDF_Catalog_GetPageLayout (pdf->catalog);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetPageLayout  (HPDF_Doc          pdf,
                     HPDF_PageLayout   layout)
{
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_GetPageLayout\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (layout < 0 || layout >= HPDF_PAGE_LAYOUT_EOF)
        return HPDF_RaiseError (&pdf->error, HPDF_PAGE_LAYOUT_OUT_OF_RANGE,
                (HPDF_STATUS)layout);

    if ((layout == HPDF_PAGE_LAYOUT_TWO_PAGE_LEFT || layout == HPDF_PAGE_LAYOUT_TWO_PAGE_RIGHT) && pdf->pdf_version < HPDF_VER_15)
        pdf->pdf_version = HPDF_VER_15 ;

    ret = HPDF_Catalog_SetPageLayout (pdf->catalog, layout);
    if (ret != HPDF_OK)
        HPDF_CheckError (&pdf->error);

    return HPDF_OK;
}

HPDF_EXPORT(HPDF_PageMode)
HPDF_GetPageMode  (HPDF_Doc   pdf)
{
    HPDF_PTRACE ((" HPDF_GetPageMode\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_PAGE_MODE_USE_NONE;

    return HPDF_Catalog_GetPageMode (pdf->catalog);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetPageMode  (HPDF_Doc        pdf,
                   HPDF_PageMode   mode)
{
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_GetPageMode\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (mode < 0 || mode >= HPDF_PAGE_MODE_EOF)
        return HPDF_RaiseError (&pdf->error, HPDF_PAGE_MODE_OUT_OF_RANGE,
                (HPDF_STATUS)mode);

    ret = HPDF_Catalog_SetPageMode (pdf->catalog, mode);
    if (ret != HPDF_OK)
        return HPDF_CheckError (&pdf->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetOpenAction  (HPDF_Doc            pdf,
                     HPDF_Destination    open_action)
{
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_SetOpenAction\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (open_action && !HPDF_Destination_Validate (open_action))
        return HPDF_RaiseError (&pdf->error, HPDF_INVALID_DESTINATION, 0);

    ret = HPDF_Catalog_SetOpenAction (pdf->catalog, open_action);
    if (ret != HPDF_OK)
        return HPDF_CheckError (&pdf->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_UINT)
HPDF_GetViewerPreference  (HPDF_Doc   pdf)
{
    HPDF_PTRACE ((" HPDF_Catalog_GetViewerPreference\n"));

    if (!HPDF_HasDoc (pdf))
        return 0;

    return HPDF_Catalog_GetViewerPreference (pdf->catalog);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetViewerPreference  (HPDF_Doc     pdf,
                           HPDF_UINT    value)
{
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_Catalog_SetViewerPreference\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    ret = HPDF_Catalog_SetViewerPreference (pdf->catalog, value);
    if (ret != HPDF_OK)
        return HPDF_CheckError (&pdf->error);

    pdf->pdf_version = HPDF_VER_16;

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_AddPageLabel  (HPDF_Doc             pdf,
                    HPDF_UINT            page_num,
                    HPDF_PageNumStyle    style,
                    HPDF_UINT            first_page,
                    const char     *prefix)
{
    HPDF_Dict page_label;
    HPDF_STATUS ret;

    HPDF_PTRACE ((" HPDF_AddPageLabel\n"));

    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    page_label = HPDF_PageLabel_New (pdf, style, first_page, prefix);

    if (!page_label)
        return HPDF_CheckError (&pdf->error);

    if (style < 0 || style >= HPDF_PAGE_NUM_STYLE_EOF)
        return HPDF_RaiseError (&pdf->error, HPDF_PAGE_NUM_STYLE_OUT_OF_RANGE,
                    (HPDF_STATUS)style);

    ret = HPDF_Catalog_AddPageLabel (pdf->catalog, page_num, page_label);
    if (ret != HPDF_OK)
        return HPDF_CheckError (&pdf->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_EmbeddedFile)
HPDF_AttachFile  (HPDF_Doc    pdf,
                  const char *file)
{
    HPDF_NameDict names;
    HPDF_NameTree ntree;
    HPDF_EmbeddedFile efile;
    HPDF_String name;
    HPDF_STATUS ret = HPDF_OK;

    HPDF_PTRACE ((" HPDF_AttachFile\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    names = HPDF_Catalog_GetNames (pdf->catalog);
    if (!names) {
        names = HPDF_NameDict_New (pdf->mmgr, pdf->xref);
        if (!names)
            return NULL;

        ret = HPDF_Catalog_SetNames (pdf->catalog, names);
        if (ret != HPDF_OK)
            return NULL;
    }

    ntree = HPDF_NameDict_GetNameTree (names, HPDF_NAME_EMBEDDED_FILES);
    if (!ntree) {
        ntree = HPDF_NameTree_New (pdf->mmgr, pdf->xref);
        if (!ntree)
            return NULL;

        ret = HPDF_NameDict_SetNameTree (names, HPDF_NAME_EMBEDDED_FILES, ntree);
        if (ret != HPDF_OK)
            return NULL;
    }

    efile = HPDF_EmbeddedFile_New (pdf->mmgr, pdf->xref, file);
    if (!efile)
        return NULL;

    name = HPDF_String_New (pdf->mmgr, file, NULL);
    if (!name)
        return NULL;

    ret += HPDF_NameTree_Add (ntree, name, efile);
    if (ret != HPDF_OK)
        return NULL;

    return efile;
}

/*----- Info ---------------------------------------------------------------*/

static HPDF_Dict
GetInfo  (HPDF_Doc  pdf)
{
    if (!HPDF_HasDoc (pdf))
        return NULL;

    if (!pdf->info) {
        pdf->info = HPDF_Dict_New (pdf->mmgr);

        if (!pdf->info || HPDF_Xref_Add (pdf->xref, pdf->info) != HPDF_OK)
            pdf->info = NULL;
    }

    return pdf->info;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetInfoAttr  (HPDF_Doc          pdf,
                   HPDF_InfoType     type,
                   const char  *value)
{
    HPDF_STATUS ret;
    HPDF_Dict info = GetInfo (pdf);

    HPDF_PTRACE((" HPDF_SetInfoAttr\n"));

    if (!info)
        return HPDF_CheckError (&pdf->error);

    ret = HPDF_Info_SetInfoAttr (info, type, value, pdf->cur_encoder);
    if (ret != HPDF_OK)
        return HPDF_CheckError (&pdf->error);

    return ret;
}


HPDF_EXPORT(const char*)
HPDF_GetInfoAttr  (HPDF_Doc        pdf,
                   HPDF_InfoType   type)
{
    const char *ret = NULL;
    HPDF_Dict info = GetInfo (pdf);

    HPDF_PTRACE((" HPDF_GetInfoAttr\n"));

    if (info)
        ret = HPDF_Info_GetInfoAttr (info, type);
    else
        HPDF_CheckError (&pdf->error);

    return ret;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetInfoDateAttr  (HPDF_Doc        pdf,
                       HPDF_InfoType   type,
                       HPDF_Date       value)
{
    HPDF_STATUS ret;
    HPDF_Dict info = GetInfo (pdf);

    HPDF_PTRACE((" HPDF_SetInfoDateAttr\n"));

    if (!info)
        return HPDF_CheckError (&pdf->error);

    ret = HPDF_Info_SetInfoDateAttr (info, type, value);
    if (ret != HPDF_OK)
        return HPDF_CheckError (&pdf->error);

    return ret;
}


HPDF_EXPORT(HPDF_Outline)
HPDF_CreateOutline  (HPDF_Doc       pdf,
                     HPDF_Outline   parent,
                     const char    *title,
                     HPDF_Encoder   encoder)
{
    HPDF_Outline outline;

    if (!HPDF_HasDoc (pdf))
        return NULL;

    if (!parent) {
        if (pdf->outlines) {
            parent = pdf->outlines;
        } else {
            pdf->outlines = HPDF_OutlineRoot_New (pdf->mmgr, pdf->xref);

            if (pdf->outlines) {
                HPDF_STATUS ret = HPDF_Dict_Add (pdf->catalog, "Outlines",
                            pdf->outlines);
                if (ret != HPDF_OK) {
                    HPDF_CheckError (&pdf->error);
                    pdf->outlines = NULL;
                    return NULL;
                }

                parent = pdf->outlines;
            } else {
                HPDF_CheckError (&pdf->error);
                return NULL;
            }
        }
    }

    if (!HPDF_Outline_Validate (parent) || pdf->mmgr != parent->mmgr) {
        HPDF_RaiseError (&pdf->error, HPDF_INVALID_OUTLINE, 0);
        return NULL;
    }

    outline = HPDF_Outline_New (pdf->mmgr, parent, title, encoder, pdf->xref);
    if (!outline)
        HPDF_CheckError (&pdf->error);

    return outline;
}


HPDF_EXPORT(HPDF_ExtGState)
HPDF_CreateExtGState  (HPDF_Doc  pdf)
{
    HPDF_ExtGState ext_gstate;

    if (!HPDF_HasDoc (pdf))
        return NULL;

    pdf->pdf_version = HPDF_VER_14;

    ext_gstate = HPDF_ExtGState_New (pdf->mmgr, pdf->xref);
    if (!ext_gstate)
        HPDF_CheckError (&pdf->error);

    return ext_gstate;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_SetCompressionMode  (HPDF_Doc    pdf,
                          HPDF_UINT   mode)
{
    if (!HPDF_Doc_Validate (pdf))
        return HPDF_INVALID_DOCUMENT;

    if (mode != (mode & HPDF_COMP_MASK))
        return HPDF_RaiseError (&pdf->error, HPDF_INVALID_COMPRESSION_MODE, 0);

#ifndef LIBHPDF_HAVE_NOZLIB
    pdf->compression_mode = mode;

    return HPDF_OK;

#else /* LIBHPDF_HAVE_NOZLIB */

    return HPDF_INVALID_COMPRESSION_MODE;

#endif /* LIBHPDF_HAVE_NOZLIB */
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_GetError  (HPDF_Doc   pdf)
{
    if (!HPDF_Doc_Validate (pdf))
        return HPDF_INVALID_DOCUMENT;

    return HPDF_Error_GetCode (&pdf->error);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_GetErrorDetail  (HPDF_Doc   pdf)
{
    if (!HPDF_Doc_Validate (pdf))
        return HPDF_INVALID_DOCUMENT;

    return HPDF_Error_GetDetailCode (&pdf->error);
}


HPDF_EXPORT(void)
HPDF_ResetError  (HPDF_Doc   pdf)
{
    if (!HPDF_Doc_Validate (pdf))
        return;

    HPDF_Error_Reset (&pdf->error);
}

/*
 * create an intententry
 */
HPDF_EXPORT(HPDF_OutputIntent)
HPDF_OutputIntent_New  (HPDF_Doc  pdf,
                      const char* identifier,
                      const char* condition,
                      const char* registry,
                      const char* info,
                      HPDF_Array  outputprofile)
{
    HPDF_OutputIntent intent;
    HPDF_STATUS ret = HPDF_OK;

    HPDF_PTRACE((" HPDF_OutputIntent_New\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    intent = HPDF_Dict_New (pdf->mmgr);
    if (!intent)
        return NULL;

    if (HPDF_Xref_Add (pdf->xref, intent) != HPDF_OK) {
        HPDF_Dict_Free(intent);
        return NULL;
    }

    ret += HPDF_Dict_AddName (intent, "Type", "OutputIntent");
    ret += HPDF_Dict_AddName (intent, "S", "GTS_PDFX");
    ret += HPDF_Dict_Add (intent, "OutputConditionIdentifier", HPDF_String_New (pdf->mmgr, identifier, NULL));
    ret += HPDF_Dict_Add (intent, "OutputCondition", HPDF_String_New (pdf->mmgr, condition,NULL));
    ret += HPDF_Dict_Add (intent, "RegistryName", HPDF_String_New (pdf->mmgr, registry, NULL));

    if (info != NULL) {
        ret += HPDF_Dict_Add (intent, "Info", HPDF_String_New (pdf->mmgr, info, NULL));
    }

    /* add ICC base stream */
    if (outputprofile != NULL) {
        ret += HPDF_Dict_Add (intent, "DestOutputProfile ", outputprofile);
    }

    if (ret != HPDF_OK) {
        HPDF_Dict_Free(intent);
        return NULL;
    }

    return intent;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_AddIntent(HPDF_Doc  pdf,
      HPDF_OutputIntent  intent)
{
    HPDF_Array intents;
    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    intents = HPDF_Dict_GetItem (pdf->catalog, "OutputIntents", HPDF_OCLASS_ARRAY);
    if (intents == NULL) {
        intents = HPDF_Array_New (pdf->mmgr);
        if (intents) {
            HPDF_STATUS ret = HPDF_Dict_Add (pdf->catalog, "OutputIntents", intents);
            if (ret != HPDF_OK) {
                HPDF_CheckError (&pdf->error);
                return HPDF_Error_GetDetailCode (&pdf->error);
            }
        }
    }
    HPDF_Array_Add(intents,intent);
    return HPDF_Error_GetDetailCode (&pdf->error);
}

/* "Perceptual", "RelativeColorimetric", "Saturation", "AbsoluteColorimetric" */
HPDF_EXPORT(HPDF_OutputIntent)
HPDF_ICC_LoadIccFromMem (HPDF_Doc   pdf,
		                HPDF_MMgr   mmgr,
                        HPDF_Stream iccdata,
                        HPDF_Xref   xref,
						int         numcomponent)
{
   HPDF_OutputIntent icc;
   HPDF_STATUS ret;

   HPDF_PTRACE ((" HPDF_ICC_LoadIccFromMem\n"));

   icc = HPDF_DictStream_New (mmgr, xref);
   if (!icc)
        return NULL;

   HPDF_Dict_AddNumber (icc, "N", numcomponent);
   switch (numcomponent) {
   case 1 :
	   HPDF_Dict_AddName (icc, "Alternate", "DeviceGray");
	   break;
   case 3 :
	   HPDF_Dict_AddName (icc, "Alternate", "DeviceRGB");
	   break;
   case 4 :
	   HPDF_Dict_AddName (icc, "Alternate", "DeviceCMYK");
	   break;
   default : /* unsupported */
       HPDF_RaiseError (&pdf->error, HPDF_INVALID_ICC_COMPONENT_NUM, 0);
       HPDF_Dict_Free(icc);
       return NULL;
   }

   for (;;) {
        HPDF_BYTE buf[HPDF_STREAM_BUF_SIZ];
        HPDF_UINT len = HPDF_STREAM_BUF_SIZ;
        ret = HPDF_Stream_Read (iccdata, buf, &len);

        if (ret != HPDF_OK) {
            if (ret == HPDF_STREAM_EOF) {
               if (len > 0) {
                    ret = HPDF_Stream_Write (icc->stream, buf, len);
                    if (ret != HPDF_OK) {
                        HPDF_Dict_Free(icc);
                        return NULL;
                    }
                }
                break;
            } else {
                HPDF_Dict_Free(icc);
                return NULL;
            }
        }

        if (HPDF_Stream_Write (icc->stream, buf, len) != HPDF_OK) {
            HPDF_Dict_Free(icc);
            return NULL;
        }
    }

    return icc;
}

HPDF_EXPORT(HPDF_Array)
HPDF_AddColorspaceFromProfile  (HPDF_Doc pdf,
                               HPDF_Dict icc)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Array iccentry;

    if (!HPDF_HasDoc (pdf))
        return NULL;

    iccentry = HPDF_Array_New(pdf->mmgr);
	if (!iccentry)
        return NULL;

    ret = HPDF_Array_AddName (iccentry, "ICCBased" );
    if (ret != HPDF_OK) {
		HPDF_Array_Free(iccentry);
        HPDF_CheckError (&pdf->error);
        return NULL;
    }

    ret = HPDF_Array_Add (iccentry, icc );
    if (ret != HPDF_OK) {
		HPDF_Array_Free(iccentry);
        return NULL;
    }
    return iccentry;
}

HPDF_EXPORT(HPDF_OutputIntent)
HPDF_LoadIccProfileFromFile  (HPDF_Doc pdf,
                           const char* icc_file_name,
						           int numcomponent)
{
    HPDF_Stream iccdata;
    HPDF_OutputIntent iccentry;

    HPDF_PTRACE ((" HPDF_LoadIccProfileFromFile\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

      /* create file stream */
    iccdata = HPDF_FileReader_New (pdf->mmgr, icc_file_name);

    if (HPDF_Stream_Validate (iccdata)) {
        iccentry = HPDF_ICC_LoadIccFromMem(pdf, pdf->mmgr, iccdata, pdf->xref, numcomponent);
    } else
        iccentry = NULL;

    /* destroy file stream */
    if (iccdata)
        HPDF_Stream_Free (iccdata);

    if (!iccentry)
        HPDF_CheckError (&pdf->error);

    return iccentry;
}

