/*
 * << Haru Free PDF Library >> -- hpdf_encryptdict.c
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

#include <time.h>
#include "hpdf_conf.h"
#include "hpdf_utils.h"
#include "hpdf_objects.h"
#include "hpdf_encryptdict.h"
#include "hpdf_info.h"
#ifndef HPDF_UNUSED
#define HPDF_UNUSED(a) ((void)(a))
#endif

HPDF_EncryptDict
HPDF_EncryptDict_New  (HPDF_MMgr  mmgr,
                       HPDF_Xref  xref)
{
    HPDF_Encrypt attr;
    HPDF_EncryptDict dict;

    HPDF_PTRACE((" HPDF_EncryptDict_New\n"));

    dict = HPDF_Dict_New (mmgr);
    if (!dict)
        return NULL;

    dict->header.obj_class |= HPDF_OSUBCLASS_ENCRYPT;
    dict->free_fn = HPDF_EncryptDict_OnFree;

    attr = HPDF_GetMem (dict->mmgr, sizeof(HPDF_Encrypt_Rec));
    if (!attr) {
        HPDF_Dict_Free (dict);
        return NULL;
    }

    dict->attr = attr;
    HPDF_Encrypt_Init (attr);

    if (HPDF_Xref_Add (xref, dict) != HPDF_OK)
        return NULL;

    return dict;
}


void
HPDF_EncryptDict_CreateID  (HPDF_EncryptDict  dict,
                            HPDF_Dict         info,
                            HPDF_Xref         xref)
{
    HPDF_MD5_CTX ctx;
    HPDF_Encrypt attr = (HPDF_Encrypt)dict->attr;

    /* use the result of 'time' function to get random value.
     * when debugging, 'time' value is ignored.
     */
#ifndef LIBHPDF_DEBUG
    time_t t = HPDF_TIME (NULL);
#endif /* LIBHPDF_DEBUG */

    HPDF_MD5Init (&ctx);
    HPDF_UNUSED (xref);
    HPDF_UNUSED (info);

#ifndef LIBHPDF_DEBUG
    HPDF_MD5Update(&ctx, (HPDF_BYTE *)&t, sizeof(t));

    /* create File Identifier from elements of Into dictionary. */
    if (info) {
        const char *s;
        HPDF_UINT len;

        /* Author */
        s = HPDF_Info_GetInfoAttr (info, HPDF_INFO_AUTHOR);
        if ((len = HPDF_StrLen (s, -1)) > 0)
            HPDF_MD5Update(&ctx, (const HPDF_BYTE *)s, len);

        /* Creator */
        s = HPDF_Info_GetInfoAttr (info, HPDF_INFO_CREATOR);
        if ((len = HPDF_StrLen (s, -1)) > 0)
            HPDF_MD5Update(&ctx, (const HPDF_BYTE *)s, len);

        /* Producer */
        s = HPDF_Info_GetInfoAttr (info, HPDF_INFO_PRODUCER);
        if ((len = HPDF_StrLen (s, -1)) > 0)
            HPDF_MD5Update(&ctx, (const HPDF_BYTE *)s, len);

        /* Title */
        s = HPDF_Info_GetInfoAttr (info, HPDF_INFO_TITLE);
        if ((len = HPDF_StrLen (s, -1)) > 0)
            HPDF_MD5Update(&ctx, (const HPDF_BYTE *)s, len);

        /* Subject */
        s = HPDF_Info_GetInfoAttr (info, HPDF_INFO_SUBJECT);
        if ((len = HPDF_StrLen (s, -1)) > 0)
            HPDF_MD5Update(&ctx, (const HPDF_BYTE *)s, len);

        /* Keywords */
        s = HPDF_Info_GetInfoAttr (info, HPDF_INFO_KEYWORDS);
        if ((len = HPDF_StrLen (s, -1)) > 0)
            HPDF_MD5Update(&ctx, (const HPDF_BYTE *)s, len);

        HPDF_MD5Update(&ctx, (const HPDF_BYTE *)&(xref->entries->count),
                sizeof(HPDF_UINT32));

    }
#endif
    HPDF_MD5Final(attr->encrypt_id, &ctx);
}


HPDF_STATUS
HPDF_EncryptDict_Prepare  (HPDF_EncryptDict  dict,
                           HPDF_Dict         info,
                           HPDF_Xref         xref)
{
    HPDF_STATUS ret;
    HPDF_Encrypt attr = (HPDF_Encrypt)dict->attr;
    HPDF_Binary user_key;
    HPDF_Binary owner_key;

    HPDF_PTRACE((" HPDF_EncryptDict_Prepare\n"));

    HPDF_EncryptDict_CreateID (dict, info, xref);
    HPDF_Encrypt_CreateOwnerKey (attr);
    HPDF_Encrypt_CreateEncryptionKey (attr);
    HPDF_Encrypt_CreateUserKey (attr);

    owner_key = HPDF_Binary_New (dict->mmgr, attr->owner_key, HPDF_PASSWD_LEN);
    if (!owner_key)
        return HPDF_Error_GetCode (dict->error);

    if ((ret = HPDF_Dict_Add (dict, "O", owner_key)) != HPDF_OK)
        return ret;

    user_key = HPDF_Binary_New (dict->mmgr, attr->user_key, HPDF_PASSWD_LEN);
    if (!user_key)
        return HPDF_Error_GetCode (dict->error);

    if ((ret = HPDF_Dict_Add (dict, "U", user_key)) != HPDF_OK)
        return ret;

    ret += HPDF_Dict_AddName (dict, "Filter", "Standard");

    if (attr->mode == HPDF_ENCRYPT_R2) {
        ret += HPDF_Dict_AddNumber (dict, "V", 1);
        ret += HPDF_Dict_AddNumber (dict, "R", 2);
    } else if (attr->mode == HPDF_ENCRYPT_R3) {
        ret += HPDF_Dict_AddNumber (dict, "V", 2);
        ret += HPDF_Dict_AddNumber (dict, "R", 3);
        ret += HPDF_Dict_AddNumber (dict, "Length", attr->key_len * 8);
    }

    ret += HPDF_Dict_AddNumber (dict, "P", attr->permission);

    if (ret != HPDF_OK)
        return HPDF_Error_GetCode (dict->error);

    return HPDF_OK;
}


void
HPDF_EncryptDict_OnFree  (HPDF_Dict  obj)
{
    HPDF_Encrypt attr = (HPDF_Encrypt)obj->attr;

    HPDF_PTRACE((" HPDF_EncryptDict_OnFree\n"));

    if (attr)
        HPDF_FreeMem (obj->mmgr, attr);
}


HPDF_STATUS
HPDF_EncryptDict_SetPassword  (HPDF_EncryptDict  dict,
                               const char   *owner_passwd,
                               const char   *user_passwd)
{
    HPDF_Encrypt attr = (HPDF_Encrypt)dict->attr;

    HPDF_PTRACE((" HPDF_EncryptDict_SetPassword\n"));

    if (HPDF_StrLen(owner_passwd, 2) == 0)
        return HPDF_SetError(dict->error, HPDF_ENCRYPT_INVALID_PASSWORD, 0);

    if (owner_passwd && user_passwd &&
            HPDF_StrCmp (owner_passwd, user_passwd) == 0)
        return HPDF_SetError(dict->error, HPDF_ENCRYPT_INVALID_PASSWORD, 0);

    HPDF_PadOrTrancatePasswd (owner_passwd, attr->owner_passwd);
    HPDF_PadOrTrancatePasswd (user_passwd, attr->user_passwd);

    return HPDF_OK;
}


HPDF_BOOL
HPDF_EncryptDict_Validate  (HPDF_EncryptDict  dict)
{
    HPDF_Obj_Header *header = (HPDF_Obj_Header *)dict;

    HPDF_PTRACE((" HPDF_EncryptDict_Validate\n"));

    if (!dict || !dict->attr)
        return HPDF_FALSE;

    if (header->obj_class != (HPDF_OCLASS_DICT | HPDF_OSUBCLASS_ENCRYPT))
        return HPDF_FALSE;

    return HPDF_TRUE;
}


HPDF_Encrypt
HPDF_EncryptDict_GetAttr (HPDF_EncryptDict  dict)
{
    HPDF_Obj_Header *header = (HPDF_Obj_Header *)dict;

    HPDF_PTRACE((" HPDF_EncryptDict_GetAttr\n"));

    if (dict && dict->attr &&
        (header->obj_class == (HPDF_OCLASS_DICT | HPDF_OSUBCLASS_ENCRYPT)))
        return (HPDF_Encrypt)dict->attr;

    return NULL;
}


