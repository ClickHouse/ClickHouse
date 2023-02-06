/*
 * << Haru Free PDF Library >> -- hpdf_destination.c
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

const char * const HPDF_DESTINATION_TYPE_NAMES[] = {
        "XYZ",
        "Fit",
        "FitH",
        "FitV",
        "FitR",
        "FitB",
        "FitBH",
        "FitBV",
        NULL
};

/*----------------------------------------------------------------------------*/
/*----- HPDF_Destination -----------------------------------------------------*/

HPDF_Destination
HPDF_Destination_New  (HPDF_MMgr   mmgr,
                       HPDF_Page   target,
                       HPDF_Xref   xref)
{
    HPDF_Destination dst;

    HPDF_PTRACE((" HPDF_Destination_New\n"));

    if (!HPDF_Page_Validate (target)) {
        HPDF_SetError (mmgr->error, HPDF_INVALID_PAGE, 0);
        return NULL;
    }

    dst = HPDF_Array_New (mmgr);
    if (!dst)
        return NULL;

    dst->header.obj_class |= HPDF_OSUBCLASS_DESTINATION;

    if (HPDF_Xref_Add (xref, dst) != HPDF_OK)
        return NULL;

    /* first item of array must be target page */
    if (HPDF_Array_Add (dst, target) != HPDF_OK)
        return NULL;

    /* default type is HPDF_FIT */
    if (HPDF_Array_AddName (dst,
            HPDF_DESTINATION_TYPE_NAMES[(HPDF_INT)HPDF_FIT]) != HPDF_OK)
        return NULL;

    return dst;
}


HPDF_BOOL
HPDF_Destination_Validate (HPDF_Destination  dst)
{
    HPDF_Obj_Header *header = (HPDF_Obj_Header *)dst;
    HPDF_Page target;

    if (!dst || header->obj_class !=
                (HPDF_OCLASS_ARRAY | HPDF_OSUBCLASS_DESTINATION))
        return HPDF_FALSE;

    /* destination-types not defined. */
    if (dst->list->count < 2)
        return HPDF_FALSE;

    target = (HPDF_Page)HPDF_Array_GetItem (dst, 0, HPDF_OCLASS_DICT);
    if (!HPDF_Page_Validate (target)) {
	    HPDF_SetError (dst->error, HPDF_INVALID_PAGE, 0);
        return HPDF_FALSE;
    }

    return HPDF_TRUE;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Destination_SetXYZ  (HPDF_Destination  dst,
                          HPDF_REAL         left,
                          HPDF_REAL         top,
                          HPDF_REAL         zoom)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Page target;

    HPDF_PTRACE((" HPDF_Destination_SetXYZ\n"));

    if (!HPDF_Destination_Validate (dst))
        return HPDF_INVALID_DESTINATION;

    if (left < 0 || top < 0 || zoom < 0.08 || zoom > 32)
        return HPDF_RaiseError (dst->error, HPDF_INVALID_PARAMETER, 0);


    target = (HPDF_Page)HPDF_Array_GetItem (dst, 0, HPDF_OCLASS_DICT);

    if (dst->list->count > 1) {
        HPDF_Array_Clear (dst);
        ret += HPDF_Array_Add (dst, target);
    }

    ret += HPDF_Array_AddName (dst,
                HPDF_DESTINATION_TYPE_NAMES[(HPDF_INT)HPDF_XYZ]);
    ret += HPDF_Array_AddReal (dst, left);
    ret += HPDF_Array_AddReal (dst, top);
    ret += HPDF_Array_AddReal (dst, zoom);

    if (ret != HPDF_OK)
        return HPDF_CheckError (dst->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Destination_SetFit  (HPDF_Destination  dst)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Page target;

    HPDF_PTRACE((" HPDF_Destination_SetFit\n"));

    if (!HPDF_Destination_Validate (dst))
        return HPDF_INVALID_DESTINATION;

    target = (HPDF_Page)HPDF_Array_GetItem (dst, 0, HPDF_OCLASS_DICT);

    if (dst->list->count > 1) {
        HPDF_Array_Clear (dst);
        ret += HPDF_Array_Add (dst, target);
    }

    ret += HPDF_Array_AddName (dst,
                HPDF_DESTINATION_TYPE_NAMES[(HPDF_INT)HPDF_FIT]);

    if (ret != HPDF_OK)
        return HPDF_CheckError (dst->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Destination_SetFitH  (HPDF_Destination  dst,
                           HPDF_REAL         top)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Page target;

    HPDF_PTRACE((" HPDF_Destination_SetFitH\n"));

    if (!HPDF_Destination_Validate (dst))
        return HPDF_INVALID_DESTINATION;

    target = (HPDF_Page)HPDF_Array_GetItem (dst, 0, HPDF_OCLASS_DICT);

    if (dst->list->count > 1) {
        HPDF_Array_Clear (dst);
        ret += HPDF_Array_Add (dst, target);
    }

    ret += HPDF_Array_AddName (dst,
                HPDF_DESTINATION_TYPE_NAMES[(HPDF_INT)HPDF_FIT_H]);
    ret += HPDF_Array_AddReal (dst, top);

    if (ret != HPDF_OK)
        return HPDF_CheckError (dst->error);

    return HPDF_OK;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_Destination_SetFitV  (HPDF_Destination  dst,
                           HPDF_REAL         left)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Page target;

    HPDF_PTRACE((" HPDF_Destination_SetFitV\n"));

    if (!HPDF_Destination_Validate (dst))
        return HPDF_INVALID_DESTINATION;

    target = (HPDF_Page)HPDF_Array_GetItem (dst, 0, HPDF_OCLASS_DICT);

    if (dst->list->count > 1) {
        HPDF_Array_Clear (dst);
        ret += HPDF_Array_Add (dst, target);
    }

    ret += HPDF_Array_AddName (dst,
                HPDF_DESTINATION_TYPE_NAMES[(HPDF_INT)HPDF_FIT_V]);
    ret += HPDF_Array_AddReal (dst, left);

    if (ret != HPDF_OK)
        return HPDF_CheckError (dst->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Destination_SetFitR  (HPDF_Destination  dst,
                           HPDF_REAL         left,
                           HPDF_REAL         bottom,
                           HPDF_REAL         right,
                           HPDF_REAL         top)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Page target;

    HPDF_PTRACE((" HPDF_Destination_SetFitR\n"));

    if (!HPDF_Destination_Validate (dst))
        return HPDF_INVALID_DESTINATION;

    target = (HPDF_Page)HPDF_Array_GetItem (dst, 0, HPDF_OCLASS_DICT);

    if (dst->list->count > 1) {
        HPDF_Array_Clear (dst);
        ret += HPDF_Array_Add (dst, target);
    }

    ret += HPDF_Array_AddName (dst,
                HPDF_DESTINATION_TYPE_NAMES[(HPDF_INT)HPDF_FIT_R]);
    ret += HPDF_Array_AddReal (dst, left);
    ret += HPDF_Array_AddReal (dst, bottom);
    ret += HPDF_Array_AddReal (dst, right);
    ret += HPDF_Array_AddReal (dst, top);

    if (ret != HPDF_OK)
        return HPDF_CheckError (dst->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Destination_SetFitB  (HPDF_Destination  dst)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Page target;

    HPDF_PTRACE((" HPDF_Destination_SetFitB\n"));

    if (!HPDF_Destination_Validate (dst))
        return HPDF_INVALID_DESTINATION;

    target = (HPDF_Page)HPDF_Array_GetItem (dst, 0, HPDF_OCLASS_DICT);

    if (dst->list->count > 1) {
        HPDF_Array_Clear (dst);
        ret += HPDF_Array_Add (dst, target);
    }

    ret += HPDF_Array_AddName (dst,
                HPDF_DESTINATION_TYPE_NAMES[(HPDF_INT)HPDF_FIT_B]);

    if (ret != HPDF_OK)
        return HPDF_CheckError (dst->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Destination_SetFitBH  (HPDF_Destination  dst,
                            HPDF_REAL         top)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Page target;

    HPDF_PTRACE((" HPDF_Destination_SetFitBH\n"));

    if (!HPDF_Destination_Validate (dst))
        return HPDF_INVALID_DESTINATION;

    target = (HPDF_Page)HPDF_Array_GetItem (dst, 0, HPDF_OCLASS_DICT);

    if (dst->list->count > 1) {
        HPDF_Array_Clear (dst);
        ret += HPDF_Array_Add (dst, target);
    }

    ret += HPDF_Array_AddName (dst,
                HPDF_DESTINATION_TYPE_NAMES[(HPDF_INT)HPDF_FIT_BH]);
    ret += HPDF_Array_AddReal (dst, top);

    if (ret != HPDF_OK)
        return HPDF_CheckError (dst->error);

    return HPDF_OK;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_Destination_SetFitBV  (HPDF_Destination  dst,
                            HPDF_REAL         left)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Page target;

    HPDF_PTRACE((" HPDF_Destination_SetFitBV\n"));

    if (!HPDF_Destination_Validate (dst))
        return HPDF_INVALID_DESTINATION;

    target = (HPDF_Page)HPDF_Array_GetItem (dst, 0, HPDF_OCLASS_DICT);

    if (dst->list->count > 1) {
        HPDF_Array_Clear (dst);
        ret += HPDF_Array_Add (dst, target);
    }

    ret += HPDF_Array_AddName (dst,
                HPDF_DESTINATION_TYPE_NAMES[(HPDF_INT)HPDF_FIT_BV]);
    ret += HPDF_Array_AddReal (dst, left);

    if (ret != HPDF_OK)
        return HPDF_CheckError (dst->error);

    return HPDF_OK;

}

