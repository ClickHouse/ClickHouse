/*
 * << Haru Free PDF Library >> -- hpdf_outline.c
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
#include "hpdf_destination.h"
#include "hpdf.h"

#define HPDF_OUTLINE_CLOSED     0
#define HPDF_OUTLINE_OPENED     1


static HPDF_STATUS
AddChild  (HPDF_Outline  parent,
           HPDF_Outline  item);


static HPDF_STATUS
BeforeWrite  (HPDF_Dict obj);


static HPDF_UINT
CountChild (HPDF_Outline  outline);



/*----------------------------------------------------------------------------*/
/*----- HPDF_Outline ---------------------------------------------------------*/

HPDF_Outline
HPDF_OutlineRoot_New  (HPDF_MMgr   mmgr,
                       HPDF_Xref   xref)
{
    HPDF_Outline outline;
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Number open_flg;

    HPDF_PTRACE((" HPDF_OutlineRoot_New\n"));

    outline = HPDF_Dict_New (mmgr);
    if (!outline)
        return NULL;

    outline->before_write_fn = BeforeWrite;

    if (HPDF_Xref_Add (xref, outline) != HPDF_OK)
        return NULL;

    open_flg = HPDF_Number_New (mmgr, HPDF_OUTLINE_OPENED);
    if (!open_flg)
        return NULL;

    open_flg->header.obj_id |= HPDF_OTYPE_HIDDEN;

    ret += HPDF_Dict_Add (outline, "_OPENED", open_flg);
    ret += HPDF_Dict_AddName (outline, "Type", "Outlines");

    if (ret != HPDF_OK)
        return NULL;

    outline->header.obj_class |= HPDF_OSUBCLASS_OUTLINE;

    return outline;
}


HPDF_Outline
HPDF_Outline_New  (HPDF_MMgr          mmgr,
                   HPDF_Outline       parent,
                   const char   *title,
                   HPDF_Encoder       encoder,
                   HPDF_Xref          xref)
{
    HPDF_Outline outline;
    HPDF_String s;
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Number open_flg;

    HPDF_PTRACE((" HPDF_Outline_New\n"));

    if (!mmgr || !parent || !xref)
        return NULL;

    outline = HPDF_Dict_New (mmgr);
    if (!outline)
        return NULL;

    outline->before_write_fn = BeforeWrite;

    if (HPDF_Xref_Add (xref, outline) != HPDF_OK)
        return NULL;

    s = HPDF_String_New (mmgr, title, encoder);
    if (!s)
        return NULL;
    else
        ret += HPDF_Dict_Add (outline, "Title", s);

    open_flg = HPDF_Number_New (mmgr, HPDF_OUTLINE_OPENED);
    if (!open_flg)
        return NULL;

    open_flg->header.obj_id |= HPDF_OTYPE_HIDDEN;
    ret += HPDF_Dict_Add (outline, "_OPENED", open_flg);

    ret += HPDF_Dict_AddName (outline, "Type", "Outlines");
    ret += AddChild (parent, outline);

    if (ret != HPDF_OK)
        return NULL;

    outline->header.obj_class |= HPDF_OSUBCLASS_OUTLINE;

    return outline;
}



static HPDF_STATUS
AddChild  (HPDF_Outline  parent,
           HPDF_Outline  item)
{
    HPDF_Outline first = (HPDF_Outline)HPDF_Dict_GetItem (parent, "First",
                    HPDF_OCLASS_DICT);
    HPDF_Outline last = (HPDF_Outline)HPDF_Dict_GetItem (parent, "Last",
                    HPDF_OCLASS_DICT);
    HPDF_STATUS ret = 0;

    HPDF_PTRACE((" AddChild\n"));

    if (!first)
        ret += HPDF_Dict_Add (parent, "First", item);

    if (last) {
        ret += HPDF_Dict_Add (last, "Next", item);
        ret += HPDF_Dict_Add (item, "Prev", last);
    }

    ret += HPDF_Dict_Add (parent, "Last", item);
    ret += HPDF_Dict_Add (item, "Parent", parent);

    if (ret != HPDF_OK)
        return HPDF_Error_GetCode (item->error);

    return HPDF_OK;
}


HPDF_BOOL
HPDF_Outline_GetOpened  (HPDF_Outline  outline)
{
    HPDF_Number n = (HPDF_Number)HPDF_Dict_GetItem (outline, "_OPENED",
                        HPDF_OCLASS_NUMBER);

    HPDF_PTRACE((" HPDF_Outline_GetOpened\n"));

    if (!n)
        return HPDF_FALSE;

    return (HPDF_BOOL)n->value;
}


HPDF_Outline
HPDF_Outline_GetFirst  (HPDF_Outline outline)
{
    HPDF_PTRACE((" HPDF_Outline_GetFirst\n"));

    return (HPDF_Outline)HPDF_Dict_GetItem (outline, "First",
                    HPDF_OCLASS_DICT);
}


HPDF_Outline
HPDF_Outline_GetLast  (HPDF_Outline outline)
{
    HPDF_PTRACE((" HPDF_Outline_GetLast\n"));

    return (HPDF_Outline)HPDF_Dict_GetItem (outline, "Last", HPDF_OCLASS_DICT);
}


HPDF_Outline
HPDF_Outline_GetPrev  (HPDF_Outline outline)
{
    HPDF_PTRACE((" HPDF_Outline_GetPrev\n"));

    return (HPDF_Outline)HPDF_Dict_GetItem (outline, "Prev", HPDF_OCLASS_DICT);
}


HPDF_Outline
HPDF_Outline_GetNext  (HPDF_Outline outline)
{
    HPDF_PTRACE((" HPDF_Outline_GetNext\n"));

    return (HPDF_Outline)HPDF_Dict_GetItem (outline, "Next", HPDF_OCLASS_DICT);
}


HPDF_Outline
HPDF_Outline_GetParent  (HPDF_Outline outline)
{
    HPDF_PTRACE((" HPDF_Outline_GetParent\n"));

    return (HPDF_Outline)HPDF_Dict_GetItem (outline, "Parent",
                    HPDF_OCLASS_DICT);
}


static HPDF_STATUS
BeforeWrite  (HPDF_Dict obj)
{
    HPDF_Number n = (HPDF_Number)HPDF_Dict_GetItem (obj, "Count",
                HPDF_OCLASS_NUMBER);
    HPDF_UINT count = CountChild ((HPDF_Outline)obj);

    HPDF_PTRACE((" BeforeWrite\n"));

    if (count == 0 && n)
        return HPDF_Dict_RemoveElement (obj, "Count");

    if (!HPDF_Outline_GetOpened ((HPDF_Outline)obj))
        count = count * -1;

    if (n)
        n->value = count;
    else
        if (count)
            return HPDF_Dict_AddNumber (obj, "Count", count);

    return HPDF_OK;
}


static HPDF_UINT
CountChild (HPDF_Outline  outline)
{
    HPDF_Outline  child = HPDF_Outline_GetFirst (outline);
    HPDF_UINT count = 0;

    HPDF_PTRACE((" CountChild\n"));

    while (child) {
        count++;

        if (HPDF_Outline_GetOpened (child))
            count += CountChild (child);

        child = HPDF_Outline_GetNext (child);
    }

    return count;
}


HPDF_BOOL
HPDF_Outline_Validate (HPDF_Outline  outline)
{
    if (!outline)
        return HPDF_FALSE;

    HPDF_PTRACE((" HPDF_Outline_Validate\n"));

    if (outline->header.obj_class !=
                (HPDF_OSUBCLASS_OUTLINE | HPDF_OCLASS_DICT))
        return HPDF_FALSE;

    return HPDF_TRUE;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_Outline_SetDestination (HPDF_Outline      outline,
                             HPDF_Destination  dst)
{
    HPDF_PTRACE((" HPDF_Outline_SetDestination\n"));

    if (!HPDF_Outline_Validate (outline))
        return HPDF_INVALID_OUTLINE;

    if (!HPDF_Destination_Validate (dst))
        return HPDF_RaiseError (outline->error, HPDF_INVALID_DESTINATION, 0);

    if (dst == NULL)
        return HPDF_Dict_RemoveElement (outline, "Dest");

    if (HPDF_Dict_Add (outline, "Dest", dst) != HPDF_OK)
        return HPDF_CheckError (outline->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Outline_SetOpened  (HPDF_Outline  outline,
                         HPDF_BOOL     opened)
{
    HPDF_Number n;

    if (!HPDF_Outline_Validate (outline))
        return HPDF_INVALID_OUTLINE;

    n = (HPDF_Number)HPDF_Dict_GetItem (outline, "_OPENED",
                        HPDF_OCLASS_NUMBER);

    HPDF_PTRACE((" HPDF_Outline_SetOpened\n"));

    if (!n) {
        n = HPDF_Number_New (outline->mmgr, (HPDF_INT)opened);
        if (!n || HPDF_Dict_Add (outline, "_OPENED", n) != HPDF_OK)
            return HPDF_CheckError (outline->error);
    } else
        n->value = (HPDF_INT)opened;

    return HPDF_OK;
}
