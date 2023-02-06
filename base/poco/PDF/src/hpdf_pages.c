/*
 * << Haru Free PDF Library >> -- hpdf_pages.c
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
#include "hpdf_annotation.h"
#include "hpdf_destination.h"
#include "hpdf_3dmeasure.h"
#include "hpdf_exdata.h"
#include "hpdf_u3d.h"

/*----------------------------------------------------------------------------*/

typedef struct _HPDF_PageSizeValue {
    HPDF_REAL   x;
    HPDF_REAL   y;
} HPDF_PageSizeValue;

static const HPDF_PageSizeValue HPDF_PREDEFINED_PAGE_SIZES[] = {
    {612, 792},     /* HPDF_PAGE_SIZE_LETTER */
    {612, 1008},    /* HPDF_PAGE_SIZE_LEGAL */
    {(HPDF_REAL)841.89, (HPDF_REAL)1190.551},    /* HPDF_PAGE_SIZE_A3 */
    {(HPDF_REAL)595.276, (HPDF_REAL)841.89},     /* HPDF_PAGE_SIZE_A4 */
    {(HPDF_REAL)419.528, (HPDF_REAL)595.276},     /* HPDF_PAGE_SIZE_A5 */
    {(HPDF_REAL)708.661, (HPDF_REAL)1000.63},     /* HPDF_PAGE_SIZE_B4 */
    {(HPDF_REAL)498.898, (HPDF_REAL)708.661},     /* HPDF_PAGE_SIZE_B5 */
    {522, 756},     /* HPDF_PAGE_SIZE_EXECUTIVE */
    {288, 432},     /* HPDF_PAGE_SIZE_US4x6 */
    {288, 576},     /* HPDF_PAGE_SIZE_US4x8 */
    {360, 504},     /* HPDF_PAGE_SIZE_US5x7 */
    {297, 684}      /* HPDF_PAGE_SIZE_COMM10 */
};


static const HPDF_RGBColor DEF_RGB_COLOR = {0, 0, 0};

static const HPDF_CMYKColor DEF_CMYK_COLOR = {0, 0, 0, 0};


static HPDF_STATUS
Pages_BeforeWrite  (HPDF_Dict    obj);


static HPDF_STATUS
Page_BeforeWrite  (HPDF_Dict    obj);


static void
Page_OnFree  (HPDF_Dict  obj);


static HPDF_STATUS
AddResource  (HPDF_Page  page);


static HPDF_STATUS
AddAnnotation  (HPDF_Page        page,
                HPDF_Annotation  annot);



static HPDF_UINT
GetPageCount  (HPDF_Dict    pages);

static const char * const HPDF_INHERITABLE_ENTRIES[5] = {
                        "Resources",
                        "MediaBox",
                        "CropBox",
                        "Rotate",
                        NULL
                        };


/*----------------------------------------------------------------------------*/
/*----- HPDF_Pages -----------------------------------------------------------*/

HPDF_Pages
HPDF_Pages_New  (HPDF_MMgr   mmgr,
                 HPDF_Pages  parent,
                 HPDF_Xref   xref)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Pages pages;


    HPDF_PTRACE((" HPDF_Pages_New\n"));

    pages = HPDF_Dict_New (mmgr);
    if (!pages)
        return NULL;

    pages->header.obj_class |= HPDF_OSUBCLASS_PAGES;
    pages->before_write_fn = Pages_BeforeWrite;

    if (HPDF_Xref_Add (xref, pages) != HPDF_OK)
        return NULL;

    /* add requiered elements */
    ret += HPDF_Dict_AddName (pages, "Type", "Pages");
    ret += HPDF_Dict_Add (pages, "Kids", HPDF_Array_New (pages->mmgr));
    ret += HPDF_Dict_Add (pages, "Count", HPDF_Number_New (pages->mmgr, 0));

    if (ret == HPDF_OK && parent)
        ret += HPDF_Pages_AddKids (parent, pages);

    if (ret != HPDF_OK)
        return NULL;

    return pages;
}


HPDF_STATUS
HPDF_Pages_AddKids  (HPDF_Pages  parent,
                     HPDF_Dict   kid)
{
    HPDF_Array kids;
    HPDF_STATUS ret;

    HPDF_PTRACE((" HPDF_Pages_AddKids\n"));

    if (HPDF_Dict_GetItem (kid, "Parent", HPDF_OCLASS_DICT))
        return HPDF_SetError (parent->error, HPDF_PAGE_CANNOT_SET_PARENT, 0);

    if ((ret = HPDF_Dict_Add (kid, "Parent", parent)) != HPDF_OK)
        return ret;

    kids = (HPDF_Array )HPDF_Dict_GetItem (parent, "Kids", HPDF_OCLASS_ARRAY);
    if (!kids)
        return HPDF_SetError (parent->error, HPDF_PAGES_MISSING_KIDS_ENTRY, 0);

    if (kid->header.obj_class == (HPDF_OCLASS_DICT | HPDF_OSUBCLASS_PAGE)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)kid->attr;

        attr->parent = parent;
    }

    return HPDF_Array_Add (kids, kid);
}


HPDF_STATUS
HPDF_Page_InsertBefore  (HPDF_Page   page,
                         HPDF_Page   target)
{
    HPDF_Page parent;
    HPDF_Array kids;
    HPDF_STATUS ret;
    HPDF_PageAttr attr;

    HPDF_PTRACE((" HPDF_Page_InsertBefore\n"));

    if (!target)
        return HPDF_INVALID_PARAMETER;

    attr = (HPDF_PageAttr )target->attr;
    parent = attr->parent;

    if (!parent)
        return HPDF_PAGE_CANNOT_SET_PARENT;

    if (HPDF_Dict_GetItem (page, "Parent", HPDF_OCLASS_DICT))
        return HPDF_SetError (parent->error, HPDF_PAGE_CANNOT_SET_PARENT, 0);

    if ((ret = HPDF_Dict_Add (page, "Parent", parent)) != HPDF_OK)
        return ret;

    kids = (HPDF_Array )HPDF_Dict_GetItem (parent, "Kids", HPDF_OCLASS_ARRAY);
    if (!kids)
        return HPDF_SetError (parent->error, HPDF_PAGES_MISSING_KIDS_ENTRY, 0);

    attr = (HPDF_PageAttr)page->attr;
    attr->parent = parent;

    return HPDF_Array_Insert (kids, target, page);
}


HPDF_STATUS
Pages_BeforeWrite  (HPDF_Dict    obj)
{
    HPDF_Array kids = (HPDF_Array )HPDF_Dict_GetItem (obj, "Kids",
                    HPDF_OCLASS_ARRAY);
    HPDF_Number count = (HPDF_Number)HPDF_Dict_GetItem (obj, "Count",
                    HPDF_OCLASS_NUMBER);
    HPDF_STATUS ret;

    HPDF_PTRACE((" HPDF_Pages_BeforeWrite\n"));

    if (!kids)
        return HPDF_SetError (obj->error, HPDF_PAGES_MISSING_KIDS_ENTRY, 0);

    if (count)
        count->value = GetPageCount (obj);
    else {
        count = HPDF_Number_New (obj->mmgr, GetPageCount (obj));
        if (!count)
            return HPDF_Error_GetCode (obj->error);

        if ((ret = HPDF_Dict_Add (obj, "Count", count)) != HPDF_OK)
            return ret;
    }

    return HPDF_OK;
}


static HPDF_STATUS
Page_BeforeWrite  (HPDF_Dict    obj)
{
    HPDF_STATUS ret;
    HPDF_Page page = (HPDF_Page)obj;
    HPDF_PageAttr attr = (HPDF_PageAttr)obj->attr;

    HPDF_PTRACE((" HPDF_Page_BeforeWrite\n"));

    if (attr->gmode == HPDF_GMODE_PATH_OBJECT) {
        HPDF_PTRACE((" HPDF_Page_BeforeWrite warning path object is not"
                    " end\n"));

        if ((ret = HPDF_Page_EndPath (page)) != HPDF_OK)
           return ret;
    }

    if (attr->gmode == HPDF_GMODE_TEXT_OBJECT) {
        HPDF_PTRACE((" HPDF_Page_BeforeWrite warning text block is not end\n"));

        if ((ret = HPDF_Page_EndText (page)) != HPDF_OK)
            return ret;
    }

    if (attr->gstate)
        while (attr->gstate->prev) {
            if ((ret = HPDF_Page_GRestore (page)) != HPDF_OK)
                return ret;
        }

    return HPDF_OK;
}


static HPDF_UINT
GetPageCount  (HPDF_Dict    pages)
{
    HPDF_UINT i;
    HPDF_UINT count = 0;
    HPDF_Array kids = (HPDF_Array)HPDF_Dict_GetItem (pages, "Kids",
            HPDF_OCLASS_ARRAY);

    HPDF_PTRACE((" GetPageCount\n"));

    if (!kids)
        return 0;

    for (i = 0; i < kids->list->count; i++) {
        void *obj = HPDF_Array_GetItem (kids, i, HPDF_OCLASS_DICT);
        HPDF_Obj_Header *header = (HPDF_Obj_Header *)obj;

        if (header->obj_class == (HPDF_OCLASS_DICT | HPDF_OSUBCLASS_PAGES))
            count += GetPageCount ((HPDF_Dict)obj);
        else if (header->obj_class == (HPDF_OCLASS_DICT | HPDF_OSUBCLASS_PAGE))
            count += 1;
    }

    return count;
}


HPDF_BOOL
HPDF_Pages_Validate  (HPDF_Pages  pages)
{
    HPDF_Obj_Header *header = (HPDF_Obj_Header *)pages;

    HPDF_PTRACE((" HPDF_Pages_Validate\n"));

    if (!pages || header->obj_class != (HPDF_OCLASS_DICT |
                HPDF_OSUBCLASS_PAGES))
        return HPDF_FALSE;

    return HPDF_TRUE;
}


/*----------------------------------------------------------------------------*/
/*----- HPDF_Page ------------------------------------------------------------*/


HPDF_Page
HPDF_Page_New  (HPDF_MMgr   mmgr,
                HPDF_Xref   xref)
{
    HPDF_STATUS ret;
    HPDF_PageAttr attr;
    HPDF_Page page;

    HPDF_PTRACE((" HPDF_Page_New\n"));

    page = HPDF_Dict_New (mmgr);
    if (!page)
        return NULL;

    page->header.obj_class |= HPDF_OSUBCLASS_PAGE;
    page->free_fn = Page_OnFree;
    page->before_write_fn = Page_BeforeWrite;

    attr = HPDF_GetMem (page->mmgr, sizeof(HPDF_PageAttr_Rec));
    if (!attr) {
        HPDF_Dict_Free (page);
        return NULL;
    }

    page->attr = attr;
    HPDF_MemSet (attr, 0, sizeof(HPDF_PageAttr_Rec));
    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;
    attr->cur_pos = HPDF_ToPoint (0, 0);
    attr->text_pos = HPDF_ToPoint (0, 0);

    ret = HPDF_Xref_Add (xref, page);
    if (ret != HPDF_OK)
        return NULL;

    attr->gstate = HPDF_GState_New (page->mmgr, NULL);
    attr->contents = HPDF_DictStream_New (page->mmgr, xref);

    if (!attr->gstate || !attr->contents)
        return NULL;

    attr->stream = attr->contents->stream;
    attr->xref = xref;

    /* add requiered elements */
    ret += HPDF_Dict_AddName (page, "Type", "Page");
    ret += HPDF_Dict_Add (page, "MediaBox", HPDF_Box_Array_New (page->mmgr,
                HPDF_ToBox (0, 0, (HPDF_INT16)(HPDF_DEF_PAGE_WIDTH), (HPDF_INT16)(HPDF_DEF_PAGE_HEIGHT))));
    ret += HPDF_Dict_Add (page, "Contents", attr->contents);

    ret += AddResource (page);

    if (ret != HPDF_OK)
        return NULL;

    return page;
}




static void
Page_OnFree  (HPDF_Dict  obj)
{
    HPDF_PageAttr attr = (HPDF_PageAttr)obj->attr;

    HPDF_PTRACE((" HPDF_Page_OnFree\n"));

    if (attr) {
        if (attr->gstate)
            HPDF_GState_Free (obj->mmgr, attr->gstate);

        HPDF_FreeMem (obj->mmgr, attr);
    }
}


HPDF_STATUS
HPDF_Page_CheckState  (HPDF_Page  page,
                       HPDF_UINT  mode)
{
    if (!page)
        return HPDF_INVALID_OBJECT;

    if (page->header.obj_class != (HPDF_OSUBCLASS_PAGE | HPDF_OCLASS_DICT))
        return HPDF_INVALID_PAGE;

    if (!(((HPDF_PageAttr)page->attr)->gmode & mode))
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_GMODE, 0);

    return HPDF_OK;
}


void*
HPDF_Page_GetInheritableItem  (HPDF_Page          page,
                               const char   *key,
                               HPDF_UINT16        obj_class)
{
    HPDF_BOOL chk = HPDF_FALSE;
    HPDF_INT i = 0;
    void * obj;

    HPDF_PTRACE((" HPDF_Page_GetInheritableItem\n"));

    /* check whether the specified key is valid */
    while (HPDF_INHERITABLE_ENTRIES[i]) {
        if (HPDF_StrCmp (key, HPDF_INHERITABLE_ENTRIES[i]) == 0) {
            chk = HPDF_TRUE;
            break;
        }
        i++;
    }

    /* the key is not inheritable */
    if (chk != HPDF_TRUE) {
        HPDF_SetError (page->error, HPDF_INVALID_PARAMETER, 0);
        return NULL;
    }

    obj = HPDF_Dict_GetItem (page, key, obj_class);

    /* if resources of the object is NULL, search resources of parent
     * pages recursivly
     */
    if (!obj) {
        HPDF_Pages pages = HPDF_Dict_GetItem (page, "Parent", HPDF_OCLASS_DICT);
        while (pages) {
            obj = HPDF_Dict_GetItem (page, key, obj_class);

            if (obj)
                break;

            pages = HPDF_Dict_GetItem (pages, "Parent", HPDF_OCLASS_DICT);
        }
    }

    return obj;
}


HPDF_STATUS
AddResource  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Dict resource;
    HPDF_Array procset;

    HPDF_PTRACE((" HPDF_Page_AddResource\n"));

    resource = HPDF_Dict_New (page->mmgr);
    if (!resource)
        return HPDF_Error_GetCode (page->error);

    /* althoth ProcSet-entry is obsolete, add it to resouce for
     * compatibility
     */

    ret += HPDF_Dict_Add (page, "Resources", resource);

    procset = HPDF_Array_New (page->mmgr);
    if (!procset)
        return HPDF_Error_GetCode (page->error);

    if (HPDF_Dict_Add (resource, "ProcSet", procset) != HPDF_OK)
        return HPDF_Error_GetCode (resource->error);

    ret += HPDF_Array_Add (procset, HPDF_Name_New (page->mmgr, "PDF"));
    ret += HPDF_Array_Add (procset, HPDF_Name_New (page->mmgr, "Text"));
    ret += HPDF_Array_Add (procset, HPDF_Name_New (page->mmgr, "ImageB"));
    ret += HPDF_Array_Add (procset, HPDF_Name_New (page->mmgr, "ImageC"));
    ret += HPDF_Array_Add (procset, HPDF_Name_New (page->mmgr, "ImageI"));

    if (ret != HPDF_OK)
       return HPDF_Error_GetCode (procset->error);

    return HPDF_OK;
}


const char*
HPDF_Page_GetLocalFontName  (HPDF_Page  page,
                             HPDF_Font  font)
{
    HPDF_PageAttr attr = (HPDF_PageAttr )page->attr;
    const char *key;

    HPDF_PTRACE((" HPDF_Page_GetLocalFontName\n"));

    /* whether check font-resource exists.  when it does not exists,
     * create font-resource
     * 2006.07.21 Fixed a problem which may cause a memory leak.
     */
    if (!attr->fonts) {
        HPDF_Dict resources;
        HPDF_Dict fonts;

        resources = HPDF_Page_GetInheritableItem (page, "Resources",
                        HPDF_OCLASS_DICT);
        if (!resources)
            return NULL;

        fonts = HPDF_Dict_New (page->mmgr);
        if (!fonts)
            return NULL;

        if (HPDF_Dict_Add (resources, "Font", fonts) != HPDF_OK)
            return NULL;

        attr->fonts = fonts;
    }

    /* search font-object from font-resource */
    key = HPDF_Dict_GetKeyByObj (attr->fonts, font);
    if (!key) {
        /* if the font is not resisterd in font-resource, register font to
         * font-resource.
         */
        char fontName[HPDF_LIMIT_MAX_NAME_LEN + 1];
        char *ptr;
        char *end_ptr = fontName + HPDF_LIMIT_MAX_NAME_LEN;

        ptr = (char *)HPDF_StrCpy (fontName, "F", end_ptr);
        HPDF_IToA (ptr, attr->fonts->list->count + 1, end_ptr);

        if (HPDF_Dict_Add (attr->fonts, fontName, font) != HPDF_OK)
            return NULL;

        key = HPDF_Dict_GetKeyByObj (attr->fonts, font);
    }

    return key;
}


HPDF_Box
HPDF_Page_GetMediaBox  (HPDF_Page   page)
{
    HPDF_Box media_box = {0, 0, 0, 0};

    HPDF_PTRACE((" HPDF_Page_GetMediaBox\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_Array array = HPDF_Page_GetInheritableItem (page, "MediaBox",
                        HPDF_OCLASS_ARRAY);

        if (array) {
            HPDF_Real r;

            r = HPDF_Array_GetItem (array, 0, HPDF_OCLASS_REAL);
            if (r)
                media_box.left = r->value;

            r = HPDF_Array_GetItem (array, 1, HPDF_OCLASS_REAL);
            if (r)
                media_box.bottom = r->value;

            r = HPDF_Array_GetItem (array, 2, HPDF_OCLASS_REAL);
            if (r)
                media_box.right = r->value;

            r = HPDF_Array_GetItem (array, 3, HPDF_OCLASS_REAL);
            if (r)
                media_box.top = r->value;

            HPDF_CheckError (page->error);
        } else HPDF_RaiseError (page->error, HPDF_PAGE_CANNOT_FIND_OBJECT, 0);
    }

    return media_box;
}


const char*
HPDF_Page_GetXObjectName  (HPDF_Page     page,
                           HPDF_XObject  xobj)
{
    HPDF_PageAttr attr = (HPDF_PageAttr )page->attr;
    const char *key;

    HPDF_PTRACE((" HPDF_Page_GetXObjectName\n"));

    if (!attr->xobjects) {
        HPDF_Dict resources;
        HPDF_Dict xobjects;

        resources = HPDF_Page_GetInheritableItem (page, "Resources",
                        HPDF_OCLASS_DICT);
        if (!resources)
            return NULL;

        xobjects = HPDF_Dict_New (page->mmgr);
        if (!xobjects)
            return NULL;

        if (HPDF_Dict_Add (resources, "XObject", xobjects) != HPDF_OK)
            return NULL;

        attr->xobjects = xobjects;
    }

    /* search xobject-object from xobject-resource */
    key = HPDF_Dict_GetKeyByObj (attr->xobjects, xobj);
    if (!key) {
        /* if the xobject is not resisterd in xobject-resource, register
         * xobject to xobject-resource.
         */
        char xobj_name[HPDF_LIMIT_MAX_NAME_LEN + 1];
        char *ptr;
        char *end_ptr = xobj_name + HPDF_LIMIT_MAX_NAME_LEN;

        ptr = (char *)HPDF_StrCpy (xobj_name, "X", end_ptr);
        HPDF_IToA (ptr, attr->xobjects->list->count + 1, end_ptr);

        if (HPDF_Dict_Add (attr->xobjects, xobj_name, xobj) != HPDF_OK)
            return NULL;

        key = HPDF_Dict_GetKeyByObj (attr->xobjects, xobj);
    }

    return key;
}


const char*
HPDF_Page_GetExtGStateName  (HPDF_Page       page,
                             HPDF_ExtGState  state)
{
    HPDF_PageAttr attr = (HPDF_PageAttr )page->attr;
    const char *key;

    HPDF_PTRACE((" HPDF_Page_GetExtGStateName\n"));

    if (!attr->ext_gstates) {
        HPDF_Dict resources;
        HPDF_Dict ext_gstates;

        resources = HPDF_Page_GetInheritableItem (page, "Resources",
                        HPDF_OCLASS_DICT);
        if (!resources)
            return NULL;

        ext_gstates = HPDF_Dict_New (page->mmgr);
        if (!ext_gstates)
            return NULL;

        if (HPDF_Dict_Add (resources, "ExtGState", ext_gstates) != HPDF_OK)
            return NULL;

        attr->ext_gstates = ext_gstates;
    }

    /* search ext_gstate-object from ext_gstate-resource */
    key = HPDF_Dict_GetKeyByObj (attr->ext_gstates, state);
    if (!key) {
        /* if the ext-gstate is not resisterd in ext-gstate resource, register
         *  to ext-gstate resource.
         */
        char ext_gstate_name[HPDF_LIMIT_MAX_NAME_LEN + 1];
        char *ptr;
        char *end_ptr = ext_gstate_name + HPDF_LIMIT_MAX_NAME_LEN;

        ptr = (char *)HPDF_StrCpy (ext_gstate_name, "E", end_ptr);
        HPDF_IToA (ptr, attr->ext_gstates->list->count + 1, end_ptr);

        if (HPDF_Dict_Add (attr->ext_gstates, ext_gstate_name, state) != HPDF_OK)
            return NULL;

        key = HPDF_Dict_GetKeyByObj (attr->ext_gstates, state);
    }

    return key;
}


static HPDF_STATUS
AddAnnotation  (HPDF_Page        page,
                HPDF_Annotation  annot)
{
    HPDF_Array array;
    HPDF_STATUS ret = HPDF_OK;

    HPDF_PTRACE((" HPDF_Pages\n"));

    /* find "Annots" entry */
    array = HPDF_Dict_GetItem (page, "Annots", HPDF_OCLASS_ARRAY);

    if (!array) {
        array = HPDF_Array_New (page->mmgr);
        if (!array)
            return HPDF_Error_GetCode (page->error);

        ret = HPDF_Dict_Add (page, "Annots", array);
        if (ret != HPDF_OK)
            return ret;
    }
    
    if ((ret = HPDF_Array_Add (array, annot)) != HPDF_OK)
       return ret;

    /* Add Parent to the annotation  */
    ret = HPDF_Dict_Add( annot, "P", page);

    return ret;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_TextWidth  (HPDF_Page        page,
                      const char      *text)
{
    HPDF_PageAttr attr;
    HPDF_TextWidth tw;
    HPDF_REAL ret = 0;
    HPDF_UINT len = HPDF_StrLen(text, HPDF_LIMIT_MAX_STRING_LEN + 1);

    HPDF_PTRACE((" HPDF_Page_TextWidth\n"));

    if (!HPDF_Page_Validate (page) || len == 0)
        return 0;

    attr = (HPDF_PageAttr )page->attr;

    /* no font exists */
    if (!attr->gstate->font) {
        HPDF_RaiseError (page->error, HPDF_PAGE_FONT_NOT_FOUND, 0);
        return 0;
    }

    tw = HPDF_Font_TextWidth (attr->gstate->font, (HPDF_BYTE *)text, len);

    ret += attr->gstate->word_space * tw.numspace;
    ret += tw.width * attr->gstate->font_size  / 1000;
    ret += attr->gstate->char_space * tw.numchars;

    HPDF_CheckError (page->error);

    return ret;
}


HPDF_EXPORT(HPDF_UINT)
HPDF_Page_MeasureText  (HPDF_Page          page,
                        const char        *text,
                        HPDF_REAL          width,
                        HPDF_BOOL          wordwrap,
                        HPDF_REAL         *real_width)
{
    HPDF_PageAttr attr;
    HPDF_UINT len = HPDF_StrLen(text, HPDF_LIMIT_MAX_STRING_LEN + 1);
    HPDF_UINT ret;

    if (!HPDF_Page_Validate (page) || len == 0)
        return 0;

    attr = (HPDF_PageAttr )page->attr;

    HPDF_PTRACE((" HPDF_Page_MeasureText\n"));

    /* no font exists */
    if (!attr->gstate->font) {
        HPDF_RaiseError (page->error, HPDF_PAGE_FONT_NOT_FOUND, 0);
        return 0;
    }

    ret = HPDF_Font_MeasureText (attr->gstate->font, (HPDF_BYTE *)text, len, width,
        attr->gstate->font_size, attr->gstate->char_space,
        attr->gstate->word_space, wordwrap, real_width);

    HPDF_CheckError (page->error);

    return ret;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetWidth  (HPDF_Page    page)
{
    return HPDF_Page_GetMediaBox (page).right;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetHeight  (HPDF_Page    page)
{
    return HPDF_Page_GetMediaBox (page).top;
}


HPDF_EXPORT(HPDF_Font)
HPDF_Page_GetCurrentFont  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetFontName\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->font;
    } else
        return NULL;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetCurrentFontSize  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetCurrentFontSize\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return (attr->gstate->font) ? attr->gstate->font_size : 0;
    } else
        return 0;
}


HPDF_EXPORT(HPDF_TransMatrix)
HPDF_Page_GetTransMatrix  (HPDF_Page   page)
{
    HPDF_TransMatrix DEF_MATRIX = {1, 0, 0, 1, 0, 0};

    HPDF_PTRACE((" HPDF_Page_GetTransMatrix\n"));
    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->trans_matrix;
    } else
        return DEF_MATRIX;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetLineWidth  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetLineWidth\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->line_width;
    } else
        return HPDF_DEF_LINEWIDTH;
}


HPDF_EXPORT(HPDF_LineCap)
HPDF_Page_GetLineCap  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetLineCap\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->line_cap;
    } else
        return HPDF_DEF_LINECAP;
}


HPDF_EXPORT(HPDF_LineJoin)
HPDF_Page_GetLineJoin  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetLineJoin\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->line_join;
    } else
        return HPDF_DEF_LINEJOIN;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetMiterLimit  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetMiterLimit\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->miter_limit;
    } else
        return HPDF_DEF_MITERLIMIT;
}


HPDF_EXPORT(HPDF_DashMode)
HPDF_Page_GetDash  (HPDF_Page   page)
{
    HPDF_DashMode mode = {{0, 0, 0, 0, 0, 0, 0, 0}, 0, 0};

    HPDF_PTRACE((" HPDF_Page_GetDash\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        mode = attr->gstate->dash_mode;
    }

    return mode;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetFlat  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetFlat\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->flatness;
    } else
        return HPDF_DEF_FLATNESS;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetWordSpace  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetWordSpace\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->word_space;
    } else
        return HPDF_DEF_WORDSPACE;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetCharSpace  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetCharSpace\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->char_space;
    } else
        return HPDF_DEF_CHARSPACE;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetHorizontalScalling  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetHorizontalScalling\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->h_scalling;
    } else
        return HPDF_DEF_HSCALING;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetTextLeading  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetTextLeading\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->text_leading;
    } else
        return HPDF_DEF_LEADING;
}


HPDF_EXPORT(HPDF_TextRenderingMode)
HPDF_Page_GetTextRenderingMode  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GettextRenderingMode\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->rendering_mode;
    } else
        return HPDF_DEF_RENDERING_MODE;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetTextRaise  (HPDF_Page   page)
{
    return HPDF_Page_GetTextRise (page);
}

HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetTextRise  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetTextRise\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->text_rise;
    } else
        return HPDF_DEF_RISE;
}


HPDF_EXPORT(HPDF_RGBColor)
HPDF_Page_GetRGBFill  (HPDF_Page   page)
{
    HPDF_RGBColor DEF_RGB_COLOR = {0, 0, 0};

    HPDF_PTRACE((" HPDF_Page_GetRGBFill\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        if (attr->gstate->cs_fill == HPDF_CS_DEVICE_RGB)
            return attr->gstate->rgb_fill;
    }

    return DEF_RGB_COLOR;
}


HPDF_EXPORT(HPDF_RGBColor)
HPDF_Page_GetRGBStroke  (HPDF_Page   page)
{
    HPDF_RGBColor DEF_RGB_COLOR = {0, 0, 0};

    HPDF_PTRACE((" HPDF_Page_GetRGBStroke\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        if (attr->gstate->cs_stroke == HPDF_CS_DEVICE_RGB)
            return attr->gstate->rgb_stroke;
    }

    return DEF_RGB_COLOR;
}

HPDF_EXPORT(HPDF_CMYKColor)
HPDF_Page_GetCMYKFill  (HPDF_Page   page)
{
    HPDF_CMYKColor DEF_CMYK_COLOR = {0, 0, 0, 0};

    HPDF_PTRACE((" HPDF_Page_GetCMYKFill\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        if (attr->gstate->cs_fill == HPDF_CS_DEVICE_CMYK)
            return attr->gstate->cmyk_fill;
    }

    return DEF_CMYK_COLOR;
}


HPDF_EXPORT(HPDF_CMYKColor)
HPDF_Page_GetCMYKStroke  (HPDF_Page   page)
{
    HPDF_CMYKColor DEF_CMYK_COLOR = {0, 0, 0, 0};

    HPDF_PTRACE((" HPDF_Page_GetCMYKStroke\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        if (attr->gstate->cs_stroke == HPDF_CS_DEVICE_CMYK)
            return attr->gstate->cmyk_stroke;
    }

    return DEF_CMYK_COLOR;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetGrayFill  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetGrayFill\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        if (attr->gstate->cs_fill == HPDF_CS_DEVICE_GRAY)
            return attr->gstate->gray_fill;
    }

    return 0;
}


HPDF_EXPORT(HPDF_REAL)
HPDF_Page_GetGrayStroke  (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetGrayStroke\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        if (attr->gstate->cs_stroke == HPDF_CS_DEVICE_GRAY)
            return attr->gstate->gray_stroke;
    }

    return 0;
}


HPDF_EXPORT(HPDF_ColorSpace)
HPDF_Page_GetStrokingColorSpace (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetStrokingColorSpace\n"));

    if (HPDF_Page_Validate (page))
        return ((HPDF_PageAttr)page->attr)->gstate->cs_stroke;

    return HPDF_CS_EOF;
}


HPDF_EXPORT(HPDF_ColorSpace)
HPDF_Page_GetFillingColorSpace (HPDF_Page   page)
{
    HPDF_PTRACE((" HPDF_Page_GetFillingColorSpace\n"));

    if (HPDF_Page_Validate (page))
        return ((HPDF_PageAttr)page->attr)->gstate->cs_fill;

    return HPDF_CS_EOF;
}


HPDF_EXPORT(HPDF_TransMatrix)
HPDF_Page_GetTextMatrix  (HPDF_Page   page)
{
    HPDF_TransMatrix DEF_MATRIX = {1, 0, 0, 1, 0, 0};

    HPDF_PTRACE((" HPDF_Page_GetTextMatrix\n"));
    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->text_matrix;
    } else
        return DEF_MATRIX;
}


HPDF_EXPORT(HPDF_UINT)
HPDF_Page_GetGStateDepth  (HPDF_Page    page)
{
    HPDF_PTRACE((" HPDF_Page_GetGStateDepth\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        return attr->gstate->depth;
    } else
        return 0;
}


HPDF_EXPORT(HPDF_UINT16)
HPDF_Page_GetGMode  (HPDF_Page   page)
{
    if (HPDF_Page_Validate (page))
        return ((HPDF_PageAttr)page->attr)->gmode;

    return 0;
}

HPDF_EXPORT(HPDF_Point)
HPDF_Page_GetCurrentPos (HPDF_Page  page)
{
    HPDF_Point pos = {0, 0};

    HPDF_PTRACE((" HPDF_Page_GetCurrentPos\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        if (attr->gmode & HPDF_GMODE_PATH_OBJECT)
            pos = attr->cur_pos;
    }

    return pos;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_GetCurrentPos2 (HPDF_Page   page,
                          HPDF_Point *pos)
{
    HPDF_PageAttr attr;
    HPDF_PTRACE((" HPDF_Page_GetCurrentPos2\n"));

    pos->x = 0;
    pos->y = 0;
    if (!HPDF_Page_Validate (page))
        return HPDF_INVALID_PAGE;

    attr = (HPDF_PageAttr)page->attr;

    if (attr->gmode & HPDF_GMODE_PATH_OBJECT)
        *pos = attr->cur_pos;

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_Point)
HPDF_Page_GetCurrentTextPos (HPDF_Page  page)
{
    HPDF_Point pos = {0, 0};

    HPDF_PTRACE((" HPDF_Page_GetCurrentTextPos\n"));

    if (HPDF_Page_Validate (page)) {
        HPDF_PageAttr attr = (HPDF_PageAttr)page->attr;

        if (attr->gmode & HPDF_GMODE_TEXT_OBJECT)
            pos = attr->text_pos;
    }
    
    return pos;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_GetCurrentTextPos2 (HPDF_Page   page,
                              HPDF_Point *pos)
{
    HPDF_PageAttr attr;

    HPDF_PTRACE((" HPDF_Page_GetCurrentTextPos2\n"));

    pos->x = 0;
    pos->y = 0;
    if (!HPDF_Page_Validate (page))
        return HPDF_INVALID_PAGE;

    attr = (HPDF_PageAttr)page->attr;

    if (attr->gmode & HPDF_GMODE_TEXT_OBJECT)
        *pos = attr->text_pos;

    return HPDF_OK;
}


HPDF_STATUS
HPDF_Page_SetBoxValue (HPDF_Page          page,
                       const char   *name,
                       HPDF_UINT          index,
                       HPDF_REAL          value)
{
    HPDF_Real r;
    HPDF_Array array;

    HPDF_PTRACE((" HPDF_Page_SetBoxValue\n"));

    if (!HPDF_Page_Validate (page))
        return HPDF_INVALID_PAGE;

    array = HPDF_Page_GetInheritableItem (page, name, HPDF_OCLASS_ARRAY);
    if (!array)
        return HPDF_SetError (page->error, HPDF_PAGE_CANNOT_FIND_OBJECT, 0);

    r = HPDF_Array_GetItem (array, index, HPDF_OCLASS_REAL);
    if (!r)
        return HPDF_SetError (page->error, HPDF_PAGE_INVALID_INDEX, 0);

    r->value = value;

    return HPDF_OK;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetRotate (HPDF_Page      page,
                     HPDF_UINT16    angle)
{
    HPDF_Number n;
    HPDF_STATUS ret = HPDF_OK;

    HPDF_PTRACE((" HPDF_Page_SetRotate\n"));

    if (!HPDF_Page_Validate (page))
        return HPDF_INVALID_PAGE;

    if (angle % 90 != 0)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_ROTATE_VALUE,
                (HPDF_STATUS)angle);

    n = HPDF_Page_GetInheritableItem (page, "Rotate", HPDF_OCLASS_NUMBER);

    if (!n)
        ret = HPDF_Dict_AddNumber (page, "Rotate", angle);
    else
        n->value = angle;

    return ret;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetZoom  (HPDF_Page   page,
                    HPDF_REAL   zoom)
{
    HPDF_STATUS ret = HPDF_OK;

    HPDF_PTRACE((" HPDF_Page_SetZoom\n"));

    if (!HPDF_Page_Validate (page)) {
        return HPDF_INVALID_PAGE;
    }

    if (zoom < 0.08 || zoom > 32) {
        return HPDF_RaiseError (page->error, HPDF_INVALID_PARAMETER, 0);
    }

    ret = HPDF_Dict_AddReal (page, "PZ", zoom);
    return ret;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetWidth  (HPDF_Page    page,
                     HPDF_REAL    value)
{
    HPDF_PTRACE((" HPDF_Page_SetWidth\n"));

    if (value < 3 || value > 14400)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_SIZE, 0);

    if (HPDF_Page_SetBoxValue (page, "MediaBox", 2, value) != HPDF_OK)
        return HPDF_CheckError (page->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetHeight  (HPDF_Page    page,
                      HPDF_REAL    value)
{
    HPDF_PTRACE((" HPDF_Page_SetWidth\n"));

    if (value < 3 || value > 14400)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_SIZE, 0);

    if (HPDF_Page_SetBoxValue (page, "MediaBox", 3, value) != HPDF_OK)
        return HPDF_CheckError (page->error);

    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetSize  (HPDF_Page             page,
                    HPDF_PageSizes        size,
                    HPDF_PageDirection    direction)
{
    HPDF_STATUS ret = HPDF_OK;

    HPDF_PTRACE((" HPDF_Page_SetSize\n"));

    if (!HPDF_Page_Validate (page))
        return HPDF_INVALID_PAGE;

    if (size < 0 || size > HPDF_PAGE_SIZE_EOF)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_SIZE,
                (HPDF_STATUS)direction);

    if (direction == HPDF_PAGE_LANDSCAPE) {
        ret += HPDF_Page_SetHeight (page,
            HPDF_PREDEFINED_PAGE_SIZES[(HPDF_UINT)size].x);
        ret += HPDF_Page_SetWidth (page,
            HPDF_PREDEFINED_PAGE_SIZES[(HPDF_UINT)size].y);
    } else if (direction == HPDF_PAGE_PORTRAIT) {
        ret += HPDF_Page_SetHeight (page,
            HPDF_PREDEFINED_PAGE_SIZES[(HPDF_UINT)size].y);
        ret += HPDF_Page_SetWidth (page,
            HPDF_PREDEFINED_PAGE_SIZES[(HPDF_UINT)size].x);
    } else
        ret = HPDF_SetError (page->error, HPDF_PAGE_INVALID_DIRECTION,
                (HPDF_STATUS)direction);

    if (ret != HPDF_OK)
        return HPDF_CheckError (page->error);

    return HPDF_OK;
}


HPDF_BOOL
HPDF_Page_Validate  (HPDF_Page  page)
{
    HPDF_Obj_Header *header = (HPDF_Obj_Header *)page;

    HPDF_PTRACE((" HPDF_Page_Validate\n"));

    if (!page || !page->attr)
        return HPDF_FALSE;

    if (header->obj_class != (HPDF_OCLASS_DICT | HPDF_OSUBCLASS_PAGE))
        return HPDF_FALSE;

    return HPDF_TRUE;
}


HPDF_EXPORT(HPDF_Destination)
HPDF_Page_CreateDestination  (HPDF_Page   page)
{
    HPDF_PageAttr attr;
    HPDF_Destination dst;

    HPDF_PTRACE((" HPDF_Page_CreateDestination\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    dst = HPDF_Destination_New (page->mmgr, page, attr->xref);
    if (!dst)
        HPDF_CheckError (page->error);

    return dst;
}


HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_Create3DAnnot    (HPDF_Page       page,
                            HPDF_Rect       rect,
                            HPDF_U3D u3d)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;

    HPDF_PTRACE((" HPDF_Page_Create3DAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    annot = HPDF_3DAnnot_New (page->mmgr, attr->xref, rect, u3d);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateTextAnnot  (HPDF_Page          page,
                            HPDF_Rect          rect,
                            const char   *text,
                            HPDF_Encoder       encoder)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;

    HPDF_PTRACE((" HPDF_Page_CreateTextAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    if (encoder && !HPDF_Encoder_Validate (encoder)) {
        HPDF_RaiseError (page->error, HPDF_INVALID_ENCODER, 0);
        return NULL;
    }

    annot = HPDF_MarkupAnnot_New (page->mmgr, attr->xref, rect, text, encoder, HPDF_ANNOT_TEXT_NOTES);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateFreeTextAnnot  (HPDF_Page          page,
                                HPDF_Rect          rect,
                                const char   *text,
                                HPDF_Encoder       encoder)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;

    HPDF_PTRACE((" HPDF_Page_CreateFreeTextAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    if (encoder && !HPDF_Encoder_Validate (encoder)) {
        HPDF_RaiseError (page->error, HPDF_INVALID_ENCODER, 0);
        return NULL;
    }

    annot = HPDF_MarkupAnnot_New (page->mmgr, attr->xref, rect, text, encoder, HPDF_ANNOT_FREE_TEXT);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateLineAnnot  (HPDF_Page          page,
                            const char           *text,
                            HPDF_Encoder       encoder)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;
    HPDF_Rect rect = {0,0,0,0};

    HPDF_PTRACE((" HPDF_Page_CreateLineAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    if (encoder && !HPDF_Encoder_Validate (encoder)) {
        HPDF_RaiseError (page->error, HPDF_INVALID_ENCODER, 0);
        return NULL;
    }

    annot = HPDF_MarkupAnnot_New (page->mmgr, attr->xref, rect, text, encoder, HPDF_ANNOT_LINE);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateLinkAnnot  (HPDF_Page          page,
                            HPDF_Rect          rect,
                            HPDF_Destination   dst)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;

    HPDF_PTRACE((" HPDF_Page_CreateLinkAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    if (!HPDF_Destination_Validate (dst)) {
        HPDF_RaiseError (page->error, HPDF_INVALID_DESTINATION, 0);
        return NULL;
    }

    annot = HPDF_LinkAnnot_New (page->mmgr, attr->xref, rect, dst);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}


HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateURILinkAnnot  (HPDF_Page          page,
                               HPDF_Rect          rect,
                               const char   *uri)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;

    HPDF_PTRACE((" HPDF_Page_CreateURILinkAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_StrLen (uri, HPDF_LIMIT_MAX_STRING_LEN) <= 0) {
        HPDF_RaiseError (page->error, HPDF_INVALID_URI, 0);
        return NULL;
    }

    annot = HPDF_URILinkAnnot_New (page->mmgr, attr->xref, rect, uri);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateCircleAnnot (HPDF_Page          page,
                             HPDF_Rect          rect,
                             const char            *text,
                             HPDF_Encoder       encoder)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;

    HPDF_PTRACE((" HPDF_Page_CreateCircleAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    if (encoder && !HPDF_Encoder_Validate (encoder)) {
        HPDF_RaiseError (page->error, HPDF_INVALID_ENCODER, 0);
        return NULL;
    }

    annot = HPDF_MarkupAnnot_New (page->mmgr, attr->xref, rect, text, encoder, HPDF_ANNOT_CIRCLE);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateSquareAnnot (HPDF_Page          page,
                             HPDF_Rect          rect,
                             const char            *text,
                             HPDF_Encoder       encoder)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;

    HPDF_PTRACE((" HPDF_Page_CreateCircleAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    if (encoder && !HPDF_Encoder_Validate (encoder)) {
        HPDF_RaiseError (page->error, HPDF_INVALID_ENCODER, 0);
        return NULL;
    }

    annot = HPDF_MarkupAnnot_New (page->mmgr, attr->xref, rect, text, encoder, HPDF_ANNOT_SQUARE);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}

HPDF_EXPORT(HPDF_Dict)
HPDF_Page_Create3DView    (HPDF_Page       page,
                           HPDF_U3D        u3d,
                           HPDF_Annotation    annot3d,
                           const char *name)
{
    HPDF_PageAttr attr;
    HPDF_Dict view;

    HPDF_PTRACE((" HPDF_Page_Create3DView\n"));
    HPDF_UNUSED(annot3d);

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    view = HPDF_3DView_New( page->mmgr, attr->xref, u3d, name);
    if (!view) {
        HPDF_CheckError (page->error);
    }
    return view;
}

HPDF_Annotation
HPDF_Page_CreateTextMarkupAnnot (HPDF_Page     page,
                                HPDF_Rect      rect,
                                const char     *text,
                                HPDF_Encoder   encoder,
                                HPDF_AnnotType subType)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;

    HPDF_PTRACE((" HPDF_Page_CreateTextMarkupAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    if (encoder && !HPDF_Encoder_Validate (encoder)) {
        HPDF_RaiseError (page->error, HPDF_INVALID_ENCODER, 0);
        return NULL;
    }

    annot = HPDF_MarkupAnnot_New ( page->mmgr, attr->xref, rect, text, encoder, subType);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}


HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateHighlightAnnot  (HPDF_Page          page,
                                HPDF_Rect          rect,
                                const char   *text,
                                HPDF_Encoder       encoder)
{
    HPDF_PTRACE((" HPDF_Page_CreateHighlightAnnot\n"));

    return HPDF_Page_CreateTextMarkupAnnot( page, rect, text, encoder, HPDF_ANNOT_HIGHTLIGHT);
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateSquigglyAnnot  (HPDF_Page          page,
                                HPDF_Rect          rect,
                                const char   *text,
                                HPDF_Encoder       encoder)
{
    HPDF_PTRACE((" HPDF_Page_CreateSquigglyAnnot\n"));

    return HPDF_Page_CreateTextMarkupAnnot( page, rect, text, encoder, HPDF_ANNOT_SQUIGGLY);
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateUnderlineAnnot  (HPDF_Page          page,
                                HPDF_Rect          rect,
                                const char   *text,
                                HPDF_Encoder       encoder)
{
    HPDF_PTRACE((" HPDF_Page_CreateUnderlineAnnot\n"));

    return HPDF_Page_CreateTextMarkupAnnot( page, rect, text, encoder, HPDF_ANNOT_UNDERLINE);
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateStrikeOutAnnot  (HPDF_Page          page,
                                HPDF_Rect          rect,
                                const char   *text,
                                HPDF_Encoder       encoder)
{
    HPDF_PTRACE((" HPDF_Page_CreateStrikeOutAnnot\n"));

    return HPDF_Page_CreateTextMarkupAnnot( page, rect, text, encoder, HPDF_ANNOT_STRIKE_OUT);
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreatePopupAnnot  (    HPDF_Page          page,
                                HPDF_Rect          rect,
                                HPDF_Annotation       parent)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;

    HPDF_PTRACE((" HPDF_Page_CreatePopupAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    annot = HPDF_PopupAnnot_New ( page->mmgr, attr->xref, rect, parent);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateStampAnnot  (    HPDF_Page           page,
                                HPDF_Rect           rect,
                                HPDF_StampAnnotName name,
                                const char*            text,
                                HPDF_Encoder        encoder)
{
    HPDF_PageAttr attr;
    HPDF_Annotation annot;

    HPDF_PTRACE((" HPDF_Page_CreateStampAnnot\n"));

    if (!HPDF_Page_Validate (page))
        return NULL;

    attr = (HPDF_PageAttr)page->attr;

    annot = HPDF_StampAnnot_New ( page->mmgr, attr->xref, rect, name, text, encoder);
    if (annot) {
        if (AddAnnotation (page, annot) != HPDF_OK) {
            HPDF_CheckError (page->error);
            annot = NULL;
        }
    } else
        HPDF_CheckError (page->error);

    return annot;
}

HPDF_EXPORT(HPDF_Annotation)
HPDF_Page_CreateProjectionAnnot(HPDF_Page page,
								HPDF_Rect rect,
								const char* text,
								HPDF_Encoder encoder)
{
	HPDF_PageAttr attr;
	HPDF_Annotation annot;

	HPDF_PTRACE((" HPDF_Page_CreateProjectionAnnot\n"));

	if (!HPDF_Page_Validate (page))
		return NULL;

	attr = (HPDF_PageAttr)page->attr;

	annot = HPDF_ProjectionAnnot_New (page->mmgr, attr->xref, rect, text, encoder);
	if (annot) {
		if (AddAnnotation (page, annot) != HPDF_OK) {
			HPDF_CheckError (page->error);
			annot = NULL;
		}
	} else
		HPDF_CheckError (page->error);

	return annot;
}


HPDF_EXPORT(HPDF_3DMeasure)
HPDF_Page_Create3DC3DMeasure(HPDF_Page page,
							 HPDF_Point3D    firstanchorpoint,
							 HPDF_Point3D    textanchorpoint)
{
	HPDF_PageAttr attr;
	HPDF_Annotation measure;

	HPDF_PTRACE((" HPDF_Page_Create3DC3DMeasure\n"));

	if (!HPDF_Page_Validate (page))
		return NULL;

	attr = (HPDF_PageAttr)page->attr;

	measure = HPDF_3DC3DMeasure_New(page->mmgr, attr->xref, firstanchorpoint, textanchorpoint);
	if ( !measure) 
		HPDF_CheckError (page->error);

	return measure;
}

HPDF_EXPORT(HPDF_3DMeasure)
HPDF_Page_CreatePD33DMeasure(HPDF_Page       page,
							 HPDF_Point3D    annotationPlaneNormal,
							 HPDF_Point3D    firstAnchorPoint,
							 HPDF_Point3D    secondAnchorPoint,
							 HPDF_Point3D    leaderLinesDirection,
							 HPDF_Point3D    measurementValuePoint,
							 HPDF_Point3D    textYDirection,
							 HPDF_REAL       value,
							 const char*     unitsString
							 )
{
	HPDF_PageAttr attr;
	HPDF_Annotation measure;

	HPDF_PTRACE((" HPDF_Page_CreatePD33DMeasure\n"));

	if (!HPDF_Page_Validate (page))
		return NULL;

	attr = (HPDF_PageAttr)page->attr;

	measure = HPDF_PD33DMeasure_New(page->mmgr, 
		attr->xref, 
		annotationPlaneNormal, 
		firstAnchorPoint,
		secondAnchorPoint,
		leaderLinesDirection,
		measurementValuePoint,
		textYDirection,
		value,
		unitsString
		);
	if ( !measure) 
		HPDF_CheckError (page->error);

	return measure;
}


HPDF_EXPORT(HPDF_ExData)
HPDF_Page_Create3DAnnotExData(HPDF_Page page)
{
	HPDF_PageAttr attr;
	HPDF_Annotation exData;

	HPDF_PTRACE((" HPDF_Page_Create3DAnnotExData\n"));

	if (!HPDF_Page_Validate (page))
		return NULL;

	attr = (HPDF_PageAttr)page->attr;

	exData = HPDF_3DAnnotExData_New(page->mmgr, attr->xref);
	if ( !exData) 
		HPDF_CheckError (page->error);

	return exData;
}



void
HPDF_Page_SetFilter  (HPDF_Page    page,
                      HPDF_UINT    filter)
{
    HPDF_PageAttr attr;

    HPDF_PTRACE((" HPDF_Page_SetFilter\n"));

    attr = (HPDF_PageAttr)page->attr;
    attr->contents->filter = filter;
}

