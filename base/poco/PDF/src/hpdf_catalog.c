/*
 * << Haru Free PDF Library >> -- hpdf_catalog.c
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
#include "hpdf_catalog.h"
#include "hpdf_pages.h"

static const char * const HPDF_PAGE_LAYOUT_NAMES[] = {
                        "SinglePage",
                        "OneColumn",
                        "TwoColumnLeft",
                        "TwoColumnRight",
                        "TwoPageLeft",
                        "TwoPageRight",
                        NULL
};


static const char * const HPDF_PAGE_MODE_NAMES[] = {
                        "UseNone",
                        "UseOutlines",
                        "UseThumbs",
                        "FullScreen",
                        "UseOC",
                        "UseAttachments",
                        NULL
};


HPDF_Catalog
HPDF_Catalog_New  (HPDF_MMgr  mmgr,
                   HPDF_Xref  xref)
{
    HPDF_Catalog catalog;
    HPDF_STATUS ret = 0;

    catalog = HPDF_Dict_New (mmgr);
    if (!catalog)
        return NULL;

    catalog->header.obj_class |= HPDF_OSUBCLASS_CATALOG;

    if (HPDF_Xref_Add (xref, catalog) != HPDF_OK)
        return NULL;

    /* add requiered elements */
    ret += HPDF_Dict_AddName (catalog, "Type", "Catalog");
    ret += HPDF_Dict_Add (catalog, "Pages", HPDF_Pages_New (mmgr, NULL, xref));

    if (ret != HPDF_OK)
        return NULL;

    return catalog;
}


HPDF_Pages
HPDF_Catalog_GetRoot  (HPDF_Catalog  catalog)
{
    HPDF_Dict pages;

    if (!catalog)
        return NULL;

    pages = HPDF_Dict_GetItem (catalog, "Pages", HPDF_OCLASS_DICT);
    if (!pages || pages->header.obj_class != (HPDF_OSUBCLASS_PAGES |
                HPDF_OCLASS_DICT))
        HPDF_SetError (catalog->error, HPDF_PAGE_CANNOT_GET_ROOT_PAGES, 0);

    return pages;
}


HPDF_NameDict
HPDF_Catalog_GetNames  (HPDF_Catalog catalog)
{
    if (!catalog)
        return NULL;
    return HPDF_Dict_GetItem (catalog, "Names", HPDF_OCLASS_DICT);
}


HPDF_STATUS
HPDF_Catalog_SetNames  (HPDF_Catalog catalog,
                        HPDF_NameDict dict)
{
    return HPDF_Dict_Add (catalog, "Names", dict);
}


HPDF_PageLayout
HPDF_Catalog_GetPageLayout  (HPDF_Catalog  catalog)
{
    HPDF_Name layout;
    HPDF_UINT i = 0;

    layout = (HPDF_Name)HPDF_Dict_GetItem (catalog, "PageLayout",
            HPDF_OCLASS_NAME);
    if (!layout)
        return HPDF_PAGE_LAYOUT_EOF;

    while (HPDF_PAGE_LAYOUT_NAMES[i]) {
        if (HPDF_StrCmp (layout->value, HPDF_PAGE_LAYOUT_NAMES[i]) == 0)
            return (HPDF_PageLayout)i;
        i++;
    }

    return HPDF_PAGE_LAYOUT_EOF;
}


HPDF_STATUS
HPDF_Catalog_SetPageLayout  (HPDF_Catalog      catalog,
                             HPDF_PageLayout   layout)
{
    return HPDF_Dict_AddName (catalog, "PageLayout",
                    HPDF_PAGE_LAYOUT_NAMES[(HPDF_INT)layout]);
}




HPDF_PageMode
HPDF_Catalog_GetPageMode  (HPDF_Catalog  catalog)
{
    HPDF_Name mode;
    HPDF_UINT i = 0;

    mode = (HPDF_Name)HPDF_Dict_GetItem (catalog, "PageMode", HPDF_OCLASS_NAME);
    if (!mode)
        return HPDF_PAGE_MODE_USE_NONE;

    while (HPDF_PAGE_MODE_NAMES[i]) {
        if (HPDF_StrCmp (mode->value, HPDF_PAGE_MODE_NAMES[i]) == 0)
            return (HPDF_PageMode)i;
        i++;
    }

    return HPDF_PAGE_MODE_USE_NONE;
}


HPDF_STATUS
HPDF_Catalog_SetPageMode  (HPDF_Catalog   catalog,
                           HPDF_PageMode  mode)
{
    return HPDF_Dict_AddName (catalog, "PageMode",
                    HPDF_PAGE_MODE_NAMES[(HPDF_INT)mode]);
}


HPDF_STATUS
HPDF_Catalog_SetOpenAction  (HPDF_Catalog       catalog,
                             HPDF_Destination   open_action)
{
    if (!open_action) {
        HPDF_Dict_RemoveElement (catalog, "OpenAction");
        return HPDF_OK;
    }

    return HPDF_Dict_Add (catalog, "OpenAction", open_action);
}


HPDF_BOOL
HPDF_Catalog_Validate  (HPDF_Catalog   catalog)
{
    if (!catalog)
        return HPDF_FALSE;

    if (catalog->header.obj_class != (HPDF_OSUBCLASS_CATALOG |
                HPDF_OCLASS_DICT)) {
        HPDF_SetError (catalog->error, HPDF_INVALID_OBJECT, 0);
        return HPDF_FALSE;
    }

    return HPDF_TRUE;
}


HPDF_STATUS
HPDF_Catalog_AddPageLabel  (HPDF_Catalog   catalog,
                            HPDF_UINT      page_num,
                            HPDF_Dict      page_label)
{
    HPDF_STATUS ret;
    HPDF_Array nums;
    HPDF_Dict labels = HPDF_Dict_GetItem (catalog, "PageLabels",
        HPDF_OCLASS_DICT);

    HPDF_PTRACE ((" HPDF_Catalog_AddPageLabel\n"));

    if (!labels) {
        labels = HPDF_Dict_New (catalog->mmgr);

        if (!labels)
            return catalog->error->error_no;

        if ((ret = HPDF_Dict_Add (catalog, "PageLabels", labels)) != HPDF_OK)
            return ret;
    }

    nums = HPDF_Dict_GetItem (labels, "Nums", HPDF_OCLASS_ARRAY);

    if (!nums) {
        nums = HPDF_Array_New (catalog->mmgr);

        if (!nums)
            return catalog->error->error_no;

        if ((ret = HPDF_Dict_Add (labels, "Nums", nums)) != HPDF_OK)
            return ret;
    }

    if ((ret = HPDF_Array_AddNumber (nums, page_num)) != HPDF_OK)
        return ret;

    return HPDF_Array_Add (nums, page_label);
}

HPDF_STATUS
HPDF_Catalog_SetViewerPreference  (HPDF_Catalog   catalog,
                                   HPDF_UINT      value)
{
    HPDF_STATUS ret;
    HPDF_Dict preferences;

    HPDF_PTRACE ((" HPDF_Catalog_SetViewerPreference\n"));

    if (!value) {
        ret = HPDF_Dict_RemoveElement (catalog, "ViewerPreferences");

        if (ret == HPDF_DICT_ITEM_NOT_FOUND)
            ret = HPDF_OK;

        return ret;
    }

    preferences = HPDF_Dict_New (catalog->mmgr);
    if (!preferences)
        return catalog->error->error_no;

    if ((ret = HPDF_Dict_Add (catalog, "ViewerPreferences", preferences))
            != HPDF_OK)
        return ret;

    /*  */

    if (value & HPDF_HIDE_TOOLBAR) {
        if ((ret = HPDF_Dict_AddBoolean (preferences, "HideToolbar",
                HPDF_TRUE)) != HPDF_OK)
            return ret;
    } else {
        if ((ret = HPDF_Dict_RemoveElement (preferences, "HideToolbar")) !=
                HPDF_OK)
            if (ret != HPDF_DICT_ITEM_NOT_FOUND)
                return ret;
    }

    if (value & HPDF_HIDE_MENUBAR) {
        if ((ret = HPDF_Dict_AddBoolean (preferences, "HideMenubar",
                HPDF_TRUE)) != HPDF_OK)
            return ret;
    } else {
        if ((ret = HPDF_Dict_RemoveElement (preferences, "HideMenubar")) !=
                HPDF_OK)
            if (ret != HPDF_DICT_ITEM_NOT_FOUND)
                return ret;
    }

    if (value & HPDF_HIDE_WINDOW_UI) {
        if ((ret = HPDF_Dict_AddBoolean (preferences, "HideWindowUI",
                HPDF_TRUE)) != HPDF_OK)
            return ret;
    } else {
        if ((ret = HPDF_Dict_RemoveElement (preferences, "HideWindowUI")) !=
                HPDF_OK)
            if (ret != HPDF_DICT_ITEM_NOT_FOUND)
                return ret;
    }

    if (value & HPDF_FIT_WINDOW) {
        if ((ret = HPDF_Dict_AddBoolean (preferences, "FitWindow",
                HPDF_TRUE)) != HPDF_OK)
            return ret;
    } else {
        if ((ret = HPDF_Dict_RemoveElement (preferences, "FitWindow")) !=
                HPDF_OK)
            if (ret != HPDF_DICT_ITEM_NOT_FOUND)
                return ret;
    }

    if (value & HPDF_CENTER_WINDOW) {
        if ((ret = HPDF_Dict_AddBoolean (preferences, "CenterWindow",
                HPDF_TRUE)) != HPDF_OK)
            return ret;
    } else {
        if ((ret = HPDF_Dict_RemoveElement (preferences, "CenterWindow")) !=
                HPDF_OK)
            if (ret != HPDF_DICT_ITEM_NOT_FOUND)
                return ret;
    }

    if (value & HPDF_PRINT_SCALING_NONE) {
        if ((ret = HPDF_Dict_AddName (preferences, "PrintScaling",
                "None")) != HPDF_OK)
            return ret;
    } else {
        if ((ret = HPDF_Dict_RemoveElement (preferences, "PrintScaling")) !=
                HPDF_OK)
            if (ret != HPDF_DICT_ITEM_NOT_FOUND)
                return ret;
    }

    return HPDF_OK;
}

HPDF_UINT
HPDF_Catalog_GetViewerPreference  (HPDF_Catalog   catalog)
{
    HPDF_Dict preferences;
    HPDF_UINT value = 0;
    HPDF_Boolean obj;

    HPDF_PTRACE ((" HPDF_Catalog_GetViewerPreference\n"));

    preferences = (HPDF_Dict)HPDF_Dict_GetItem (catalog, "ViewerPreferences",
            HPDF_OCLASS_DICT);

    if (!preferences)
        return 0;

    obj = (HPDF_Boolean)HPDF_Dict_GetItem (preferences, "HideToolbar",
            HPDF_OCLASS_BOOLEAN);
    if (obj) {
        if (obj->value)
            value += HPDF_HIDE_TOOLBAR;
    }

    obj = (HPDF_Boolean)HPDF_Dict_GetItem (preferences, "HideMenubar",
            HPDF_OCLASS_BOOLEAN);
    if (obj) {
        if (obj->value)
            value += HPDF_HIDE_MENUBAR;
    }

    obj = (HPDF_Boolean)HPDF_Dict_GetItem (preferences, "HideWindowUI",
            HPDF_OCLASS_BOOLEAN);
    if (obj) {
        if (obj->value)
            value += HPDF_HIDE_WINDOW_UI;
    }

    obj = (HPDF_Boolean)HPDF_Dict_GetItem (preferences, "FitWindow",
            HPDF_OCLASS_BOOLEAN);
    if (obj) {
        if (obj->value)
            value += HPDF_FIT_WINDOW;
    }

    obj = (HPDF_Boolean)HPDF_Dict_GetItem (preferences, "CenterWindow",
            HPDF_OCLASS_BOOLEAN);
    if (obj) {
        if (obj->value)
            value += HPDF_CENTER_WINDOW;
    }

    return value;
}

