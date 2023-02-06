/*
 * << Haru Free PDF Library >> -- hpdf_annotation.c
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
#include "hpdf_info.h"
#include "hpdf_exdata.h"
#include "hpdf.h"

/*----------------------------------------------------------------------------*/
/*------ HPDF_ExData -----------------------------------------------------*/



HPDF_ExData
HPDF_3DAnnotExData_New(HPDF_MMgr mmgr,
					   HPDF_Xref xref)
{
	HPDF_ExData exdata;
	HPDF_STATUS ret = HPDF_OK;


	HPDF_PTRACE((" HPDF_ExData_New\n"));

	exdata = HPDF_Dict_New (mmgr);
	if (!exdata)
		return NULL;

	if (HPDF_Xref_Add (xref, exdata) != HPDF_OK)
		return NULL;

	ret += HPDF_Dict_AddName (exdata, "Type", "ExData");
	ret += HPDF_Dict_AddName (exdata, "Subtype", "3DM");

	if (ret != HPDF_OK)
		return NULL;

	return exdata;
}



HPDF_EXPORT(HPDF_STATUS)
HPDF_3DAnnotExData_Set3DMeasurement(HPDF_ExData exdata, 
						   HPDF_3DMeasure measure)
{
	HPDF_STATUS ret = HPDF_OK;

	ret = HPDF_Dict_Add (exdata, "M3DREF", measure);
	if (ret != HPDF_OK)
		return ret;

	return ret;
}

