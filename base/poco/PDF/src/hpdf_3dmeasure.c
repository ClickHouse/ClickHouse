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
#include "hpdf_3dmeasure.h"
#include "hpdf.h"

/*----------------------------------------------------------------------------*/
/*------ HPDF_3DMeasure -----------------------------------------------------*/

HPDF_STATUS
HPDF_Dict_AddPoint3D(HPDF_Dict dict, const char* key, HPDF_Point3D point)
{
	HPDF_Array array;
	HPDF_STATUS ret = HPDF_OK;
	array = HPDF_Array_New (dict->mmgr);
	if (!array)
		return HPDF_Error_GetCode ( dict->error);

	if (HPDF_Dict_Add (dict, key, array) != HPDF_OK)
		return HPDF_Error_GetCode ( dict->error);

	ret += HPDF_Array_AddReal(array, point.x);
	ret += HPDF_Array_AddReal(array, point.y);
	ret += HPDF_Array_AddReal(array, point.z);

	return ret;
}




HPDF_3DMeasure
HPDF_3DC3DMeasure_New(HPDF_MMgr mmgr,
					  HPDF_Xref xref,
					  HPDF_Point3D    firstanchorpoint,
					  HPDF_Point3D    textanchorpoint
					  )
{
	HPDF_3DMeasure measure;
	HPDF_STATUS ret = HPDF_OK;


	HPDF_PTRACE((" HPDF_3DC3DMeasure_New\n"));

	measure = HPDF_Dict_New (mmgr);
	if (!measure)
		return NULL;

	if (HPDF_Xref_Add (xref, measure) != HPDF_OK)
		return NULL;

	ret += HPDF_Dict_AddPoint3D(measure, "A1", firstanchorpoint);
	ret += HPDF_Dict_AddPoint3D(measure, "TP", textanchorpoint);

	ret += HPDF_Dict_AddName (measure, "Type", "3DMeasure");
	ret += HPDF_Dict_AddName (measure, "Subtype", "3DC");

	if (ret != HPDF_OK)
		return NULL;

	return measure;
}



HPDF_EXPORT(HPDF_STATUS)
HPDF_3DMeasure_SetColor(HPDF_3DMeasure measure, 
						   HPDF_RGBColor color)
{
	HPDF_Array array;
	HPDF_STATUS ret = HPDF_OK;

	array = HPDF_Array_New (measure->mmgr);
	if (!array)
		return HPDF_Error_GetCode (measure->error);

	ret = HPDF_Dict_Add (measure, "C", array);
	if (ret != HPDF_OK)
		return ret;

	ret += HPDF_Array_AddName(array, "DeviceRGB");
	ret += HPDF_Array_AddReal(array, color.r);
	ret += HPDF_Array_AddReal(array, color.g);
	ret += HPDF_Array_AddReal(array, color.b);

	return ret;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_3DMeasure_SetTextSize(HPDF_3DMeasure measure, 
						   HPDF_REAL textsize)
{
	HPDF_STATUS ret = HPDF_OK;

	ret = HPDF_Dict_AddReal(measure, "TS", textsize);

	return ret;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_3DMeasure_SetName(HPDF_3DMeasure measure, 
					   const char* name)
{
	HPDF_STATUS ret = HPDF_OK;
	HPDF_String s;

	s = HPDF_String_New (measure->mmgr, name, 0);
	if (!s)
		return HPDF_Error_GetCode ( s->error);

	ret = HPDF_Dict_Add(measure, "TRL", s);

	return ret;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_3DC3DMeasure_SetTextBoxSize(HPDF_3DMeasure measure, 
							 HPDF_INT32 x,
							 HPDF_INT32 y)
{
	HPDF_Array array;
	HPDF_STATUS ret = HPDF_OK;

	array = HPDF_Array_New (measure->mmgr);
	if (!array)
		return HPDF_Error_GetCode (measure->error);

	ret = HPDF_Dict_Add (measure, "TB", array);
	if (ret != HPDF_OK)
		return ret;

	ret += HPDF_Array_AddNumber(array, x);
	ret += HPDF_Array_AddNumber(array, y);

	return ret;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_3DC3DMeasure_SetText(HPDF_3DMeasure measure, 
						  const char* text,
						  HPDF_Encoder encoder)
{
	HPDF_STATUS ret = HPDF_OK;
	HPDF_String s;

	s = HPDF_String_New (measure->mmgr, text, encoder);
	if (!s)
		return HPDF_Error_GetCode ( s->error);

	ret = HPDF_Dict_Add(measure, "UT", s);

	return ret;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_3DC3DMeasure_SetProjectionAnotation(HPDF_3DMeasure measure, 
						                 HPDF_Annotation projectionanotation)
{
	HPDF_STATUS ret = HPDF_OK;

	ret = HPDF_Dict_Add(measure, "S", projectionanotation);

	return ret;
}


HPDF_3DMeasure
HPDF_PD33DMeasure_New(HPDF_MMgr mmgr,
					  HPDF_Xref xref,
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
	HPDF_3DMeasure measure;
	HPDF_STATUS ret = HPDF_OK;
	HPDF_String s;

	HPDF_PTRACE((" HPDF_3DC3DMeasure_New\n"));

	measure = HPDF_Dict_New (mmgr);
	if (!measure)
		return NULL;

	if (HPDF_Xref_Add (xref, measure) != HPDF_OK)
		return NULL;

	ret += HPDF_Dict_AddPoint3D(measure, "AP", annotationPlaneNormal);
	ret += HPDF_Dict_AddPoint3D(measure, "A1", firstAnchorPoint);
	ret += HPDF_Dict_AddPoint3D(measure, "A2", secondAnchorPoint);
	ret += HPDF_Dict_AddPoint3D(measure, "D1", leaderLinesDirection);
	ret += HPDF_Dict_AddPoint3D(measure, "TP", measurementValuePoint);
	ret += HPDF_Dict_AddPoint3D(measure, "TY", textYDirection);

	ret += HPDF_Dict_AddReal(measure, "V", value);

	s = HPDF_String_New (measure->mmgr, unitsString, 0);
	if (!s)
		return NULL;

	ret = HPDF_Dict_Add(measure, "U", s);

	ret += HPDF_Dict_AddName (measure, "Type", "3DMeasure");
	ret += HPDF_Dict_AddName (measure, "Subtype", "PD3");

	if (ret != HPDF_OK)
		return NULL;

	return measure;
}

