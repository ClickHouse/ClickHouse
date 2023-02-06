/*
 * << Haru Free PDF Library >> -- hpdf_annotation.h
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

#ifndef _HPDF_3DMEASURE_H
#define _HPDF_3DMEASURE_H

#include "hpdf_objects.h"

#ifdef __cplusplus
extern "C" {
#endif

/*----------------------------------------------------------------------------*/
/*------ HPDF_3DMeasure -----------------------------------------------------*/


HPDF_3DMeasure
HPDF_3DC3DMeasure_New(HPDF_MMgr mmgr,
		              HPDF_Xref xref,
		              HPDF_Point3D    firstanchorpoint,
		              HPDF_Point3D    textanchorpoint
		);
             
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
					  );


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_3DMEASURE_H */

