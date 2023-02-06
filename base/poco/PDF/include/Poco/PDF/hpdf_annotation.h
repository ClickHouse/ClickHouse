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

#ifndef _HPDF_ANNOTATION_H
#define _HPDF_ANNOTATION_H

#include "hpdf_objects.h"

#ifdef __cplusplus
extern "C" {
#endif

/*----------------------------------------------------------------------------*/
/*------ HPDF_Annotation -----------------------------------------------------*/


HPDF_Annotation
HPDF_Annotation_New  (HPDF_MMgr       mmgr,
                      HPDF_Xref       xref,
                      HPDF_AnnotType  type,
                      HPDF_Rect       rect);


HPDF_Annotation
HPDF_LinkAnnot_New  (HPDF_MMgr           mmgr,
                     HPDF_Xref         xref,
                     HPDF_Rect         rect,
                     HPDF_Destination  dst);


HPDF_Annotation
HPDF_URILinkAnnot_New  (HPDF_MMgr          mmgr,
                        HPDF_Xref          xref,
                        HPDF_Rect          rect,
                        const char   *uri);


HPDF_Annotation
HPDF_3DAnnot_New  (HPDF_MMgr        mmgr,
                   HPDF_Xref        xref,
                   HPDF_Rect        rect,
                   HPDF_U3D u3d);

HPDF_Annotation
HPDF_MarkupAnnot_New    (HPDF_MMgr        mmgr,
						 HPDF_Xref        xref,
						 HPDF_Rect        rect,
						 const char      *text,
						 HPDF_Encoder     encoder,
					 	 HPDF_AnnotType  subtype);

HPDF_Annotation
HPDF_PopupAnnot_New (HPDF_MMgr         mmgr,
                     HPDF_Xref         xref,
                     HPDF_Rect         rect,
					 HPDF_Annotation   parent);

HPDF_Annotation
HPDF_StampAnnot_New (HPDF_MMgr         mmgr,
                     HPDF_Xref         xref,
                     HPDF_Rect         rect,
					 HPDF_StampAnnotName name,
					 const char*	   text,
					 HPDF_Encoder	   encoder);

HPDF_Annotation
HPDF_ProjectionAnnot_New (HPDF_MMgr         mmgr,
						  HPDF_Xref         xref,
						  HPDF_Rect         rect,
						  const char*       text,
						  HPDF_Encoder       encoder);

HPDF_BOOL
HPDF_Annotation_Validate (HPDF_Annotation  annot);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_ANNOTATION_H */

