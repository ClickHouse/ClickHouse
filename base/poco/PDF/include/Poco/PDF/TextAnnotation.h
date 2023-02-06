//
// TextAnnotation.h
//
// Library: PDF
// Package: PDFCore
// Module:  TextAnnotation
//
// Definition of the TextAnnotation class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_TextAnnotation_INCLUDED
#define PDF_TextAnnotation_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Resource.h"
#include "Poco/PDF/Destination.h"


namespace Poco {
namespace PDF {


class PDF_API TextAnnotation: public Resource<HPDF_Annotation>
	/// A TextAnnotation represents a PDF annotation resource.
{
public:
	enum IconType
	{
		ANNOTATION_ICON_COMMENT = HPDF_ANNOT_ICON_COMMENT,
		ANNOTATION_ICON_KEY = HPDF_ANNOT_ICON_KEY,
		ANNOTATION_ICON_NOTE = HPDF_ANNOT_ICON_NOTE,
		ANNOTATION_ICON_HELP = HPDF_ANNOT_ICON_HELP,
		ANNOTATION_ICON_NEW_PARAGRAPH = HPDF_ANNOT_ICON_NEW_PARAGRAPH,
		ANNOTATION_ICON_PARAGRAPH = HPDF_ANNOT_ICON_PARAGRAPH,
		ANNOTATION_ICON_INSERT = HPDF_ANNOT_ICON_INSERT
	};

	TextAnnotation(HPDF_Doc* pPDF,
		const HPDF_Annotation& annotation,
		const std::string& name = "");
		/// Creates the annotation.

	virtual ~TextAnnotation();
		/// Destroys the annotation.

	void open();
		/// Opens the annotation.

	void close();
		/// Closes the annotation.

	void icon(IconType iconType);
};


//
// inlines
//

inline void TextAnnotation::open()
{
	HPDF_TextAnnot_SetOpened(handle(), HPDF_TRUE);
}


inline void TextAnnotation::close()
{
	HPDF_TextAnnot_SetOpened(handle(), HPDF_FALSE);
}


inline void TextAnnotation::icon(IconType iconType)
{
	HPDF_TextAnnot_SetIcon(handle(), static_cast<HPDF_AnnotIcon>(iconType));
}


} } // namespace Poco::PDF


#endif // PDF_TextAnnotation_INCLUDED
