//
// LinkAnnotation.h
//
// Library: PDF
// Package: PDFCore
// Module:  LinkAnnotation
//
// Definition of the LinkAnnotation class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_LinkAnnotation_INCLUDED
#define PDF_LinkAnnotation_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Resource.h"
#include "Poco/PDF/Destination.h"


namespace Poco {
namespace PDF {


class PDF_API LinkAnnotation: public Resource<HPDF_Annotation>
	/// A LinkAnnotation represents a PDF annotation resource.
{
public:
	enum Highlight
	{
		HIGHTLIGHT_NONE = HPDF_ANNOT_NO_HIGHTLIGHT,
			/// No highlighting.
		HIGHTLIGHT_INVERT_BOX = HPDF_ANNOT_INVERT_BOX,
			/// Invert the contents of the area of annotation.
		HIGHTLIGHT_INVERT_BORDER = HPDF_ANNOT_INVERT_BORDER,
			/// Invert the annotation’s border.
		HIGHTLIGHT_DOWN_APPEARANCE = HPDF_ANNOT_DOWN_APPEARANCE
			/// Dent the annotation.
	};

	LinkAnnotation(HPDF_Doc* pPDF,
		const HPDF_Annotation& annotation,
		const std::string& name = "");
		/// Creates the annotation.

	virtual ~LinkAnnotation();
		/// Destroys the annotation.

	void setHighlight(Highlight mode);
		/// Sets highlighting of the link.

	void setBorderStyle(float width, Poco::UInt32 dashOn, Poco::UInt32 dashOff);
		/// Sets the link border style.
};


//
// inlines
//

inline void LinkAnnotation::setHighlight(Highlight mode)
{
	HPDF_LinkAnnot_SetHighlightMode(handle(),
		static_cast<HPDF_AnnotHighlightMode>(mode));
}

inline void LinkAnnotation::setBorderStyle(float width, Poco::UInt32 dashOn, Poco::UInt32 dashOff)
{
	HPDF_LinkAnnot_SetBorderStyle(handle(), width, dashOn, dashOff);
}


} } // namespace Poco::PDF


#endif // PDF_LinkAnnotation_INCLUDED
