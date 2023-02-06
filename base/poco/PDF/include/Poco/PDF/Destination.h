//
// Destination.h
//
// Library: PDF
// Package: PDFCore
// Module:  Destination
//
// Definition of the Destination class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_Destination_INCLUDED
#define PDF_Destination_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Resource.h"


namespace Poco {
namespace PDF {


class PDF_API Destination: public Resource<HPDF_Destination>
	/// Destination class represents destination resource.
{
public:
	Destination(HPDF_Doc* pPDF, const HPDF_Destination& resource, const std::string& name = "");
		/// Creates the destination.

	~Destination();
		/// Destroys the destination.

	void positionAndZoom(float x, float y, float zoom);
		/// Sets the position and zoom for destination.

	void fit();
		/// Sets the appearance of the page to displaying entire page within the window. 

	void fitHorizontal(float top);
		/// Defines the appearance of a page to magnifying to fit the height of the
		/// page within the window and setting the top position of the page to the
		/// value of the "top" parameter.

	void fitVertical(float left);
		/// Defines the appearance of a page to magnifying to fit the height of the
		/// page within the window and setting the left position of the page to the
		/// value of the "left" parameter.

	void fitRectangle(float left, float top, float right, float bottom);
		/// Defines the appearance of a page to magnifying the page to fit a rectangle
		/// specified by left, bottom, right and top.

	void fitWindow();
		/// Sets the appearance of the page to magnifying to fit  the bounding box of
		/// the page within the window. 

	void fitWindowHorizontal(float top);
		/// Defines the appearance of a page to magnifying to fit the width of the
		/// bounding box of the page within the window and setting the top position
		/// of the page to the value of the "top" parameter.

	void fitWindowVertical(float left);
		/// Defines the appearance of a page to magnifying to fit the height of the
		/// bounding box of the page within the window and setting the left position
		/// of the page to the value of the "left" parameter.
};


//
// inlines
//

inline void Destination::positionAndZoom(float x, float y, float zoom)
{
	HPDF_Destination_SetXYZ(handle(), x, y, zoom);
}


inline void Destination::fit()
{
	HPDF_Destination_SetFit(handle());
}


inline void Destination::fitHorizontal(float top)
{
	HPDF_Destination_SetFitH(handle(), top);
}


inline void Destination::fitVertical(float left)
{
	HPDF_Destination_SetFitV(handle(), left);
}


inline void Destination::fitRectangle(float left, float top, float right, float bottom)
{
	HPDF_Destination_SetFitR(handle(), left, bottom, right, top);
}


inline void Destination::fitWindow()
{
	HPDF_Destination_SetFitB(handle());
}


inline void Destination::fitWindowHorizontal(float top)
{
	HPDF_Destination_SetFitBH(handle(), top);
}


inline void Destination::fitWindowVertical(float left)
{
	HPDF_Destination_SetFitBV(handle(), left);
}


} } // namespace Poco::PDF


#endif // PDF_Destination_INCLUDED
