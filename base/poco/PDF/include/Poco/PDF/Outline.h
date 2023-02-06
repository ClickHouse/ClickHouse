//
// Outline.h
//
// Library: PDF
// Package: PDFCore
// Module:  Outline
//
// Definition of the Outline class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_Outline_INCLUDED
#define PDF_Outline_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Resource.h"
#include "Poco/PDF/Destination.h"


namespace Poco {
namespace PDF {


class PDF_API Outline: public Resource<HPDF_Outline>
	/// A Outline represents a PDF outline resource.
{
public:
	typedef HPDF_Outline Type;

	Outline(HPDF_Doc* pPDF, const HPDF_Outline& outline, const std::string& name = "");
		/// Creates the outline.

	Outline(const Outline& other);
		/// Copy creates the resource.

	~Outline();
		/// Destroys the outline.

	Outline& operator = (const Outline& resource);
		/// Assignment operator.

	void open();
		/// Opens the outline.

	void close();
		/// Closes the outline.

	void destination(const Destination& dest);
		/// Sets the destination for this outline.
};


//
// inlines
//

inline void Outline::open()
{
	HPDF_Outline_SetOpened(handle(), HPDF_TRUE);
}


inline void Outline::close()
{
	HPDF_Outline_SetOpened(handle(), HPDF_FALSE);
}


inline void Outline::destination(const Destination& dest)
{
	HPDF_Outline_SetDestination(handle(), dest);
}


} } // namespace Poco::PDF


#endif // PDF_Outline_INCLUDED
