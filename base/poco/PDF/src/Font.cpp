//
// Font.cpp
//
// Library: PDF
// Package: PDFCore
// Module:  Font
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PDF/Font.h"


namespace Poco {
namespace PDF {


Font::Font(HPDF_Doc* pPDF, const HPDF_Font& font): 
	Resource<HPDF_Font>(pPDF, font, HPDF_Font_GetFontName(font))
{
}



Font::~Font()
{
}


} } // namespace Poco::PDF
