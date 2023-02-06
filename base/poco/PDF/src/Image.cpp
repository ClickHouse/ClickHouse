//
// Image.cpp
//
// Library: PDF
// Package: PDFCore
// Module:  Image
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PDF/Image.h"


namespace Poco {
namespace PDF {


Image::Image(HPDF_Doc* pPDF, const HPDF_Image& resource, const std::string& name): 
	Resource<HPDF_Image>(pPDF, resource, name)
{
}



Image::~Image()
{
}


} } // namespace Poco::PDF
