//
// TextAnnotation.cpp
//
// Library: PDF
// Package: PDFCore
// Module:  TextAnnotation
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PDF/TextAnnotation.h"
#include "Poco/PDF/PDFException.h"


namespace Poco {
namespace PDF {


TextAnnotation::TextAnnotation(HPDF_Doc* pPDF,
	const HPDF_Annotation& annotation,
	const std::string& name): 
	Resource<HPDF_Annotation>(pPDF, annotation, name)
{
	open();
}


TextAnnotation::~TextAnnotation()
{
	close();
}


} } // namespace Poco::PDF
