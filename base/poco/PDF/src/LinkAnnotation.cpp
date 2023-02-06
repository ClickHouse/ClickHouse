//
// LinkAnnotation.cpp
//
// Library: PDF
// Package: PDFCore
// Module:  LinkAnnotation
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PDF/LinkAnnotation.h"
#include "Poco/PDF/PDFException.h"


namespace Poco {
namespace PDF {


LinkAnnotation::LinkAnnotation(HPDF_Doc* pPDF,
	const HPDF_Annotation& annotation,
	const std::string& name): 
	Resource<HPDF_Annotation>(pPDF, annotation, name)
{
}


LinkAnnotation::~LinkAnnotation()
{
}


} } // namespace Poco::PDF
