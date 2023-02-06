//
// Destination.cpp
//
// Library: PDF
// Package: PDFCore
// Module:  Destination
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PDF/Destination.h"


namespace Poco {
namespace PDF {


Destination::Destination(HPDF_Doc* pPDF,
	const HPDF_Destination& destination,
	const std::string& name): 
	Resource<HPDF_Destination>(pPDF, destination, name)
{
}



Destination::~Destination()
{
}


} } // namespace Poco::PDF
