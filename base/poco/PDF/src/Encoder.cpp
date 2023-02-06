//
// Encoder.cpp
//
// Library: PDF
// Package: PDFCore
// Module:  Encoder
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PDF/Encoder.h"


namespace Poco {
namespace PDF {


Encoder::Encoder(HPDF_Doc* pPDF,
	const HPDF_Encoder& encoder,
	const std::string& name): 
	Resource<HPDF_Encoder>(pPDF, encoder, name)
{
}



Encoder::~Encoder()
{
}


} } // namespace Poco::PDF
