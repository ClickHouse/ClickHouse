//
// PDFException.h
//
// Library: PDF
// Package: PDFCore
// Module:  PDFException
//
// Definition of the PDFException class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_PDFException_INCLUDED
#define PDF_PDFException_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/Exception.h"


namespace Poco {
namespace PDF {


void HPDF_Error_Handler(HPDF_STATUS error_no, HPDF_STATUS detail_no, void* user_data);
	/// HARU library error handler function.
	/// Throws appropriate exception.


POCO_DECLARE_EXCEPTION(PDF_API, PDFException, Poco::RuntimeException)
POCO_DECLARE_EXCEPTION(PDF_API, PDFCreateException, PDFException)


} } // namespace Poco::PDF


#endif // PDF_PDFException_INCLUDED
