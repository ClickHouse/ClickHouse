//
// ZipException.h
//
// Library: Zip
// Package: Zip
// Module:  ZipException
//
// Definition of the ZipException class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_ZipException_INCLUDED
#define Zip_ZipException_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Zip {


POCO_DECLARE_EXCEPTION(Zip_API, ZipException, Poco::RuntimeException)
POCO_DECLARE_EXCEPTION(Zip_API, ZipManipulationException, ZipException)


} } // namespace Poco::Zip


#endif // Zip_ZipException_INCLUDED
