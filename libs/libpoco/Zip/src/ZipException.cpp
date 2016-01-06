//
// ZipException.cpp
//
// $Id: //poco/1.4/Zip/src/ZipException.cpp#1 $
//
// Library: Zip
// Package: Zip
// Module:  ZipException
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/ZipException.h"
#include <typeinfo>


namespace Poco {
namespace Zip {


POCO_IMPLEMENT_EXCEPTION(ZipException, Poco::RuntimeException, "ZIP Exception")
POCO_IMPLEMENT_EXCEPTION(ZipManipulationException, ZipException, "ZIP Manipulation Exception")


} } // namespace Poco::Zip
