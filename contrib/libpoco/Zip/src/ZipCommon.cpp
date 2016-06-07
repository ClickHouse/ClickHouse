//
// ZipCommon.cpp
//
// $Id: //poco/1.4/Zip/src/ZipCommon.cpp#1 $
//
// Library: Zip
// Package: Zip
// Module:  ZipCommon
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/ZipCommon.h"


namespace Poco {
namespace Zip {


bool ZipCommon::isValidPath(const std::string& path)
{
	if (path == "..")
		return false;
	if (path.compare(0, 3, "../") == 0)
		return false;
	if (path.compare(0, 3, "..\\") == 0)
		return false;
	if (path.find("/..") != std::string::npos)
		return false;
	if (path.find("\\..") != std::string::npos)
		return false;
	return true;
}


} } // namespace Poco::Zip
