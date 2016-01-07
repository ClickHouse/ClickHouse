//
// Path_VMS.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Path_VMS.h#1 $
//
// Library: Foundation
// Package: Filesystem
// Module:  Path
//
// Definition of the PathImpl class for OpenVMS.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Path_VMS_INCLUDED
#define Foundation_Path_VMS_INCLUDED


#include "Poco/Foundation.h"
#include <vector>


namespace Poco {


class PathImpl
{
public:
	static std::string currentImpl();
	static std::string homeImpl();
	static std::string tempImpl();
	static std::string nullImpl();
	static std::string expandImpl(const std::string& path);
	static void listRootsImpl(std::vector<std::string>& roots);
};


} // namespace Poco


#endif // Foundation_Path_VMS_INCLUDED
