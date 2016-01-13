//
// LogFile.cpp
//
// $Id: //poco/1.4/Foundation/src/LogFile.cpp#1 $
//
// Library: Foundation
// Package: Logging
// Module:  LogFile
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/LogFile.h"


#if defined(POCO_OS_FAMILY_WINDOWS) && defined(POCO_WIN32_UTF8)
#include "LogFile_WIN32U.cpp"
#elif defined(POCO_OS_FAMILY_WINDOWS)
#include "LogFile_WIN32.cpp"
#elif defined(POCO_OS_FAMILY_VMS)
#include "LogFile_VMS.cpp"
#else
#include "LogFile_STD.cpp"
#endif


namespace Poco {


LogFile::LogFile(const std::string& path): LogFileImpl(path)
{
}


LogFile::~LogFile()
{
}


} // namespace Poco
