//
// LogFile_STD.cpp
//
// $Id: //poco/1.4/Foundation/src/LogFile_STD.cpp#1 $
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


#include "Poco/LogFile_STD.h"
#include "Poco/File.h"
#include "Poco/Exception.h"


namespace Poco {


LogFileImpl::LogFileImpl(const std::string& path): 
	_path(path),
	_str(_path, std::ios::app)
{
	if (sizeImpl() == 0)
		_creationDate = File(path).getLastModified();
	else
		_creationDate = File(path).created();
}


LogFileImpl::~LogFileImpl()
{
}


void LogFileImpl::writeImpl(const std::string& text, bool flush)
{
	_str << text;
	if (flush)
		_str << std::endl;
	else
		_str << "\n";
	if (!_str.good()) throw WriteFileException(_path);
}


UInt64 LogFileImpl::sizeImpl() const
{
	return (UInt64) _str.tellp();
}


Timestamp LogFileImpl::creationDateImpl() const
{
	return _creationDate;
}


const std::string& LogFileImpl::pathImpl() const
{
	return _path;
}


} // namespace Poco
