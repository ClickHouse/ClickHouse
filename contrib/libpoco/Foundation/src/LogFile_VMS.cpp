//
// LogFile_VMS.cpp
//
// $Id: //poco/1.4/Foundation/src/LogFile_VMS.cpp#1 $
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


#include "Poco/LogFile_VMS.h"
#include "Poco/File.h"
#include "Poco/Exception.h"


namespace Poco {


LogFileImpl::LogFileImpl(const std::string& path): _path(path)
{
	_file = fopen(path.c_str(), "a", "ctx=rec,bin", "shr=get", "fop=dfw", "alq=500", "deq=500");
	if (!_file) throw OpenFileException(path);
	if (size() == 0)
		_creationDate = File(path).getLastModified();
	else
		_creationDate = File(path).created();
}


LogFileImpl::~LogFileImpl()
{
	fclose(_file);
}


void LogFileImpl::writeImpl(const std::string& text, bool flush)
{
	int rc = fputs(text.c_str(), _file);
	if (rc == EOF) throw WriteFileException(_path);
	rc = fputc('\n', _file);
	if (rc == EOF) throw WriteFileException(_path);
	if (flush)
	{
		rc = fflush(_file);
		if (rc == EOF) throw WriteFileException(_path);
	}
}


UInt64 LogFileImpl::sizeImpl() const
{
	return (UInt64) ftell(_file);
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
