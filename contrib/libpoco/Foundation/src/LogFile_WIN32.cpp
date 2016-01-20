//
// LogFile_WIN32.cpp
//
// $Id: //poco/1.4/Foundation/src/LogFile_WIN32.cpp#1 $
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


#include "Poco/LogFile_WIN32.h"
#include "Poco/File.h"
#include "Poco/Exception.h"


namespace Poco {


LogFileImpl::LogFileImpl(const std::string& path): _path(path), _hFile(INVALID_HANDLE_VALUE)
{
	File file(path);
	if (file.exists())
	{
		if (0 == sizeImpl())
			_creationDate = file.getLastModified();
		else
			_creationDate = file.created();
	}
}


LogFileImpl::~LogFileImpl()
{
	CloseHandle(_hFile);
}


void LogFileImpl::writeImpl(const std::string& text, bool flush)
{
	if (INVALID_HANDLE_VALUE == _hFile)	createFile();

	DWORD bytesWritten;
	BOOL res = WriteFile(_hFile, text.data(), (DWORD) text.size(), &bytesWritten, NULL);
	if (!res) throw WriteFileException(_path);
	res = WriteFile(_hFile, "\r\n", 2, &bytesWritten, NULL);
	if (!res) throw WriteFileException(_path);
	if (flush)
	{
		res = FlushFileBuffers(_hFile);
		if (!res) throw WriteFileException(_path);
	}
}


UInt64 LogFileImpl::sizeImpl() const
{
	if (INVALID_HANDLE_VALUE == _hFile)
	{
		File file(_path);
		if (file.exists()) return file.getSize();
		else return 0;
	}

	LARGE_INTEGER li;
	li.HighPart = 0;
	li.LowPart  = SetFilePointer(_hFile, 0, &li.HighPart, FILE_CURRENT);
	return li.QuadPart;
}


Timestamp LogFileImpl::creationDateImpl() const
{
	return _creationDate;
}


const std::string& LogFileImpl::pathImpl() const
{
	return _path;
}


void LogFileImpl::createFile()
{
	_hFile = CreateFileA(_path.c_str(), GENERIC_WRITE, FILE_SHARE_READ, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
	if (_hFile == INVALID_HANDLE_VALUE) throw OpenFileException(_path);
	SetFilePointer(_hFile, 0, 0, FILE_END);
	// There seems to be a strange "optimization" in the Windows NTFS
	// filesystem that causes it to reuse directory entries of deleted
	// files. Example:
	// 1. create a file named "test.dat"
	//    note the file's creation date
	// 2. delete the file "test.dat"
	// 3. wait a few seconds
	// 4. create a file named "test.dat"
	//    the new file will have the same creation
	//    date as the old one.
	// We work around this bug by taking the file's
	// modification date as a reference when the
	// file is empty.
	if (sizeImpl() == 0)
		_creationDate = File(_path).getLastModified();
	else
		_creationDate = File(_path).created();
}

} // namespace Poco
