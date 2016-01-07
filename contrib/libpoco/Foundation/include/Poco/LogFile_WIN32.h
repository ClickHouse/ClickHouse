//
// LogFile_WIN32.h
//
// $Id: //poco/1.4/Foundation/include/Poco/LogFile_WIN32.h#1 $
//
// Library: Foundation
// Package: Logging
// Module:  LogFile
//
// Definition of the LogFileImpl class using the Windows file APIs.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_LogFile_WIN32_INCLUDED
#define Foundation_LogFile_WIN32_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Timestamp.h"
#include "Poco/UnWindows.h"


namespace Poco {


class Foundation_API LogFileImpl
	/// The implementation of LogFile for Windows.
	/// The native filesystem APIs are used for
	/// total control over locking behavior.
{
public:
	LogFileImpl(const std::string& path);
	~LogFileImpl();
	void writeImpl(const std::string& text, bool flush);
	UInt64 sizeImpl() const;
	Timestamp creationDateImpl() const;
	const std::string& pathImpl() const;

private:
	void createFile();

	std::string _path;
	HANDLE      _hFile;
	Timestamp   _creationDate;
};


} // namespace Poco


#endif // Foundation_LogFile_WIN32_INCLUDED
