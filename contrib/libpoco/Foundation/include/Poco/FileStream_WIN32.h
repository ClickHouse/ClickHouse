//
// FileStream_WIN32.h
//
// $Id: //poco/1.4/Foundation/include/Poco/FileStream_WIN32.h#1 $
//
// Library: Foundation
// Package: Streams
// Module:  FileStream
//
// Definition of the FileStreamBuf, FileInputStream and FileOutputStream classes.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_FileStream_WIN32_INCLUDED
#define Foundation_FileStream_WIN32_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/BufferedBidirectionalStreamBuf.h"
#include "Poco/UnWindows.h"


namespace Poco {


class Foundation_API FileStreamBuf: public BufferedBidirectionalStreamBuf
	/// This stream buffer handles Fileio
{
public:
	FileStreamBuf();
		/// Creates a FileStreamBuf.

	~FileStreamBuf();
		/// Destroys the FileStream.

	void open(const std::string& path, std::ios::openmode mode);
		/// Opens the given file in the given mode.

	bool close();
		/// Closes the File stream buffer. Returns true if successful,
		/// false otherwise.

	std::streampos seekoff(std::streamoff off, std::ios::seekdir dir, std::ios::openmode mode = std::ios::in | std::ios::out);
		/// change position by offset, according to way and mode

	std::streampos seekpos(std::streampos pos, std::ios::openmode mode = std::ios::in | std::ios::out);
		/// change to specified position, according to mode

protected:
	enum
	{
		BUFFER_SIZE = 4096
	};

	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);

private:
	std::string _path;
	HANDLE _handle;
	UInt64 _pos;
};


} // namespace Poco


#endif // Foundation_FileStream_WIN32_INCLUDED
