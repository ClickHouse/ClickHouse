//
// FilePartSource.h
//
// Library: Net
// Package: Messages
// Module:  FilePartSource
//
// Definition of the FilePartSource class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_FilePartSource_INCLUDED
#define Net_FilePartSource_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/PartSource.h"
#include "Poco/FileStream.h"


namespace Poco {
namespace Net {


class Net_API FilePartSource: public PartSource
	/// An implementation of PartSource for
	/// plain files.
{
public:
	FilePartSource(const std::string& path);
		/// Creates the FilePartSource for the given path.
		///
		/// The MIME type is set to application/octet-stream.
		///
		/// Throws an OpenFileException if the file cannot be opened.
	
	FilePartSource(const std::string& path, const std::string& mediaType);
		/// Creates the FilePartSource for the given
		/// path and MIME type.
		///
		/// Throws an OpenFileException if the file cannot be opened.

	FilePartSource(const std::string& path, const std::string& filename, const std::string& mediaType);
		/// Creates the FilePartSource for the given
		/// path and MIME type. The given filename is 
		/// used as part filename (see filename()) only.
		///
		/// Throws an OpenFileException if the file cannot be opened.

	~FilePartSource();
		/// Destroys the FilePartSource.

	std::istream& stream();
		/// Returns a file input stream for the given file.
		
	const std::string& filename() const;
		/// Returns the filename portion of the path.

	std::streamsize getContentLength() const;
		/// Returns the file size.

private:
	std::string _path;
	std::string _filename;
	Poco::FileInputStream _istr;
};


} } // namespace Poco::Net


#endif // Net_FilePartSource_INCLUDED
