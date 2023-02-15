//
// PartStore.h
//
// Library: Net
// Package: Messages
// Module:  PartStore
//
// Definition of the PartStore class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_PartStore_INCLUDED
#define Net_PartStore_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/PartSource.h"
#include "Poco/FileStream.h"


namespace Poco {
namespace Net {


class Net_API PartStore: public PartSource
	/// A parent class for part stores storing message parts.
{
public:
	PartStore(const std::string& mediaType);
		/// Creates the PartStore for the given MIME type.

	~PartStore();
		/// Destroys the PartFileStore.

private:
	PartStore();
};


class Net_API FilePartStore: public PartStore
	/// An implementation of PartSource for persisting
	/// parts (usually email attachment files) to the file system.
{
public:
	FilePartStore(const std::string& content, const std::string& mediaType, const std::string& filename = "");
		/// Creates the FilePartStore for the given MIME type.
		/// For security purposes, attachment filename is NOT used to save file to the file system.
		/// A unique temporary file name is used to persist the file.
		/// The given filename parameter is the message part (attachment) filename (see filename()) only.
		///
		/// Throws an exception if the file cannot be opened.

	~FilePartStore();
		/// Destroys the FilePartStore.

	std::istream& stream();
		/// Returns a file input stream for the given file.

	const std::string& filename() const;
		/// Returns the filename portion of the path.
		/// This is the name under which the file is known
		/// to the user of this class (typically, MailMessage
		/// class). The real name of the file as saved
		/// to the filesystem can be obtained by calling
		/// path() member function.

	const std::string& path() const;
		/// Returns the full path to the file as saved
		/// to the file system. For security reasons,
		/// file is not saved under the real file name
		/// (as specified by the user).

private:
	std::string      _filename;
	std::string      _path;
	Poco::FileStream _fstr;
};


class PartStoreFactory
	/// Parent factory class for part stores creation.
{
public:
	virtual PartSource* createPartStore(const std::string& content, const std::string& mediaType, const std::string& filename = "") = 0;
};


class FilePartStoreFactory: public PartStoreFactory
{
public:
	PartSource* createPartStore(const std::string& content, const std::string& mediaType, const std::string& filename = "")
	{
		return new FilePartStore(content, mediaType, filename);
	}
};


} } // namespace Poco::Net


#endif // Net_PartStore_INCLUDED
