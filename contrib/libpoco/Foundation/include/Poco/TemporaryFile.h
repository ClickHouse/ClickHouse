//
// TemporaryFile.h
//
// $Id: //poco/1.4/Foundation/include/Poco/TemporaryFile.h#2 $
//
// Library: Foundation
// Package: Filesystem
// Module:  TemporaryFile
//
// Definition of the TemporaryFile class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_TemporaryFile_INCLUDED
#define Foundation_TemporaryFile_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/File.h"


namespace Poco {


class Foundation_API TemporaryFile: public File
	/// The TemporaryFile class helps with the handling
	/// of temporary files.
	/// A unique name for the temporary file is
	/// automatically chosen and the file is placed
	/// in the directory reserved for temporary
	/// files (see Path::temp()).
	/// Obtain the path by calling the path() method
	/// (inherited from File).
	///
	/// The TemporaryFile class does not actually
	/// create the file - this is up to the application.
	/// The class does, however, delete the temporary
	/// file - either in the destructor, or immediately
	/// before the application terminates.
{
public:
	TemporaryFile();
		/// Creates the TemporaryFile.

	TemporaryFile(const std::string& tempDir);
		/// Creates the TemporaryFile using the given directory.

	~TemporaryFile();
		/// Destroys the TemporaryFile and
		/// deletes the corresponding file on
		/// disk unless keep() or keepUntilExit()
		/// has been called.

	void keep();
		/// Disables automatic deletion of the file in
		/// the destructor.

	void keepUntilExit();
		/// Disables automatic deletion of the file in
		/// the destructor, but registers the file
		/// for deletion at process termination.

	static void registerForDeletion(const std::string& path);
		/// Registers the given file for deletion
		/// at process termination.

	static std::string tempName(const std::string& tempDir = "");
		/// Returns a unique path name for a temporary
		/// file in the system's scratch directory (see Path::temp())
		/// if tempDir is empty or in the directory specified in tempDir
		/// otherwise.

private:
	bool _keep;
};


} // namespace Poco


#endif // Foundation_TemporaryFile_INCLUDED
