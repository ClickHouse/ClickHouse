//
// ZipManipulator.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/ZipManipulator.h#1 $
//
// Library: Zip
// Package: Manipulation
// Module:  ZipManipulator
//
// Definition of the ZipManipulator class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_ZipManipulator_INCLUDED
#define Zip_ZipManipulator_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ZipArchive.h"
#include "Poco/Zip/ZipCommon.h"
#include "Poco/Zip/ZipOperation.h"
#include "Poco/FIFOEvent.h"
#include "Poco/SharedPtr.h"
#include <map>


namespace Poco {
namespace Zip {


class ZipArchive;


class Zip_API ZipManipulator
	/// ZipManipulator allows to add/remove/update files inside zip files
{
public:
	Poco::FIFOEvent<const ZipLocalFileHeader> EDone;
		// Fired for each entry once commit is invoked

	ZipManipulator(const std::string& zipFile, bool backupOriginalFile);
		/// Creates the ZipManipulator.

	virtual ~ZipManipulator();
		/// Destroys the ZipManipulator.

	void deleteFile(const std::string& zipPath);
		/// Removes the given file from the Zip archive.

	void replaceFile(const std::string& zipPath, const std::string& localPath);
		/// Replaces the contents of the file in the archive with the contents
		/// from the file given by localPath.

	void renameFile(const std::string& zipPath, const std::string& newZipPath);
		/// Renames the file in the archive to newZipPath

	void addFile(const std::string& zipPath, const std::string& localPath, ZipCommon::CompressionMethod cm = ZipCommon::CM_DEFLATE, ZipCommon::CompressionLevel cl = ZipCommon::CL_MAXIMUM);
		/// Adds a file to the zip file.

	ZipArchive commit();
		/// Commits all changes and re-creates the Zip File with the changes applied. 
		/// Returns the ZipArchive for the newly created archive
		///
		/// Changes will be first written to a temporary file, 
		/// then the originalfile will be either deleted or renamed to .bak,
		/// then, the temp file will be renamed to the original zip file name.

	const ZipArchive& originalArchive() const;
		/// Returns the original archive information

private:
	const ZipLocalFileHeader& getForChange(const std::string& zipPath) const;
		/// Searches for the entry given by the zipPath.
		/// Throws an exception if the entry does not exist
		/// or if an entry already exists in the Changeslist

	void addOperation(const std::string& zipPath, ZipOperation::Ptr ptrOp);
		/// Adds the operation to the changes list. Throws an exception if an
		/// entry for the zipPath already exists

	void onEDone(const void* pSender, const ZipLocalFileHeader& hdr);
		/// Forwards the event to the EDone event

	ZipArchive compress(const std::string& outFile);
		/// Compresses the new file to outFile

private:
	typedef std::map<std::string, ZipOperation::Ptr> Changes;

	const std::string _zipFile;
	bool              _backupOriginalFile;
	Changes           _changes;
	Poco::SharedPtr<ZipArchive>        _in;
};


inline const ZipArchive& ZipManipulator::originalArchive() const
{
	return *_in;
}


} } // namespace Poco::Zip


#endif // Zip_ZipManipulator_INCLUDED
