//
// Archive.h
//
// Library: SevenZip
// Package: Archive
// Module:  Archive
//
// Definition of the Archive class.
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SevenZip_Archive_INCLUDED
#define SevenZip_Archive_INCLUDED


#include "Poco/SevenZip/SevenZip.h"
#include "Poco/SevenZip/ArchiveEntry.h"
#include "Poco/BasicEvent.h"
#include <vector>
#include <utility>


namespace Poco {
namespace SevenZip {


class ArchiveImpl;


class SevenZip_API Archive
	/// This class represents a 7-Zip archive. 
	///
	/// The Archive class can be used to enumerate entries in a
	/// 7-Zip archive, and to extract files or directories from
	/// an archive.
{
public:
	typedef std::vector<ArchiveEntry> EntryVec;
	typedef EntryVec::iterator Iterator;
	typedef EntryVec::const_iterator ConstIterator;
	
	struct ExtractedEventArgs
	{
		ArchiveEntry entry;
		std::string extractedPath;
	};
	
	struct FailedEventArgs
	{
		ArchiveEntry entry;
		Poco::Exception* pException;
	};
		
	Poco::BasicEvent<const ExtractedEventArgs> extracted;
		/// Fired when an archive entry has been successfully extracted.
		
	Poco::BasicEvent<const FailedEventArgs> failed;
		/// Fired when extracting an archive entry failed.

	Archive(const std::string& path);
		/// Creates an Archive object for the 7-Zip archive
		/// with the given path. 
		
	~Archive();
		/// Destroys the Archive.
	
	const std::string& path() const;
		/// Returns the path of the archive in the filesystem.
		
	std::size_t size() const;
		/// Returns the number of entries in the archive.
	
	ConstIterator begin() const;
		/// Returns an iterator for iterating over the
		/// file or directory entries in the archive.
		
	ConstIterator end() const;
		/// Returns the end iterator.

	void extract(const std::string& destPath);
		/// Extracts the entire archive to the given path.
		///
		/// Directories will be created as necessary. File attributes
		/// will not be restored.
		///
		/// Progress and errors for single entries will be reported
		/// via the extracted and failed events.
		
	std::string extract(const ArchiveEntry& entry, const std::string& destPath);
		/// Extracts a specific entry to the given path.
		/// 
		/// Directories will be created as necessary. File attributes
		/// will not be restored.
		///
		/// Returns the absolute path of the extracted entry.

private:
	Archive();
	Archive(const Archive&);
	Archive& operator = (const Archive&);

	ArchiveImpl* _pImpl;
};


//
// inlines
//


} } // namespace Poco::SevenZip


#endif // SevenZip_ArchiveEntry_INCLUDED
