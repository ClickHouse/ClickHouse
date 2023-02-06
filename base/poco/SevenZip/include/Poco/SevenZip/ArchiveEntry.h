//
// ArchiveEntry.h
//
// Library: SevenZip
// Package: Archive
// Module:  ArchiveEntry
//
// Definition of the ArchiveEntry class.
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SevenZip_ArchiveEntry_INCLUDED
#define SevenZip_ArchiveEntry_INCLUDED


#include "Poco/SevenZip/SevenZip.h"
#include "Poco/Timestamp.h"


namespace Poco {
namespace SevenZip {


class SevenZip_API ArchiveEntry
	/// This class represents an entry in 7-Zip archive. The entry can
	/// be a file or a directory, with a path, modification date/time,
	/// original size and file attributes.
	///
	/// File attributes are based on those supported on Windows and are
	/// not of much use on non-Windows platforms.
{
public:
	enum EntryType
	{
		ENTRY_FILE      = 0,   /// The entry represents an ordinary file.
		ENTRY_DIRECTORY = 1    /// The entry represents a directory.
	};
	
	enum EntryAttributes
	{
		ATTR_READONLY  = 0x01, /// The file or directory is read-only.
		ATTR_HIDDEN    = 0x02, /// The file or directory is hidden.
		ATTR_SYSTEM    = 0x04, /// The file or directory is used by the operating system.
		ATTR_DIRECTORY = 0x10, /// The entry is a directory.
		ATTR_ARCHIVE   = 0x20  /// The file or directory is marked for backup or removal.
	};

	ArchiveEntry();
		/// Creates an empty ArchiveEntry;

	ArchiveEntry(const ArchiveEntry& entry);
		/// Creates an ArchiveEntry by copying another one.
		
	~ArchiveEntry();
		/// Destroys the ArchiveEntry.
		
	ArchiveEntry& operator = (const ArchiveEntry& entry);
		/// Assignment operator.
	
	void swap(ArchiveEntry& entry);
		/// Swaps the entry with another one.
	
	EntryType type() const;
		/// Returns the type of the entry.	
		
	const std::string& path() const;
		/// Return the UTF-8 encoded path.
		
	Poco::UInt64 size() const;
		/// Returns the original size of the file.
		
	Poco::Timestamp lastModified() const;
		/// Returns the date and time of last modification.

	Poco::UInt32 attributes() const;
		/// Returns the entry attributes as a bitmask.
		///
		/// See the EntryAttributes enumeration for valid values.
		
	bool isFile() const;
		/// Returns true iff the entry represents a file.
		
	bool isDirectory() const;
		/// Returns true iff the entry represents a directory.

protected:
	ArchiveEntry(EntryType type, const std::string& path, Poco::UInt64 size, Poco::Timestamp lastModified, Poco::UInt32 attributes, Poco::UInt32 index);
		/// Creates an ArchiveEntry.

	Poco::UInt32 index() const;
		/// Returns the index of the entry within the archive.

private:
	EntryType _type;
	std::string _path;
	Poco::UInt64 _size;
	Poco::Timestamp _lastModified;
	Poco::UInt32 _attributes;
	Poco::UInt32 _index;
	
	friend class ArchiveImpl;
};


//
// inlines
//
inline ArchiveEntry::EntryType ArchiveEntry::type() const
{
	return _type;
}


inline const std::string& ArchiveEntry::path() const
{
	return _path;
}

	
inline Poco::UInt64 ArchiveEntry::size() const
{
	return _size;
}

	
inline Poco::Timestamp ArchiveEntry::lastModified() const
{
	return _lastModified;
}


inline Poco::UInt32 ArchiveEntry::attributes() const
{
	return _attributes;
}


inline Poco::UInt32 ArchiveEntry::index() const
{
	return _index;
}


inline bool ArchiveEntry::isFile() const
{
	return _type == ENTRY_FILE;
}


inline bool ArchiveEntry::isDirectory() const
{
	return _type == ENTRY_DIRECTORY;
}


} } // namespace Poco::SevenZip


#endif // SevenZip_ArchiveEntry_INCLUDED
