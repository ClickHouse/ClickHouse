//
// DirectoryIterator.h
//
// Library: Foundation
// Package: Filesystem
// Module:  DirectoryIterator
//
// Definition of the DirectoryIterator class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_DirectoryIterator_INCLUDED
#define Foundation_DirectoryIterator_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/File.h"
#include "Poco/Path.h"


namespace Poco {


class DirectoryIteratorImpl;


class Foundation_API DirectoryIterator
	/// The DirectoryIterator class is used to enumerate
	/// all files in a directory.
	///
	/// DirectoryIterator has some limitations:
	///   * only forward iteration (++) is supported
	///   * an iterator copied from another one will always
	///     point to the same file as the original iterator,
	///     even is the original iterator has been advanced
	///     (all copies of an iterator share their state with
	///     the original iterator)
	///   * because of this you should only use the prefix 
	///     increment operator
{
public:
	DirectoryIterator();
		/// Creates the end iterator.
		
	DirectoryIterator(const std::string& path);
		/// Creates a directory iterator for the given path.

	DirectoryIterator(const DirectoryIterator& iterator);
		/// Creates a directory iterator for the given path.
		
	DirectoryIterator(const File& file);
		/// Creates a directory iterator for the given file.

	DirectoryIterator(const Path& path);
		/// Creates a directory iterator for the given path.

	virtual ~DirectoryIterator();
		/// Destroys the DirectoryIterator.

	const std::string& name() const;
		/// Returns the current filename.
		
	const Path& path() const;
		/// Returns the current path.

	DirectoryIterator& operator = (const DirectoryIterator& it);
	DirectoryIterator& operator = (const File& file);
	DirectoryIterator& operator = (const Path& path);
	DirectoryIterator& operator = (const std::string& path);
	
	virtual DirectoryIterator& operator ++ ();   // prefix
	
	//@ deprecated
	DirectoryIterator operator ++ (int); // postfix
		/// Please use the prefix increment operator instead.
	
	const File& operator * () const;
	File& operator * ();
	const File* operator -> () const;
	File* operator -> ();
	
	bool operator == (const DirectoryIterator& iterator) const;
	bool operator != (const DirectoryIterator& iterator) const;

protected:
	Path _path;
	File _file;

private:
	DirectoryIteratorImpl* _pImpl;
};


//
// inlines
//
inline const std::string& DirectoryIterator::name() const
{
	return _path.getFileName();
}

	
inline const Path& DirectoryIterator::path() const
{
	return _path;
}


inline const File& DirectoryIterator::operator * () const
{
	return _file;
}


inline File& DirectoryIterator::operator * ()
{
	return _file;
}


inline const File* DirectoryIterator::operator -> () const
{
	return &_file;
}


inline File* DirectoryIterator::operator -> ()
{
	return &_file;
}


inline bool DirectoryIterator::operator == (const DirectoryIterator& iterator) const
{
	return name() == iterator.name();
}


inline bool DirectoryIterator::operator != (const DirectoryIterator& iterator) const
{
	return name() != iterator.name();
}


} // namespace Poco


#endif // Foundation_DirectoryIterator_INCLUDED
