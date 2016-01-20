//
// SortedDirectoryIterator.cpp
//
// $Id$
//
// Library: Foundation
// Package: Filesystem
// Module:  DirectoryIterator
//
// Copyright (c) 2004-2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#include "Poco/SortedDirectoryIterator.h"
#include <algorithm>


namespace Poco {


SortedDirectoryIterator::SortedDirectoryIterator()
	: DirectoryIterator(), _is_finished(true)
{
}


SortedDirectoryIterator::SortedDirectoryIterator(const std::string& path)
	: DirectoryIterator(path), _is_finished(false)
{
	scan();
	next();
}


SortedDirectoryIterator::SortedDirectoryIterator(const DirectoryIterator& iterator)
	: DirectoryIterator(iterator), _is_finished(false)
{
	scan();
	next();
}


SortedDirectoryIterator::SortedDirectoryIterator(const File& file)
	: DirectoryIterator(file), _is_finished(false)
{
	scan();
	next();
}


SortedDirectoryIterator::SortedDirectoryIterator(const Path& path)
	: DirectoryIterator(path), _is_finished(false)
{
	scan();
	next();
}


SortedDirectoryIterator::~SortedDirectoryIterator()
{
}


SortedDirectoryIterator& SortedDirectoryIterator::operator ++()
{
	if (!_is_finished)
	{
		next();
	}
	return *this;
}


void SortedDirectoryIterator::scan()
{
	DirectoryIterator end_it;
	while (*this != end_it)
	{
		if ((*this)->isDirectory())
			_directories.push_back(_path.toString());
		else
			_files.push_back(_path.toString());

		DirectoryIterator::operator++();
	}

	std::sort(_directories.begin(), _directories.end());
	std::sort(_files.begin(), _files.end());
}


void SortedDirectoryIterator::next()
{
	DirectoryIterator end_it;
	if (!_directories.empty())
	{
		_path.assign(_directories.front());
		_directories.pop_front();
		_file = _path;
	}
	else if (!_files.empty())
	{
		_path.assign(_files.front());
		_files.pop_front();
		_file = _path;
	}
	else
	{
		_is_finished = true;
		_path = end_it.path();
		_file = _path;
	}
}


} // namespace Poco
