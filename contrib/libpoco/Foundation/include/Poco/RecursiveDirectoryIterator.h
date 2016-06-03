//
// RecursiveDirectoryIterator.h
//
// $Id$
//
// Library: Foundation
// Package: Filesystem
// Module:  RecursiveDirectoryIterator
//
// Definition of the RecursiveDirectoryIterator class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_RecursiveDirectoryIterator_INCLUDED
#define Foundation_RecursiveDirectoryIterator_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/RecursiveDirectoryIteratorImpl.h"
#include "Poco/DirectoryIteratorStrategy.h"


namespace Poco {


class DirectoryIterator;


template<class TTravStr>
class RecursiveDirectoryIteratorImpl;


template<class TTravStr = ChildrenFirstTraverse>
class RecursiveDirectoryIterator
	/// The RecursiveDirectoryIterator class is used to enumerate
	/// all files in a directory and its subdirectories.
	///
	/// RecursiveDirectoryIterator has some limitations:
	///   * only forward iteration (++) is supported
	///   * an iterator copied from another one will always
	///     point to the same file as the original iterator,
	///     even is the original iterator has been advanced
	///     (all copies of an iterator share their state with
	///     the original iterator)
	///
	/// The class can follow different traversal strategies:
	///     * depth-first strategy;
	///     * siblings-first strategy.
	/// The stategies are set by template parameter.
	/// There are two corresponding typedefs:
	///     * SimpleRecursiveDirectoryIterator;
	///     * SiblingsFirstRecursiveDirectoryIterator.
	///
	/// The depth of traversal can be limited by constructor
	/// parameter maxDepth (which sets the infinite depth by default).
{
public:
	typedef RecursiveDirectoryIterator<TTravStr> MyType;

	enum
	{
		D_INFINITE = 0 /// Constant for infinite traverse depth.
	};

	RecursiveDirectoryIterator()
		/// Creates the end iterator.
		: _pImpl(0)
	{
	}

	RecursiveDirectoryIterator(const std::string& path, UInt16 maxDepth = D_INFINITE)
		/// Creates a recursive directory iterator for the given path.
		: _pImpl(new ImplType(path, maxDepth)), _path(Path(_pImpl->get())), _file(_path)
	{
	}

	RecursiveDirectoryIterator(const MyType& iterator):
		/// Creates a copy of another recursive directory iterator.
		_pImpl(iterator._pImpl), _path(iterator._path), _file(iterator._file)
	{
	}

	RecursiveDirectoryIterator(const DirectoryIterator& iterator, UInt16 maxDepth = D_INFINITE):
		/// Creates a recursive directory iterator for the path of
		/// non-recursive directory iterator.
		_pImpl(new ImplType(iterator->path(), maxDepth)), _path(Path(_pImpl->get())), _file(_path)
	{
	}

	RecursiveDirectoryIterator(const File& file, UInt16 maxDepth = D_INFINITE):
		/// Creates a recursive directory iterator for the given path.
		_pImpl(new ImplType(file.path(), maxDepth)), _path(Path(_pImpl->get())), _file(_path)
	{
	}

	RecursiveDirectoryIterator(const Path& path, UInt16 maxDepth = D_INFINITE):
		/// Creates a recursive directory iterator for the given path.
		_pImpl(new ImplType(path.toString(), maxDepth)), _path(Path(_pImpl->get())), _file(_path)
	{
	}

	~RecursiveDirectoryIterator()
		/// Destroys the DirectoryIterator.
	{
		if (_pImpl)
			_pImpl->release();
	}

	const std::string& name() const
		/// Returns the current filename.
	{
		return _path.getFileName();
	}

	const Poco::Path& path() const
		/// Returns the current path.
	{
		return _path;
	}

	UInt16 depth() const
		/// Depth of recursion (counting from 1).
	{
		return _pImpl->depth();
	}

	UInt16 maxDepth() const
		/// Max depth of recursion (counting from 1).
	{
		return _pImpl->maxDepth();
	}


	MyType& operator = (const MyType& it)
	{
		if (_pImpl)
			_pImpl->release();
		_pImpl = it._pImpl;
		if (_pImpl)
		{
			_pImpl->duplicate();
			_path = it._path;
			_file = _path;
		}
		return *this;
	}

	MyType& operator = (const File& file)
	{
		if (_pImpl)
			_pImpl->release();
		_pImpl = new ImplType(file.path());
		_path = Path(_pImpl->get());
		_file = _path;
		return *this;
	}


	MyType& operator = (const Path& path)
	{
		if (_pImpl)
			_pImpl->release();
		_pImpl = new ImplType(path.toString());
		_path = Path(_pImpl->get());
		_file = _path;
		return *this;
	}

	MyType& operator = (const std::string& path)
	{
		if (_pImpl)
			_pImpl->release();
		_pImpl = new ImplType(path);
		_path = Path(_pImpl->get());
		_file = _path;
		return *this;
	}

	MyType& operator ++ ()
	{
		if (_pImpl)
		{
			_path = Path(_pImpl->next());
			_file = _path;
		}
		return *this;
	}

	const File& operator * () const
	{
		return _file;
	}

	File& operator *()
	{
		return _file;
	}

	const File* operator -> () const
	{
		return &_file;
	}

	File* operator -> ()
	{
		return &_file;
	}

	template<class T1, class T2>
	friend inline bool operator ==(const RecursiveDirectoryIterator<T1>& a, const RecursiveDirectoryIterator<T2>& b);
	template<class T1, class T2>
	friend inline bool operator !=(const RecursiveDirectoryIterator<T1>& a, const RecursiveDirectoryIterator<T2>& b);

private:
	typedef RecursiveDirectoryIteratorImpl<TTravStr> ImplType;

	ImplType* _pImpl;
	Path _path;
	File _file;
};


//
// friend comparsion operators
//
template<class T1, class T2>
inline bool operator ==(const RecursiveDirectoryIterator<T1>& a, const RecursiveDirectoryIterator<T2>& b)
{
	return a.path().toString() == b.path().toString();;
}

template<class T1, class T2>
inline bool operator !=(const RecursiveDirectoryIterator<T1>& a, const RecursiveDirectoryIterator<T2>& b)
{
	return a.path().toString() != b.path().toString();;
}


//
// typedefs
//
typedef RecursiveDirectoryIterator<ChildrenFirstTraverse> SimpleRecursiveDirectoryIterator;
typedef RecursiveDirectoryIterator<SiblingsFirstTraverse> SiblingsFirstRecursiveDirectoryIterator;


} // namespace Poco


#endif // Foundation_RecursiveDirectoryIterator_INCLUDED
