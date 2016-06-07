//
// RecursiveDirectoryIteratorImpl.h
//
// $Id$
//
// Library: Foundation
// Package: Filesystem
// Module:  RecursiveDirectoryIterator
//
// Definition of the RecursiveDirectoryIteratorImpl class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_RecursiveDirectoryIteratorImpl_INCLUDED
#define Foundation_RecursiveDirectoryIteratorImpl_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/DirectoryIteratorStrategy.h"
#include <stack>
#include <functional>


namespace Poco {


class ChildrenFirstTraverse;
class SiblingsFirstTraverse;


template<class TTraverseStrategy = ChildrenFirstTraverse>
class RecursiveDirectoryIteratorImpl
{
public:
	enum
	{
		D_INFINITE = 0 /// Special value for infinite traverse depth.
	};

	RecursiveDirectoryIteratorImpl(const std::string& path, UInt16 maxDepth = D_INFINITE)
		: _maxDepth(maxDepth), _traverseStrategy(std::ptr_fun(depthFun), _maxDepth), _isFinished(false), _rc(1)
	{
		_itStack.push(DirectoryIterator(path));
		_current = _itStack.top()->path();
	}

	~RecursiveDirectoryIteratorImpl()
	{
	}

	inline void duplicate()
	{
		++_rc;
	}

	inline void release()
	{
		if (--_rc == 0)
			delete this;
	}

	inline UInt16 depth() const
	{
		return depthFun(_itStack);
	}

	inline UInt16 maxDepth() const
	{
		return _maxDepth;
	}

	inline const std::string& get() const
	{
		return _current;
	}
	const std::string& next()
	{
		if (_isFinished)
			return _current;

		_current = _traverseStrategy.next(&_itStack, &_isFinished);

		return _current;
	}

private:
	typedef std::stack<DirectoryIterator> Stack;

	static UInt16 depthFun(const Stack& stack)
		/// Function which implements the logic of determining
		/// recursion depth.
	{
		return static_cast<Poco::UInt16>(stack.size());
	}

	UInt16 _maxDepth;
	TTraverseStrategy _traverseStrategy;
	bool _isFinished;
	Stack _itStack;
	std::string _current;
	int _rc;
};


} // namespace Poco


#endif // Foundation_RecursiveDirectoryIteratorImpl_INCLUDED
