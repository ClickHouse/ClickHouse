//
// RecursiveDirectoryIteratorStategies.cpp
//
// $Id$
//
// Library: Foundation
// Package: Filesystem
// Module:  RecursiveDirectoryIterator
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DirectoryIteratorStrategy.h"


namespace Poco {


//
// TraverseBase
//
TraverseBase::TraverseBase(DepthFunPtr depthDeterminer, UInt16 maxDepth)
	: _depthDeterminer(depthDeterminer), _maxDepth(maxDepth)
{
}


inline bool TraverseBase::isFiniteDepth()
{
	return _maxDepth != D_INFINITE;
}


bool TraverseBase::isDirectory(Poco::File& file)
{
	try
	{
		return file.isDirectory();
	}
	catch (...)
	{
		return false;
	}
}


//
// ChildrenFirstTraverse
//
ChildrenFirstTraverse::ChildrenFirstTraverse(DepthFunPtr depthDeterminer, UInt16 maxDepth)
	: TraverseBase(depthDeterminer, maxDepth)
{
}


const std::string ChildrenFirstTraverse::next(Stack* itStack, bool* isFinished)
{
	// pointer mustn't point to NULL and iteration mustn't be finished
	poco_check_ptr(isFinished);
	poco_assert(!(*isFinished));

	std::stack<DirectoryIterator> it;

	//_depthDeterminer(it);

	// go deeper into not empty directory
	// (if depth limit allows)
	bool isDepthLimitReached = isFiniteDepth() && _depthDeterminer(*itStack) >= _maxDepth;
	if (!isDepthLimitReached && isDirectory(*itStack->top()))
	{
		DirectoryIterator child_it(itStack->top().path());
		// check if directory is empty
		if (child_it != _itEnd)
		{
			itStack->push(child_it);
			return child_it->path();
		}
	}

	++(itStack->top());

	poco_assert(!itStack->empty());
	// return up until there isn't right sibling
	while (itStack->top() == _itEnd)
	{
		itStack->pop();

		// detect end of traversal
		if (itStack->empty())
		{
			*isFinished = true;
			return _itEnd->path();
		}
		else
		{
			++(itStack->top());
		}
	}

	return itStack->top()->path();
}


//
// SiblingsFirstTraverse
//
SiblingsFirstTraverse::SiblingsFirstTraverse(DepthFunPtr depthDeterminer, UInt16 maxDepth)
	: TraverseBase(depthDeterminer, maxDepth)
{
	_dirsStack.push(std::queue<std::string>());
}


const std::string SiblingsFirstTraverse::next(Stack* itStack, bool* isFinished)
{
	// pointer mustn't point to NULL and iteration mustn't be finished
	poco_check_ptr(isFinished);
	poco_assert(!(*isFinished));

	// add dirs to queue (if depth limit allows)
	bool isDepthLimitReached = isFiniteDepth() && _depthDeterminer(*itStack) >= _maxDepth;
	if (!isDepthLimitReached && isDirectory(*itStack->top()))
	{
		const std::string& p = itStack->top()->path();
		_dirsStack.top().push(p);
	}

	++(itStack->top());

	poco_assert(!itStack->empty());
	// return up until there isn't right sibling
	while (itStack->top() == _itEnd)
	{
		// try to find first not empty directory and go deeper
		while (!_dirsStack.top().empty())
		{
			std::string dir = _dirsStack.top().front();
			_dirsStack.top().pop();
			DirectoryIterator child_it(dir);

			// check if directory is empty
			if (child_it != _itEnd)
			{
				itStack->push(child_it);
				_dirsStack.push(std::queue<std::string>());
				return child_it->path();
			}
		}

		// if fail go upper
		itStack->pop();
		_dirsStack.pop();

		// detect end of traversal
		if (itStack->empty())
		{
			*isFinished = true;
			return _itEnd->path();
		}
	}

	return itStack->top()->path();
}


} // namespace Poco
