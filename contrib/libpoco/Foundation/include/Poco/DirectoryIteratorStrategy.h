//
// RecursiveDirectoryIteratorStategies.h
//
// $Id$
//
// Library: Foundation
// Package: Filesystem
// Module:  RecursiveDirectoryIterator
//
// Definitions of the RecursiveDirectoryIterator stategy classes.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_RecursiveDirectoryIteratorStrategy_INCLUDED
#define Foundation_RecursiveDirectoryIteratorStrategy_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/DirectoryIterator.h"
#include <stack>
#include <queue>
#include <functional>


namespace Poco {


class Foundation_API TraverseBase
{
public:
	typedef std::stack<DirectoryIterator> Stack;
	typedef std::pointer_to_unary_function<const Stack&, UInt16> DepthFunPtr;

	enum
	{
		D_INFINITE = 0 /// Special value for infinite traverse depth.
	};

	TraverseBase(DepthFunPtr depthDeterminer, UInt16 maxDepth = D_INFINITE);

protected:
	bool isFiniteDepth();
	bool isDirectory(Poco::File& file);

	DepthFunPtr _depthDeterminer;
	UInt16 _maxDepth;
	DirectoryIterator _itEnd;

private:
	TraverseBase();
	TraverseBase(const TraverseBase&);
	TraverseBase& operator=(const TraverseBase&);
};


class Foundation_API ChildrenFirstTraverse: public TraverseBase
{
public:
	ChildrenFirstTraverse(DepthFunPtr depthDeterminer, UInt16 maxDepth = D_INFINITE);

	const std::string next(Stack* itStack, bool* isFinished);

private:
	ChildrenFirstTraverse();
	ChildrenFirstTraverse(const ChildrenFirstTraverse&);
	ChildrenFirstTraverse& operator=(const ChildrenFirstTraverse&);
};


class Foundation_API SiblingsFirstTraverse: public TraverseBase
{
public:
	SiblingsFirstTraverse(DepthFunPtr depthDeterminer, UInt16 maxDepth = D_INFINITE);

	const std::string next(Stack* itStack, bool* isFinished);

private:
	SiblingsFirstTraverse();
	SiblingsFirstTraverse(const SiblingsFirstTraverse&);
	SiblingsFirstTraverse& operator=(const SiblingsFirstTraverse&);

	std::stack<std::queue<std::string> > _dirsStack;
};


} // namespace Poco


#endif // Foundation_RecursiveDirectoryIteratorStrategy_INCLUDED
