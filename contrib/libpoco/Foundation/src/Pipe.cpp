//
// Pipe.cpp
//
// $Id: //poco/1.4/Foundation/src/Pipe.cpp#1 $
//
// Library: Foundation
// Package: Processes
// Module:  Pipe
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Pipe.h"


namespace Poco {


Pipe::Pipe():
	_pImpl(new PipeImpl)
{
}

	
Pipe::Pipe(const Pipe& pipe):
	_pImpl(pipe._pImpl)
{
	_pImpl->duplicate();
}


Pipe::~Pipe()
{
	_pImpl->release();
}


Pipe& Pipe::operator = (const Pipe& pipe)
{
	if (this != &pipe)
	{
		_pImpl->release();
		_pImpl = pipe._pImpl;
		_pImpl->duplicate();
	}
	return *this;
}


void Pipe::close(CloseMode mode)
{
	switch (mode)
	{
	case CLOSE_READ:
		_pImpl->closeRead();
		break;
	case CLOSE_WRITE:
		_pImpl->closeWrite();
		break;
	default:
		_pImpl->closeRead();
		_pImpl->closeWrite();
		break;
	}
}


} // namespace Poco
