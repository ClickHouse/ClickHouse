//
// PipeImpl_DUMMY.cpp
//
// $Id: //poco/1.4/Foundation/src/PipeImpl_DUMMY.cpp#1 $
//
// Library: Foundation
// Package: Processes
// Module:  PipeImpl
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PipeImpl_DUMMY.h"


namespace Poco {


PipeImpl::PipeImpl()
{
}


PipeImpl::~PipeImpl()
{
}


int PipeImpl::writeBytes(const void* buffer, int length)
{
	return 0;
}


int PipeImpl::readBytes(void* buffer, int length)
{
	return 0;
}


PipeImpl::Handle PipeImpl::readHandle() const
{
	return 0;
}


PipeImpl::Handle PipeImpl::writeHandle() const
{
	return 0;
}


void PipeImpl::closeRead()
{
}


void PipeImpl::closeWrite()
{
}


} // namespace Poco
