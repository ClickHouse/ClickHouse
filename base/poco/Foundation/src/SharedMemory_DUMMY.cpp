//
// SharedMemoryImpl.cpp
//
// Library: Foundation
// Package: Processes
// Module:  SharedMemoryImpl
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SharedMemory_DUMMY.h"


namespace Poco {


SharedMemoryImpl::SharedMemoryImpl(const std::string&, std::size_t, SharedMemory::AccessMode, const void*, bool)
{
}


SharedMemoryImpl::SharedMemoryImpl(const Poco::File&, SharedMemory::AccessMode, const void*)
{
}


SharedMemoryImpl::~SharedMemoryImpl()
{
}


} // namespace Poco
