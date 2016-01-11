//
// RefCountedObject.cpp
//
// $Id: //poco/1.4/Foundation/src/RefCountedObject.cpp#1 $
//
// Library: Foundation
// Package: Core
// Module:  RefCountedObject
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/RefCountedObject.h"


namespace Poco {


RefCountedObject::RefCountedObject(): _counter(1)
{
}


RefCountedObject::~RefCountedObject()
{
}


} // namespace Poco
