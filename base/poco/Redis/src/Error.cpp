//
// Error.cpp
//
// Library: Redis
// Package: Redis
// Module:  Error
//
// Implementation of the Error class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Redis/Error.h"


namespace Poco {
namespace Redis {


Error::Error()
{
}


Error::Error(const std::string& message): _message(message)
{
}


Error::~Error()
{
}


} } // namespace Poco::Redis
