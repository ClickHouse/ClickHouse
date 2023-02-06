//
// Session.cpp
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  Session
//
// Copyright (c) 2010-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#if defined(__APPLE__)
// Some OpenSSL functions are deprecated in OS X 10.7
#pragma GCC diagnostic ignored "-Wdeprecated-declarations" 
#endif


#include "Poco/Net/Session.h"


namespace Poco {
namespace Net {


Session::Session()
{
}


Session::~Session()
{
}


} } // namespace Poco::Net
