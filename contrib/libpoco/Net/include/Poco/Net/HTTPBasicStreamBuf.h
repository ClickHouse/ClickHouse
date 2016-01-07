//
// HTTPBasicStreamBuf.h
//
// $Id: //poco/Main/template/class.h#4 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPBasicStreamBuf
//
// Definition of the HTTPBasicStreamBuf class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPBasicStreamBuf_INCLUDED
#define Net_HTTPBasicStreamBuf_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/BufferedStreamBuf.h"
#include "Poco/Net/HTTPBufferAllocator.h"


namespace Poco {
namespace Net {


typedef Poco::BasicBufferedStreamBuf<char, std::char_traits<char>, HTTPBufferAllocator> HTTPBasicStreamBuf;


} } // namespace Poco::Net


#endif // Net_HTTPBasicStreamBuf_INCLUDED
