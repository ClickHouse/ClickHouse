//
// HTTPBasicStreamBuf.h
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


#include "Poco/BufferedStreamBuf.h"
#include "Poco/Net/Net.h"


namespace Poco
{
namespace Net
{
    constexpr size_t HTTP_DEFAULT_BUFFER_SIZE = 1024 * 1024;

    typedef Poco::BasicBufferedStreamBuf<char, std::char_traits<char>> HTTPBasicStreamBuf;


}
} // namespace Poco::Net


#endif // Net_HTTPBasicStreamBuf_INCLUDED
