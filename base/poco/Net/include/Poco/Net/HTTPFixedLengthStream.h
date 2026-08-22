//
// HTTPFixedLengthStream.h
//
// Library: Net
// Package: HTTP
// Module:  HTTPFixedLengthStream
//
// Definition of the HTTPFixedLengthStream class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPFixedLengthStream_INCLUDED
#define Net_HTTPFixedLengthStream_INCLUDED


#include <cstddef>
#include <istream>
#include <ostream>
#include "Poco/Net/HTTPBasicStreamBuf.h"
#include "Poco/Net/Net.h"


namespace Poco
{
namespace Net
{


    class HTTPSession;


    class Net_API HTTPFixedLengthStreamBuf : public HTTPBasicStreamBuf
    /// This is the streambuf class used for reading and writing fixed-size
    /// HTTP message bodies.
    ///
    /// At most a given number of bytes are read or written.
    {
    public:
        typedef HTTPBasicStreamBuf::openmode openmode;

        typedef Poco::Int64 ContentLength;

        HTTPFixedLengthStreamBuf(HTTPSession & session, ContentLength length, openmode mode);
        ~HTTPFixedLengthStreamBuf();

        bool isComplete() const;

    protected:
        int readFromDevice(char * buffer, std::streamsize length);
        int writeToDevice(const char * buffer, std::streamsize length);

    private:
        HTTPSession & _session;
        ContentLength _length;
        ContentLength _count;
    };


    class Net_API HTTPFixedLengthIOS : public virtual std::ios
    /// The base class for HTTPFixedLengthInputStream.
    {
    public:
        HTTPFixedLengthIOS(HTTPSession & session, HTTPFixedLengthStreamBuf::ContentLength length, HTTPFixedLengthStreamBuf::openmode mode);
        ~HTTPFixedLengthIOS();
        HTTPFixedLengthStreamBuf * rdbuf();

        bool isComplete() const { return _buf.isComplete(); }

    protected:
        HTTPFixedLengthStreamBuf _buf;
    };


    class Net_API HTTPFixedLengthInputStream : public HTTPFixedLengthIOS, public std::istream
    /// This class is for internal use by HTTPSession only.
    {
    public:
        HTTPFixedLengthInputStream(HTTPSession & session, HTTPFixedLengthStreamBuf::ContentLength length);
        ~HTTPFixedLengthInputStream();
    };


    class Net_API HTTPFixedLengthOutputStream : public HTTPFixedLengthIOS, public std::ostream
    /// This class is for internal use by HTTPSession only.
    {
    public:
        HTTPFixedLengthOutputStream(HTTPSession & session, HTTPFixedLengthStreamBuf::ContentLength length);
        ~HTTPFixedLengthOutputStream();
    };


}
} // namespace Poco::Net


#endif // Net_HTTPFixedLengthStream_INCLUDED
