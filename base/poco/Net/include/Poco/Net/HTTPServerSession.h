//
// HTTPServerSession.h
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerSession
//
// Definition of the HTTPServerSession class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPServerSession_INCLUDED
#define Net_HTTPServerSession_INCLUDED


#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerSession.h"
#include "Poco/Net/HTTPSession.h"
#include "Poco/Net/Net.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Timespan.h"

#include <functional>


namespace Poco
{
namespace Net
{


    class Net_API HTTPServerSession : public HTTPSession
    /// This class handles the server side of a
    /// HTTP session. It is used internally by
    /// HTTPServer.
    {
    public:
        HTTPServerSession(const StreamSocket & socket, HTTPServerParams::Ptr pParams);
        /// Creates the HTTPServerSession.

        virtual ~HTTPServerSession();
        /// Destroys the HTTPServerSession.

        bool hasMoreRequests();
        /// Returns true if there are requests available.

        bool canKeepAlive() const;
        /// Returns true if the session can be kept alive.

        SocketAddress clientAddress();
        /// Returns the client's address.

        SocketAddress serverAddress();
        /// Returns the server's address.

        void setKeepAliveTimeout(Poco::Timespan keepAliveTimeout);

        size_t getKeepAliveTimeout() const { return _keepAliveTimeout.totalSeconds(); }

        size_t getMaxKeepAliveRequests() const { return _maxKeepAliveRequests; }

        void setStopCallback(std::function<bool()> stopCallback);
        /// Sets a predicate that aborts the wait for the next request as soon as
        /// it returns true. Used to stop waiting promptly when the server is shutting
        /// down instead of blocking for the whole keep-alive timeout.

    private:
        bool waitForRequest(Poco::Timespan timeout);
        /// Polls the socket for an incoming request, waking up periodically to
        /// re-evaluate the stop callback so an idle connection does not block
        /// shutdown for the whole timeout.

        bool _firstRequest;
        Poco::Timespan _keepAliveTimeout;
        size_t _maxKeepAliveRequests;
        std::function<bool()> _stopCallback;
    };


    //
    // inlines
    //
    inline bool HTTPServerSession::canKeepAlive() const
    {
        return getKeepAlive() && _maxKeepAliveRequests > 0;
    }


}
} // namespace Poco::Net


#endif // Net_HTTPServerSession_INCLUDED
