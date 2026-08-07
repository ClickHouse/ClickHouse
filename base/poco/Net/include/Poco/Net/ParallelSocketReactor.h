//
// ParallelSocketReactor.h
//
// Library: Net
// Package: Reactor
// Module:  ParallelSocketReactor
//
// Definition of the ParallelSocketReactor class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_ParallelSocketReactor_INCLUDED
#define Net_ParallelSocketReactor_INCLUDED


#include "Poco/NObserver.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/SocketNotification.h"
#include "Poco/Net/SocketReactor.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/SharedPtr.h"
#include "Poco/Thread.h"


using Poco::AutoPtr;
using Poco::NObserver;
using Poco::Thread;
using Poco::Net::ReadableNotification;
using Poco::Net::ServerSocket;
using Poco::Net::ShutdownNotification;
using Poco::Net::Socket;
using Poco::Net::SocketReactor;
using Poco::Net::StreamSocket;


namespace Poco
{
namespace Net
{


    template <class SR>
    class ParallelSocketReactor : public SR
    {
    public:
        typedef Poco::SharedPtr<ParallelSocketReactor> Ptr;

        ParallelSocketReactor() { _thread.start(*this); }

        ParallelSocketReactor(const Poco::Timespan & timeout) : SR(timeout) { _thread.start(*this); }

        ~ParallelSocketReactor()
        {
            try
            {
                this->stop();
                _thread.join();
            }
            catch (...)
            {
                poco_unexpected();
            }
        }

    protected:
        void onIdle()
        {
            SR::onIdle();
            Poco::Thread::yield();
        }

    private:
        Poco::Thread _thread;
    };


}
} // namespace Poco::Net


#endif // Net_ParallelSocketReactor_INCLUDED
