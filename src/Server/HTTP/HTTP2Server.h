#pragma once

#include "HTTP2RequestHandlerFactory.h"

#include <Server/TCPServer.h>

namespace DB {

    class HTTP2Server : public TCPServer {
    public:
        explicit HTTP2Server(
            HTTP2RequestHandlerFactoryPtr factory_,
            Poco::ThreadPool & thread_pool,
            Poco::Net::ServerSocket & socket,
            Poco::Net::TCPServerParams::Ptr params = new Poco::Net::TCPServerParams);

        ~HTTP2Server() override;

        void stopAll(bool abort_current = false);

    private:
        HTTP2RequestHandlerFactoryPtr factory;
    };

}
