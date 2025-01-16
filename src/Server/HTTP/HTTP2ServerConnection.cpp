#include <Server/HTTP/HTTP2ServerConnection.h>
#include <Server/HTTP/HTTP2ServerSession.h>
#include <Server/HTTP/HTTP2RequestHandler.h>

namespace DB {

    void HTTP2ServerConnection::run() {
        HTTP2ServerSession session(this, socket());
        
        while (!stopped && !session.sessionEnd()) { 
            int rv = session.sessionReceive();
            if (rv != 0) {
                return;
            }
        }
    }

    void HTTP2ServerConnection::handleRequest(HTTP2ServerStream* stream) {
        if (stopped) {
            return;
        }

        std::unique_ptr<HTTP2RequestHandler> handler(factory->createRequestHandler(stream->request));

        if (handler) {
            handler->handleRequest(stream->request, stream->response);
        } else {
            return;
        }
    }
} // DB
