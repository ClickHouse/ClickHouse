#include "config.h"

#include <Server/ENetServer.h>

#include <Server/ENetPacketMap.h>

#include <Poco/Net/TCPServer.h>
#include <Poco/ThreadPool.h>

#include <Server/IServer.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterserverIOHandler.h>

#include <Common/logger_useful.h>
#include <Compression/CompressedWriteBuffer.h>

#include <IO/WriteBufferENet.h>

#if USE_ENET

#include <enet.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

ENetServer::ENetServer(IServer & iserver_, std::string listen_host, UInt16 port) : ch_server(iserver_), host(listen_host), port_number(port)
{
    pool = new Poco::ThreadPool("ENetServer", 1, 1);
}

void ENetServer::start()
{
    if (enet_initialize () != 0)
    {
        throw 1;
    }
    logger = &Poco::Logger::get("enet");
    _stopped = false;

    enet_address_set_host(&address, host.c_str());
    address.port = port_number;

    LOG_INFO(logger, "Starting ENet server on {}:{}", host, port_number);

    pool->start(*this);
}

void ENetServer::run()
{
    LOG_INFO(logger, "Server RUN");

    server = enet_host_create (&address /* the address to bind the server host to */,
                                32      /* allow up to 32 clients and/or outgoing connections */,
                                2      /* allow up to 2 channels to be used, 0 and 1 */,
                                0      /* assume any amount of incoming bandwidth */,
                                0      /* assume any amount of outgoing bandwidth */);

    if (server == nullptr)
    {
        throw 1;
    }

    while (!_stopped)
    {
        ENetEvent event;
        LOG_INFO(logger, "Server ENET STEP");
        while (enet_host_service(server, &event, 10000) > 0)
        {
            switch (event.type)
            {
                case ENET_EVENT_TYPE_CONNECT:
                    //printf("A new client connected from %x:%u.\n", event.peer->address.host, event.peer->address.port);
                    //event.peer->data = "Client information";
                    LOG_INFO(logger, "New ENET connection");
                    break;

                case ENET_EVENT_TYPE_RECEIVE:
                    {
                        LOG_INFO(logger, "ENET receive {}", reinterpret_cast<char *>(event.packet->data));

                        ENetPack pck;
                        ENetPack resp_pck;
                        pck.deserialize(reinterpret_cast<char *>(event.packet->data));

                        enet_packet_destroy (event.packet);

                        auto endpoint_name = pck.get("endpoint");

                        bool compress = pck.get("compress") == "true";

                        auto endpoint = ch_server.context()->getInterserverIOHandler().getEndpoint(endpoint_name);

                        BufferWithOwnMemory<WriteBufferENet> out(DBMS_DEFAULT_BUFFER_SIZE);

                        std::shared_lock lock(endpoint->rwlock);
                        if (endpoint->blocker.isCancelled())
                            throw Exception(ErrorCodes::ABORTED, "Transferring part to replica was cancelled");

                        LOG_INFO(logger, "ENET Processing query");

                        if (compress)
                        {
                            CompressedWriteBuffer compressed_out(out);
                            endpoint->processQuery(pck, compressed_out, resp_pck);
                        }
                        else
                        {
                            endpoint->processQuery(pck, out, resp_pck);
                        }

                        out.finalize();

                        LOG_TRACE(logger, "\n{}", out.res());

                        //resp_pck.set("body", std::string(data));

                        //auto resp_str = resp_pck.serialize();
                        //auto resp_cstr = resp_str.c_str();

                        auto str = std::string(out.res());

                        auto response_str = resp_pck.serialize();
                        response_str += "\r" + str;

                        auto response_cstr = response_str.c_str();

                        ENetPacket * resp = enet_packet_create (response_cstr,
                                            response_str.size() + 1,
                                            ENET_PACKET_FLAG_RELIABLE);

                        enet_peer_send (event.peer, 0, resp);

                        enet_host_flush(server);

                        break;
                    }

                case ENET_EVENT_TYPE_DISCONNECT:
                    event.peer->data = nullptr;
                    break;

                case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                    event.peer->data = nullptr;
                    break;

                case ENET_EVENT_TYPE_NONE:
                    break;
            }
        }
    }
}


void ENetServer::stop()
{
    if (!_stopped)
    {
        enet_host_destroy(server);
        enet_deinitialize();
        _stopped = true;
        pool->stopAll();
    }
}

}
#endif
