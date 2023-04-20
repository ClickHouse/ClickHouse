#include "config.h"

#include <Server/ENetServer.h>

#include <Server/ENetPacketMap.h>

#include <Poco/Net/TCPServer.h>
#include <Poco/Thread.h>

#include <Server/IServer.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterserverIOHandler.h>

#include <Common/logger_useful.h>
#include <Compression/CompressedWriteBuffer.h>

#include <IO/WriteBuffer.h>

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
    thread = new Poco::Thread("ENetServer");
}

void ENetServer::start()
{
    if (enet_initialize () != 0)
    {
        throw 1;
    }
    static Poco::Logger * logger = &Poco::Logger::get("enet");
    LOG_INFO(logger, "ENET_START");
    _stopped = false;
    thread->start(*this);
}

void ENetServer::run()
{
    enet_address_set_host(&address, host.c_str());
    address.port = port_number;

    server = enet_host_create (&address /* the address to bind the server host to */,
                             32      /* allow up to 32 clients and/or outgoing connections */,
                              2      /* allow up to 2 channels to be used, 0 and 1 */,
                              0      /* assume any amount of incoming bandwidth */,
                              0      /* assume any amount of outgoing bandwidth */);

    if (server == nullptr)
    {
        throw 1;
    }

    ENetEvent event;
    ENetPacket* resp;

    while (true)
    {
        while (enet_host_service(server, &event, 1000) > 0)
        {
            switch (event.type)
            {
                case ENET_EVENT_TYPE_CONNECT:
                    //printf("A new client connected from %x:%u.\n", event.peer->address.host, event.peer->address.port);
                    //event.peer->data = "Client information";
                    break;

                case ENET_EVENT_TYPE_RECEIVE:
                    {
                        //event.packet->dataLength,
                        //event.packet->data,
                        //event.peer->data,
                        //event.channelID);
                        /* Clean up the packet now that we're done using it. */
                        ENetPack pck;
                        ENetPack resp_pck;
                        pck.deserialize(reinterpret_cast<char *>(event.packet->data));

                        enet_packet_destroy (event.packet);

                        // handle the request
                        // processQuery()

                        auto endpoint_name = pck.get("endpoint");
                        bool compress = pck.get("compress") == "true";

                        auto endpoint = ch_server.context()->getInterserverIOHandler().getEndpoint(endpoint_name);

                        WriteBuffer out(nullptr, 0);

                        std::shared_lock lock(endpoint->rwlock);
                        if (endpoint->blocker.isCancelled())
                            throw Exception(ErrorCodes::ABORTED, "Transferring part to replica was cancelled");

                        if (compress)
                        {
                            CompressedWriteBuffer compressed_out(out);
                            endpoint->processQuery(pck, compressed_out, resp_pck);
                        }
                        else
                        {
                            endpoint->processQuery(pck, out, resp_pck);
                        }

                        //auto buf = out.buffer();
                        //char[buf.size()] data = buf.start();

                        auto resp_str = resp_pck.serialize();
                        auto resp_cstr = resp_str.c_str();

                        resp = enet_packet_create (resp_cstr,
                                                strlen (resp_cstr) + 1,
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
        thread->join();
    }
}

}
#endif
