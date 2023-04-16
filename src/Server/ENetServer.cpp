#include "config.h"

#include <Server/ENetServer.h>

#include <Poco/Net/TCPServer.h>
#include <Poco/Thread.h>

#if USE_ENET

#include <enet.h>

namespace DB
{

ENetServer::ENetServer()
{
    thread = new Poco::Thread("ENetServer");
}

void ENetServer::start()
{
    if (enet_initialize () != 0)
    {
        throw 1;
    }
    _stopped = false;
    thread->start(*this);
}


void ENetServer::run()
{
    address.host = ENET_HOST_ANY;
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

    while (enet_host_service(server, &event, 1000) > 0)
    {
        switch (event.type)
        {
            case ENET_EVENT_TYPE_CONNECT:
                //printf("A new client connected from %x:%u.\n", event.peer->address.host, event.peer->address.port);
                //event.peer->data = "Client information";
                break;

            case ENET_EVENT_TYPE_RECEIVE:
                /*printf("A packet of length %lu containing %s was received from %s on channel %u.\n",
                        event.packet->dataLength,
                        event.packet->data,
                        event.peer->data,
                        event.channelID);*/
                /* Clean up the packet now that we're done using it. */
                enet_packet_destroy (event.packet);

                //work

                //response

                break;

            case ENET_EVENT_TYPE_DISCONNECT:
                //printf("%s disconnected.\n", event.peer->data);
                /* Reset the peer's client information. */
                event.peer->data = nullptr;
                break;

            case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                //printf("%s disconnected due to timeout.\n", event.peer->data);
                /* Reset the peer's client information. */
                event.peer->data = nullptr;
                break;

            case ENET_EVENT_TYPE_NONE:
                break;
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
