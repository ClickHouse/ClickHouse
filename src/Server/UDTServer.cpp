#include "config.h"

#include <Server/UDTServer.h>

#include <Server/UDPReplicationPack.h>

#include <Poco/Net/TCPServer.h>
#include <Poco/ThreadPool.h>

#include <Server/IServer.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterserverIOHandler.h>

#include <Common/logger_useful.h>
#include <Compression/CompressedWriteBuffer.h>

#include <IO/WriteBufferUDPReplication.h>

#if USE_UDT

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreserved-identifier"
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"

#include <udt.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

UDTServer::UDTServer(IServer & iserver_, std::string listen_host, UInt16 port) : ch_server(iserver_), host(listen_host), port_number(port)
{
    pool = new Poco::ThreadPool("UDTServer", 1, 1);
}

void UDTServer::start()
{
    logger = &Poco::Logger::get("UDTRelicationServer");
    _stopped = false;

    serv = UDT::socket(AF_INET, SOCK_DGRAM, 0);

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_number);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    LOG_INFO(logger, "Starting UDT server on {}:{}", host, port_number);

    pool->start(*this);
}

void UDTServer::run()
{
    memset(&(server_addr.sin_zero), '\0', 8);

    if (UDT::ERROR == UDT::bind(serv, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)))
    {
        return;
    }

    UDT::listen(serv, 10);

    while (!_stopped) {

        UDPReplicationPack pck;
        UDPReplicationPack resp_pck;

        int namelen;

        sockaddr_in client_addr;

        recver = UDT::accept(serv, reinterpret_cast<sockaddr*>(&client_addr), &namelen);

        char data[1000];

        if (UDT::ERROR == UDT::recvmsg(recver, data, 1000))
        {
            continue;
        }

        pck.deserialize(reinterpret_cast<char *>(data), strlen(data));

        std::string buf_data;
        std::stringbuf buf(buf_data);
        std::ostream data_stream(&buf);

        WriteBufferUDPReplication out(data_stream, resp_pck);

        auto endpoint_name = pck.get("endpoint");
        bool compress = pck.get("compress") == "true";
        auto endpoint = ch_server.context()->getInterserverIOHandler().getEndpoint(endpoint_name);

        std::shared_lock lock(endpoint->rwlock);
        if (endpoint->blocker.isCancelled())
            throw Exception(ErrorCodes::ABORTED, "Transferring part to replica was cancelled");

        LOG_INFO(logger, "Processing UDT replication query");

        if (compress)
        {
            CompressedWriteBuffer compressed_out(out);
            endpoint->processQuery(pck, compressed_out, resp_pck);
        }
        else
        {
            endpoint->processQuery(pck, out, resp_pck);
        }

        resp_pck.set("Connection", "Keep-Alive");

        resp_pck.set("Transfer-Encoding", "chunked");

        out.finalize();

        auto response_str = resp_pck.serialize();
        response_str += "\r" + out.res();

        LOG_INFO(logger, "Sending UDT response");

        unsigned char response_cstr[response_str.size()];

         std::copy(response_str.cbegin(), response_str.cend(), response_cstr);

        auto packet_size = std::to_string(response_str.size() + 1);

        if (UDT::ERROR == UDT::sendmsg(recver, packet_size.c_str(), static_cast<int>(packet_size.size() + 1)))
        {
            continue;
        }

        if (UDT::ERROR == UDT::sendmsg(recver, reinterpret_cast<char *>(response_cstr), static_cast<int>(response_str.size() + 1)))
        {
            continue;
        }
    }
}


void UDTServer::stop()
{
    if (!_stopped)
    {
        _stopped = true;
        UDT::close(recver);
        UDT::close(serv);
        pool->stopAll();
    }
}

}
#pragma GCC diagnostic pop
#endif
