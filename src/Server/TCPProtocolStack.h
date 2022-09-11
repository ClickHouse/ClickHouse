#pragma once

#include <cstring>
#include <memory>
#include <list>

#include <Poco/Net/NetException.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Common/logger_useful.h>
#include <Server/IServer.h>
#include <base/getFQDNOrHostName.h>
#include <Server/TCPServer.h>
#include <Poco/Net/SecureStreamSocket.h>

#include "Poco/Net/SSLManager.h"
#include <Common/NetException.h>

#include "Interpreters/Context.h"
#include "Server/TCPProtocolStackData.h"
#include "base/types.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

class TCPConnectionAccessor : public Poco::Net::TCPServerConnection
{
public:
    using Poco::Net::TCPServerConnection::socket;
    explicit TCPConnectionAccessor(const Poco::Net::StreamSocket & socket) : Poco::Net::TCPServerConnection(socket) {}
};

class TCPProtocolStack : public Poco::Net::TCPServerConnection
{
    using StreamSocket = Poco::Net::StreamSocket;
    using TCPServerConnection = Poco::Net::TCPServerConnection;
private:
    TCPServer & tcp_server;
    std::list<TCPServerConnectionFactory::Ptr> stack;
    std::string conf_name;

public:
    TCPProtocolStack(TCPServer & tcp_server_, const StreamSocket & socket, const std::list<TCPServerConnectionFactory::Ptr> & stack_, const std::string & conf_name_)
        : TCPServerConnection(socket), tcp_server(tcp_server_), stack(stack_), conf_name(conf_name_)
    {}

    void run() override
    {
        TCPProtocolStackData stack_data;
        stack_data.socket = socket();
        for (auto & factory : stack)
        {
            std::unique_ptr<TCPServerConnection> connection(factory->createConnection(socket(), tcp_server, stack_data));
            connection->run();
            if (stack_data.socket != socket())
                socket() = stack_data.socket;
//            if (auto * accessor = dynamic_cast<TCPConnectionAccessor*>(connection.get()); accessor)
  //              socket() = accessor->socket();
        }
    }
};


class TCPProtocolStackFactory : public TCPServerConnectionFactory
{
private:
    IServer & server [[maybe_unused]];
    Poco::Logger * log;
    std::string conf_name;
    std::list<TCPServerConnectionFactory::Ptr> stack;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    template <typename... T>
    explicit TCPProtocolStackFactory(IServer & server_, const std::string & conf_name_, T... factory)
        : server(server_), log(&Poco::Logger::get("TCPProtocolStackFactory")), conf_name(conf_name_), stack({factory...})
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TCPProtocolStack(tcp_server, socket, stack, conf_name);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }

    void append(TCPServerConnectionFactory::Ptr factory)
    {
        stack.push_back(factory);
    }
};



class TLSHandler : public Poco::Net::TCPServerConnection //TCPConnectionAccessor
{
    using StreamSocket = Poco::Net::StreamSocket;
    using SecureStreamSocket = Poco::Net::SecureStreamSocket;
public:
    explicit TLSHandler(const StreamSocket & socket, const std::string & conf_name_, TCPProtocolStackData & stack_data_)
        : Poco::Net::TCPServerConnection(socket) //TCPConnectionAccessor(socket)
        , conf_name(conf_name_)
        , stack_data(stack_data_)
    {}

    void run() override
    {
        socket() = SecureStreamSocket::attach(socket(), Poco::Net::SSLManager::instance().defaultServerContext());
        stack_data.socket = socket();
    }
private:
    std::string conf_name;
    TCPProtocolStackData & stack_data;
};


class TLSHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server [[maybe_unused]];
    Poco::Logger * log;
    std::string conf_name;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    explicit TLSHandlerFactory(IServer & server_, const std::string & conf_name_)
        : server(server_), log(&Poco::Logger::get("TLSHandlerFactory")), conf_name(conf_name_)
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        TCPProtocolStackData stack_data;
        return createConnection(socket, tcp_server, stack_data);
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer &/* tcp_server*/, TCPProtocolStackData & stack_data) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TLSHandler(socket, conf_name, stack_data);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};


class ProxyV1Handler : public Poco::Net::TCPServerConnection
{
    using StreamSocket = Poco::Net::StreamSocket;
public:
    explicit ProxyV1Handler(const StreamSocket & socket, IServer & server_, const std::string & conf_name_, TCPProtocolStackData & stack_data_)
        : Poco::Net::TCPServerConnection(socket), server(server_), conf_name(conf_name_), stack_data(stack_data_) {}

    void run() override
    {
        const auto & settings = server.context()->getSettingsRef();
        socket().setReceiveTimeout(settings.receive_timeout);
        
        std::string word;
        bool eol;

        // Read PROXYv1 protocol header
        // http://www.haproxy.org/download/1.8/doc/proxy-protocol.txt

        // read "PROXY"
        if (!readWord(5, word, eol) || word != "PROXY" || eol)
            throw ParsingException("PROXY protocol violation", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);

        // read "TCP4" or "TCP6" or "UNKNOWN"
        if (!readWord(7, word, eol))
            throw ParsingException("PROXY protocol violation", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);

        if (word != "TCP4" && word != "TCP6" && word != "UNKNOWN")
            throw ParsingException("PROXY protocol violation", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);

        if (word == "UNKNOWN" && eol)
            return;

        if (eol)
            throw ParsingException("PROXY protocol violation", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);

        // read address
        if (!readWord(39, word, eol) || eol)
            throw ParsingException("PROXY protocol violation", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);

        stack_data.forwarded_for = std::move(word);

        // read address
        if (!readWord(39, word, eol) || eol)
            throw ParsingException("PROXY protocol violation", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);

        // read port
        if (!readWord(5, word, eol) || eol)
            throw ParsingException("PROXY protocol violation", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
        
        // read port and "\r\n"
        if (!readWord(5, word, eol) || !eol)
            throw ParsingException("PROXY protocol violation", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    }

protected:
    bool readWord(int max_len, std::string & word, bool & eol)
    {
        word.clear();
        eol = false;

        char ch = 0;
        int n = 0;
        bool is_cr = false;
        try
        {
            for (++max_len; max_len > 0 || is_cr; --max_len)
            {
                n = socket().receiveBytes(&ch, 1);
                if (n == 0)
                {
                    socket().shutdown();
                    return false;
                }
                if (n < 0)
                    break;

                if (is_cr)
                    return ch == 0x0A;

                if (ch == 0x0D)
                {
                    is_cr = true;
                    eol = true;
                    continue;
                }

                if (ch == ' ')
                    return true;

                word.push_back(ch);
            }
        }
        catch (const Poco::Net::NetException & e)
        {
            throw NetException(e.displayText() + ", while reading from socket (" + socket().peerAddress().toString() + ")", ErrorCodes::NETWORK_ERROR);
        }
        catch (const Poco::TimeoutException &)
        {
            throw NetException(fmt::format("Timeout exceeded while reading from socket ({}, {} ms)",
                socket().peerAddress().toString(),
                socket().getReceiveTimeout().totalMilliseconds()), ErrorCodes::SOCKET_TIMEOUT);
        }
        catch (const Poco::IOException & e)
        {
            throw NetException(e.displayText() + ", while reading from socket (" + socket().peerAddress().toString() + ")", ErrorCodes::NETWORK_ERROR);
        }

        if (n < 0)
            throw NetException("Cannot read from socket (" + socket().peerAddress().toString() + ")", ErrorCodes::CANNOT_READ_FROM_SOCKET);

        return false;
    }

private:
    IServer & server;
    std::string conf_name;
    TCPProtocolStackData & stack_data;
};

class ProxyV1HandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;
    std::string conf_name;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    explicit ProxyV1HandlerFactory(IServer & server_, const std::string & conf_name_)
        : server(server_), log(&Poco::Logger::get("ProxyV1HandlerFactory")), conf_name(conf_name_)
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        TCPProtocolStackData stack_data;
        return createConnection(socket, tcp_server, stack_data);
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer &/* tcp_server*/, TCPProtocolStackData & stack_data) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new ProxyV1Handler(socket, server, conf_name, stack_data);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};

}
