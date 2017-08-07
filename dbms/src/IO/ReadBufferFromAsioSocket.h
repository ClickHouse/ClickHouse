#pragma once

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

class ReadBufferFromAsioSocket : public BufferWithOwnMemory<ReadBuffer>
{
protected:
    boost::asio::ip::tcp::socket & socket;
    boost::asio::yield_context yield;

    bool nextImpl() override
    {
        boost::system::error_code ec;
        size_t bytes_read = socket.async_read_some(
                boost::asio::buffer(internal_buffer.begin(), internal_buffer.size()),
                yield[ec]);

        if (ec == boost::asio::error::eof)
            return false;

        if (ec)
            throw Exception("Could not read.");

        if (bytes_read)
        {
            working_buffer.resize(bytes_read);
            return true;
        }

        return false;
    }

public:
    ReadBufferFromAsioSocket(
            boost::asio::ip::tcp::socket & socket_,
            boost::asio::yield_context yield_,
            size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : BufferWithOwnMemory<ReadBuffer>(buf_size)
        , socket(socket_)
        , yield(yield_)
    {
    }
};

}
