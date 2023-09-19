#include <boost/asio/buffer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <Common/ZooKeeper/ZooKeeperIO.h>


namespace Coordination
{

boost::asio::awaitable<void> writeStrict(const char * data, size_t size, tcp::socket & socket) // NOLINT
{
    auto n = co_await boost::asio::async_write(
        socket, boost::asio::buffer(data, size), boost::asio::use_awaitable);

    if (n != size)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Unexpected size while reading from ZooKeeper");
}

void write(OpNum x, WriteBuffer & out)
{
    write(static_cast<int32_t>(x), out);
}

boost::asio::awaitable<void> write(OpNum x, tcp::socket & socket) /// NOLINT
{
    co_await write(static_cast<int32_t>(x), socket);
}

void write(const std::string & s, WriteBuffer & out)
{
    write(static_cast<int32_t>(s.size()), out);
    out.write(s.data(), s.size());
}

boost::asio::awaitable<void> write(const std::string & s, tcp::socket & socket) /// NOLINT
{
    //co_await write(static_cast<int32_t>(s.size()), socket);
    //co_await writeStrict(s.data(), s.size(), socket);
    std::vector<boost::asio::const_buffer> buffers;
    int32_t size = static_cast<int32_t>(s.size());
    transformEndianness<std::endian::big>(size);
    buffers.push_back(boost::asio::const_buffer(reinterpret_cast<const char *>(&size), sizeof(size)));
    buffers.push_back(boost::asio::const_buffer(s.data(), s.size()));
    co_await boost::asio::async_write(socket, buffers, boost::asio::use_awaitable);
}

void write(const ACL & acl, WriteBuffer & out)
{
    write(acl.permissions, out);
    write(acl.scheme, out);
    write(acl.id, out);
}

void write(const Stat & stat, WriteBuffer & out)
{
    write(stat.czxid, out);
    write(stat.mzxid, out);
    write(stat.ctime, out);
    write(stat.mtime, out);
    write(stat.version, out);
    write(stat.cversion, out);
    write(stat.aversion, out);
    write(stat.ephemeralOwner, out);
    write(stat.dataLength, out);
    write(stat.numChildren, out);
    write(stat.pzxid, out);
}

boost::asio::awaitable<void> write(const Stat & stat, tcp::socket & socket) /// NOLINT
{
    co_await write(stat.czxid, socket);
    co_await write(stat.mzxid, socket);
    co_await write(stat.ctime, socket);
    co_await write(stat.mtime, socket);
    co_await write(stat.version, socket);
    co_await write(stat.cversion, socket);
    co_await write(stat.aversion, socket);
    co_await write(stat.ephemeralOwner, socket);
    co_await write(stat.dataLength, socket);
    co_await write(stat.numChildren, socket);
    co_await write(stat.pzxid, socket);
}

void write(const Error & x, WriteBuffer & out)
{
    write(static_cast<int32_t>(x), out);
}

boost::asio::awaitable<void> write(const Error & x, tcp::socket & socket) /// NOLINT
{
    co_await write(static_cast<int32_t>(x), socket);
}

void read(OpNum & x, ReadBuffer & in)
{
    int32_t raw_op_num;
    read(raw_op_num, in);
    x = getOpNum(raw_op_num);
}

boost::asio::awaitable<void> read(OpNum & x, tcp::socket & socket) /// NOLINT
{
    int32_t raw_op_num;
    co_await read(raw_op_num, socket);
    x = getOpNum(raw_op_num);
}

void read(std::string & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);

    if (size == -1)
    {
        /// It means that zookeeper node has NULL value. We will treat it like empty string.
        s.clear();
        return;
    }

    if (size < 0)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Negative size while reading string from ZooKeeper");

    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR,"Too large string size while reading from ZooKeeper");

    s.resize(size);
    size_t read_bytes = in.read(s.data(), size);
    if (read_bytes != static_cast<size_t>(size))
        throw Exception(
            Error::ZMARSHALLINGERROR, "Buffer size read from Zookeeper is not big enough. Expected {}. Got {}", size, read_bytes);
}

boost::asio::awaitable<void> read(std::string & s, tcp::socket & socket) /// NOLINT
{
    int32_t size = 0;
    co_await read(size, socket);

    if (size == -1)
    {
        /// It means that zookeeper node has NULL value. We will treat it like empty string.
        s.clear();
        co_return;
    }

    if (size < 0)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Negative size while reading string from ZooKeeper");

    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR,"Too large string size while reading from ZooKeeper");

    s.resize(size);
    auto n = co_await boost::asio::async_read(
        socket, boost::asio::buffer(reinterpret_cast<char *>(s.data()), size), boost::asio::use_awaitable);

    if (n != static_cast<size_t>(size))
        throw Exception(
            Error::ZMARSHALLINGERROR, "Buffer size read from Zookeeper is not big enough. Expected {}. Got {}", size, n);

}

void read(ACL & acl, ReadBuffer & in)
{
    read(acl.permissions, in);
    read(acl.scheme, in);
    read(acl.id, in);
}

boost::asio::awaitable<void> read(ACL & acl, tcp::socket & socket) /// NOLINT
{
    co_await read(acl.permissions, socket);
    co_await read(acl.scheme, socket);
    co_await read(acl.id, socket);
}

void read(Stat & stat, ReadBuffer & in)
{
    read(stat.czxid, in);
    read(stat.mzxid, in);
    read(stat.ctime, in);
    read(stat.mtime, in);
    read(stat.version, in);
    read(stat.cversion, in);
    read(stat.aversion, in);
    read(stat.ephemeralOwner, in);
    read(stat.dataLength, in);
    read(stat.numChildren, in);
    read(stat.pzxid, in);
}

void read(Error & x, ReadBuffer & in)
{
    int32_t code;
    read(code, in);
    x = Coordination::Error(code);
}

}
