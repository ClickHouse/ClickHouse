#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <cstdint>
#include <vector>
#include <array>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

namespace Coordination
{

using namespace DB;
using boost::asio::ip::tcp;

template <typename T>
requires is_arithmetic_v<T>
void write(T x, WriteBuffer & out)
{
    writeBinaryBigEndian(x, out);
}

boost::asio::awaitable<void> writeStrict(const char * data, size_t size, tcp::socket & socket);

template <typename T>
requires is_arithmetic_v<T>
boost::asio::awaitable<void> write(T x, tcp::socket & socket) // NOLINT
{
    transformEndianness<std::endian::big>(x);
    co_await writeStrict(reinterpret_cast<char *>(&x), sizeof(x), socket);
}

void write(OpNum x, WriteBuffer & out);
boost::asio::awaitable<void> write(OpNum x, tcp::socket & socket);

void write(const std::string & s, WriteBuffer & out);
boost::asio::awaitable<void> write(const std::string & s, tcp::socket & socket);

void write(const ACL & acl, WriteBuffer & out);

void write(const Stat & stat, WriteBuffer & out);
boost::asio::awaitable<void> write(const Stat & stat, tcp::socket & socket);

void write(const Error & x, WriteBuffer & out);
boost::asio::awaitable<void> write(const Error & x, tcp::socket & socket);

template <size_t N>
void write(const std::array<char, N> s, WriteBuffer & out)
{
    write(int32_t(N), out);
    out.write(s.data(), N);
}

template <size_t N>
boost::asio::awaitable<void> write(const std::array<char, N> s, tcp::socket & socket) /// NOLINT
{
    co_await write(int32_t(N), socket);
    co_await boost::asio::async_write(socket, boost::asio::buffer(s.data(), N), boost::asio::use_awaitable);
}

template <typename T>
void write(const std::vector<T> & arr, WriteBuffer & out)
{
    write(int32_t(arr.size()), out);
    for (const auto & elem : arr)
        write(elem, out);
}

template <typename T>
boost::asio::awaitable<void> write(const std::vector<T> & arr, tcp::socket & socket) /// NOLINT
{
    co_await write(int32_t(arr.size()), socket);
    for (const auto & elem : arr)
        co_await write(elem, socket);
}

template <typename T>
requires is_arithmetic_v<T>
void read(T & x, ReadBuffer & in)
{
    readBinaryBigEndian(x, in);
}

template <typename T>
requires is_arithmetic_v<T>
boost::asio::awaitable<void> read(T & x, tcp::socket & socket) // NOLINT
{
    auto n = co_await boost::asio::async_read(
        socket, boost::asio::buffer(reinterpret_cast<char *>(&x), sizeof(x)), boost::asio::use_awaitable);

    if (n != sizeof(x))
        throw Exception(Error::ZMARSHALLINGERROR, "Unexpected size while reading from ZooKeeper, expected {}, got {}", sizeof(x), n);

    transformEndianness<std::endian::big>(x);
}

void read(OpNum & x, ReadBuffer & in);
boost::asio::awaitable<void> read(OpNum & x, tcp::socket & socket);

void read(std::string & s, ReadBuffer & in);
boost::asio::awaitable<void> read(std::string & s, tcp::socket & socket);

void read(ACL & acl, ReadBuffer & in);
boost::asio::awaitable<void> read(ACL & acl, tcp::socket & socket);

void read(Stat & stat, ReadBuffer & in);
void read(Error & x, ReadBuffer & in);

template <size_t N>
void read(std::array<char, N> & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size != N)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Unexpected array size while reading from ZooKeeper");
    in.readStrict(s.data(), N);
}

template <size_t N>
boost::asio::awaitable<void> read(std::array<char, N> & s, tcp::socket & socket) /// NOLINT
{
    int32_t size = 0;
    co_await read(size, socket);
    if (size != N)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Unexpected array size while reading from ZooKeeper");
    co_await boost::asio::async_read(socket, boost::asio::buffer(s.data(), N), boost::asio::use_awaitable);
}

template <typename T>
void read(std::vector<T> & arr, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size < 0)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Negative size while reading array from ZooKeeper");
    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Too large array size while reading from ZooKeeper");
    arr.resize(size);
    for (auto & elem : arr)
        read(elem, in);
}

template <typename T>
boost::asio::awaitable<void> read(std::vector<T> & arr, tcp::socket & socket) /// NOLINT
{
    int32_t size = 0;
    co_await read(size, socket);
    if (size < 0)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Negative size while reading array from ZooKeeper");
    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Too large array size while reading from ZooKeeper");
    arr.resize(size);
    for (auto & elem : arr)
        co_await read(elem, socket);
}


}
