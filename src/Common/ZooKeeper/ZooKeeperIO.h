#pragma once
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <cstdint>
#include <vector>
#include <array>

namespace Coordination
{

using namespace DB;

void write(size_t x, WriteBuffer & out);

/// uint64_t != size_t on darwin
#ifdef OS_DARWIN
void write(uint64_t x, WriteBuffer & out);
#endif

void write(int64_t x, WriteBuffer & out);
void write(int32_t x, WriteBuffer & out);
void write(OpNum x, WriteBuffer & out);
void write(bool x, WriteBuffer & out);
void write(const std::string & s, WriteBuffer & out);
void write(const ACL & acl, WriteBuffer & out);
void write(const Stat & stat, WriteBuffer & out);
void write(const Error & x, WriteBuffer & out);

template <size_t N>
void write(const std::array<char, N> s, WriteBuffer & out)
{
    write(int32_t(N), out);
    out.write(s.data(), N);
}

template <typename T>
void write(const std::vector<T> & arr, WriteBuffer & out)
{
    write(int32_t(arr.size()), out);
    for (const auto & elem : arr)
        write(elem, out);
}

void read(size_t & x, ReadBuffer & in);
#ifdef OS_DARWIN
void read(uint64_t & x, ReadBuffer & in);
#endif
void read(int64_t & x, ReadBuffer & in);
void read(int32_t & x, ReadBuffer & in);
void read(OpNum & x, ReadBuffer & in);
void read(bool & x, ReadBuffer & in);
void read(int8_t & x, ReadBuffer & in);
void read(std::string & s, ReadBuffer & in);
void read(ACL & acl, ReadBuffer & in);
void read(Stat & stat, ReadBuffer & in);
void read(Error & x, ReadBuffer & in);

template <size_t N>
void read(std::array<char, N> & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size != N)
        throw Exception("Unexpected array size while reading from ZooKeeper", Error::ZMARSHALLINGERROR);
    in.read(s.data(), N);
}

template <typename T>
void read(std::vector<T> & arr, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size < 0)
        throw Exception("Negative size while reading array from ZooKeeper", Error::ZMARSHALLINGERROR);
    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception("Too large array size while reading from ZooKeeper", Error::ZMARSHALLINGERROR);
    arr.resize(size);
    for (auto & elem : arr)
        read(elem, in);
}

}
