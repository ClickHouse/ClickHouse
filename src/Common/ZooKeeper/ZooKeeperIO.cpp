#include <Common/ZooKeeper/ZooKeeperIO.h>

namespace Coordination
{

void write(size_t x, WriteBuffer & out)
{
    x = __builtin_bswap64(x);
    writeBinary(x, out);
}

#ifdef OS_DARWIN
void write(uint64_t x, WriteBuffer & out)
{
    x = __builtin_bswap64(x);
    writeBinary(x, out);
}
#endif

void write(int64_t x, WriteBuffer & out)
{
    x = __builtin_bswap64(x);
    writeBinary(x, out);
}
void write(int32_t x, WriteBuffer & out)
{
    x = __builtin_bswap32(x);
    writeBinary(x, out);
}

void write(OpNum x, WriteBuffer & out)
{
    write(static_cast<int32_t>(x), out);
}

void write(bool x, WriteBuffer & out)
{
    writeBinary(x, out);
}

void write(const std::string & s, WriteBuffer & out)
{
    write(static_cast<int32_t>(s.size()), out);
    out.write(s.data(), s.size());
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

void write(const Error & x, WriteBuffer & out)
{
    write(static_cast<int32_t>(x), out);
}

#ifdef OS_DARWIN
void read(uint64_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap64(x);
}
#endif

void read(size_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap64(x);
}

void read(int64_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap64(x);
}

void read(int32_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap32(x);
}

void read(OpNum & x, ReadBuffer & in)
{
    int32_t raw_op_num;
    read(raw_op_num, in);
    x = getOpNum(raw_op_num);
}

void read(bool & x, ReadBuffer & in)
{
    readBinary(x, in);
}

void read(int8_t & x, ReadBuffer & in)
{
    readBinary(x, in);
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
        throw Exception("Negative size while reading string from ZooKeeper", Error::ZMARSHALLINGERROR);

    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception("Too large string size while reading from ZooKeeper", Error::ZMARSHALLINGERROR);

    s.resize(size);
    in.read(s.data(), size);
}

void read(ACL & acl, ReadBuffer & in)
{
    read(acl.permissions, in);
    read(acl.scheme, in);
    read(acl.id, in);
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
