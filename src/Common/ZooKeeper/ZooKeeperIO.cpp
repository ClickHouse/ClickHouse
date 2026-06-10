#include <Common/ZooKeeper/ZooKeeperIO.h>


namespace Coordination
{

void write(OpNum x, WriteBuffer & out)
{
    write(static_cast<int32_t>(x), out);
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

size_t size(OpNum x)
{
    return size(static_cast<int32_t>(x));
}

size_t size(const std::string & s)
{
    return size(static_cast<int32_t>(s.size())) + s.size();
}

size_t size(const ACL & acl)
{
    return size(acl.permissions) + size(acl.scheme) + size(acl.id);
}

size_t size(const Stat & stat)
{
    return size(stat.czxid) + size(stat.mzxid) + size(stat.ctime) + size(stat.mtime) + size(stat.version) + size(stat.cversion)
        + size(stat.aversion) + size(stat.ephemeralOwner) + size(stat.dataLength) + size(stat.numChildren) + size(stat.pzxid);
}

size_t size(const Error & x)
{
    return size(static_cast<int32_t>(x));
}

void read(OpNum & x, ReadBuffer & in)
{
    int32_t raw_op_num;
    read(raw_op_num, in);
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
