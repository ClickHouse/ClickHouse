#pragma once
#include <iterator>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

/**
 * A light way for message serialization/deserialization
 * Define your own message as following
 *   struct MyMessage : public Marshallable
 *   {
 *       int i;
 *       map<string, string> m;
 *       AnotherMashallable x;
 *       void marshal(MarshallablePack & p) const override { p << i << m << x; }
 *       void unmarshal(MarshallableUnpack & p) override { p >> i >> m >> x; }
 *   };
 * marshal() serializes the message content into a WriteBuffer (see MarshallablePack)
 * unmarshal() deserializes the message from a ReadBuffer (see MarshallableUnpack).
 * The order of the message fields in marshal and unmarshal must be the same.
 *
 * You can add new fields into an existed message with backward compatibility。For example, if we
 * want to add a new field into MyMessage, it would be like
 *   struct MyMessage : public Marshallable
 *   {
 *       int i;
 *       map<string, string> m;
 *       AnotherMashallable x;
 *       vector<int> v; // the new field
 *
 *       // the new field must be added at the end
 *       void marshal(MarshallablePack & p) const override { p << i << m << x << v; }
 *       void unmarshal(MarshallableUnpack & p) override {
 *           p >> i >> m >> x;
 *           // for backward compatibility, need to take this check
 *           if (!p.eof()) p >> v;
 *       }
 *   };
 */
namespace DB
{

class MarshallablePack
{
public:
    explicit MarshallablePack(WriteBuffer & buffer_) : buffer(buffer_) { }
    inline WriteBuffer & getBuffer() { return buffer; }

private:
    WriteBuffer & buffer;
};

class MarshallableUnPack
{
public:
    explicit MarshallableUnPack(ReadBuffer & buffer_) : buffer(buffer_) { }

    inline ReadBuffer & getBuffer() { return buffer; }
    inline bool eof() { return buffer.eof(); }

private:
    ReadBuffer & buffer;
};

class MarshallableTraceBuffer
{
public:
    WriteBufferFromOwnString & buffer() { return write_buffer; }
    String & str() { return write_buffer.str(); }

private:
    WriteBufferFromOwnString write_buffer;
};
struct Marshallable
{
    Marshallable() = default;
    Marshallable & operator=(const Marshallable &) = default;
    Marshallable(const Marshallable &) = default;
    virtual ~Marshallable() = default;
    virtual void marshal(MarshallablePack &) const = 0;
    virtual void unmarshal(MarshallableUnPack &) = 0;
    virtual MarshallableTraceBuffer & trace(MarshallableTraceBuffer & buf) const
    {
        buf.buffer() << "trace Marshallable [ not immplement ]";
        return buf;
    }
};

inline MarshallablePack & operator<<(MarshallablePack & p, bool sign)
{
    writeVarUInt(sign ? 1 : 0, p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, bool & sign)
{
    UInt64 x;
    readVarUInt(x, p.getBuffer());
    sign = false;
    if (x)
    {
        sign = true;
    }
    return p;
}

inline MarshallablePack & operator<<(MarshallablePack & p, Int8 n)
{
    writeVarInt(n, p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, Int8 & n)
{
    Int64 x;
    readVarInt(x, p.getBuffer());
    n = static_cast<Int8>(x);
    return p;
}

inline MarshallablePack & operator<<(MarshallablePack & p, UInt8 n)
{
    writeVarUInt(n, p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, UInt8 & n)
{
    UInt64 x;
    readVarUInt(x, p.getBuffer());
    n = static_cast<UInt8>(x);
    return p;
}

inline MarshallablePack & operator<<(MarshallablePack & p, Int16 n)
{
    writeVarInt(n, p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, Int16 & n)
{
    Int64 x;
    readVarInt(x, p.getBuffer());
    n = static_cast<Int16>(x);
    return p;
}

inline MarshallablePack & operator<<(MarshallablePack & p, UInt16 n)
{
    writeVarUInt(n, p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, UInt16 & n)
{
    UInt64 x;
    readVarUInt(x, p.getBuffer());
    n = static_cast<UInt16>(x);
    return p;
}

inline MarshallablePack & operator<<(MarshallablePack & p, Int32 n)
{
    writeVarInt(n, p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, Int32 & n)
{
    Int64 x;
    readVarInt(x, p.getBuffer());
    n = static_cast<Int32>(x);
    return p;
}

inline MarshallablePack & operator<<(MarshallablePack & p, UInt32 n)
{
    writeVarUInt(n, p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, UInt32 & n)
{
    UInt64 x;
    readVarUInt(x, p.getBuffer());
    n = static_cast<UInt32>(x);
    return p;
}

inline MarshallablePack & operator<<(MarshallablePack & p, Int64 n)
{
    writeVarInt(n, p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, Int64 & n)
{
    readVarInt(n, p.getBuffer());
    return p;
}

inline MarshallablePack & operator<<(MarshallablePack & p, UInt64 n)
{
    writeVarUInt(n, p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, UInt64 & n)
{
    readVarUInt(n, p.getBuffer());
    return p;
}

inline MarshallablePack & operator<<(MarshallablePack & p, const std::string & str)
{
    writeStringBinary(str, p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, std::string & str)
{
    readStringBinary(str, p.getBuffer());
    return p;
}

inline MarshallablePack & operator<<(MarshallablePack & p, const Marshallable & m)
{
    WriteBufferFromOwnString inner_buffer;
    MarshallablePack inner_pack(inner_buffer);
    m.marshal(inner_pack);
    writeStringBinary(inner_buffer.str(), p.getBuffer());
    return p;
}

inline MarshallableUnPack & operator>>(MarshallableUnPack & p, Marshallable & m)
{
    std::string data;
    readStringBinary(data, p.getBuffer());
    ReadBufferFromString inner_buffer(data);
    MarshallableUnPack inner_unpack(inner_buffer);
    m.unmarshal(inner_unpack);
    return p;
}

template <class T1, class T2>
inline MarshallablePack & operator<<(MarshallablePack & p, const std::pair<T1, T2> & x)
{
    p << x.first << x.second;
    return p;
}

template <class T1, class T2>
inline MarshallableUnPack & operator>>(MarshallableUnPack & p, std::pair<const T1, T2> & x)
{
    const T1 & const_first = x.first;
    T1 & first = const_cast<T1 &>(const_first);
    p >> first >> x.second;
    return p;
}

template <class T>
inline void marshal_container(MarshallablePack & p, const T & container)
{
    UInt64 n = container.size();
    p << n;
    for (typename T::const_iterator it = container.begin(); it != container.end(); ++it)
    {
        p << *it;
    }
}

template <class T>
inline void unmarshal_container(MarshallableUnPack & p, T container_it)
{
    UInt64 n = 0;
    readVarUInt(n, p.getBuffer());
    for (UInt64 i = 0; i < n; ++i)
    {
        typename T::container_type::value_type tmp;
        p >> tmp;
        *container_it = tmp;
        ++container_it;
    }
}

template <class T>
inline MarshallablePack & operator<<(MarshallablePack & p, const std::set<T> & s)
{
    marshal_container(p, s);
    return p;
}

template <class T>
inline MarshallableUnPack & operator>>(MarshallableUnPack & p, std::set<T> & s)
{
    unmarshal_container(p, std::inserter(s, s.begin()));
    return p;
}

template <class T>
inline MarshallablePack & operator<<(MarshallablePack & p, const std::vector<T> & v)
{
    marshal_container(p, v);
    return p;
}

template <class T>
inline MarshallableUnPack & operator>>(MarshallableUnPack & p, std::vector<T> & v)
{
    unmarshal_container(p, std::back_inserter(v));
    return p;
}

template <class T>
inline MarshallablePack & operator<<(MarshallablePack & p, const std::list<T> & l)
{
    marshal_container(p, l);
    return p;
}

template <class T>
inline MarshallableUnPack & operator>>(MarshallableUnPack & p, std::list<T> & l)
{
    unmarshal_container(p, std::inserter(l, l.begin()));
    return p;
}

template <class K, class V>
inline MarshallablePack & operator<<(MarshallablePack & p, const std::map<K, V> & m)
{
    marshal_container(p, m);
    return p;
}

template <class K, class V>
inline MarshallableUnPack & operator>>(MarshallableUnPack & p, std::map<K, V> & m)
{
    unmarshal_container(p, std::inserter(m, m.end()));
    return p;
}

inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const Int8 & x)
{
    buf.buffer() << x;
    return buf;
}

inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const UInt8 & x)
{
    buf.buffer() << x;
    return buf;
}

inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const Int16 & x)
{
    buf.buffer() << x;
    return buf;
}

inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const UInt16 & x)
{
    buf.buffer() << x;
    return buf;
}

inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const Int32 & x)
{
    buf.buffer() << x;
    return buf;
}

inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const UInt32 & x)
{
    buf.buffer() << x;
    return buf;
}

inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const Int64 & x)
{
    buf.buffer() << x;
    return buf;
}

inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const UInt64 & x)
{
    buf.buffer() << x;
    return buf;
}

inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const char * c)
{
    buf.buffer() << c;
    return buf;
}


inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const String & s)
{
    buf.buffer() << s;
    return buf;
}

inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const Marshallable & m)
{
    buf.buffer() << "{";
    m.trace(buf);
    buf.buffer() << "}";
    return buf;
}

template <class T>
inline MarshallableTraceBuffer & trace_container(MarshallableTraceBuffer & buf, const T & c, char div = ',')
{
    for (typename T::const_iterator iter = c.begin(); iter != c.end(); ++iter)
    {
        buf << *iter << div;
    }
    return buf;
}

template <class T1, class T2>
inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const std::pair<T1, T2> & p)
{
    buf << p.first << "=" << p.second;
    return buf;
}

template <class T>
inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const std::set<T> & s)
{
    buf.buffer() << "{";
    buf = trace_container(buf, s);
    buf.buffer() << "}";
    return buf;
}

template <class T>
inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const std::vector<T> & v)
{
    buf.buffer() << "[";
    trace_container(buf, v);
    buf.buffer() << "]";
    return buf;
}

template <class T>
inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const std::list<T> & l)
{
    buf.buffer() << "[";
    trace_container(buf, l);
    buf.buffer() << "]";
    return buf;
}

template <class K, class V>
inline MarshallableTraceBuffer & operator<<(MarshallableTraceBuffer & buf, const std::map<K, V> & m)
{
    buf.buffer() << "{";
    trace_container(buf, m);
    buf.buffer() << "}";
    return buf;
}

template <class M>
void packageToString(const M & m, String & output)
{
    WriteBufferFromOwnString buffer;
    MarshallablePack pack(buffer);
    m.marshal(pack);
    output = buffer.str();
}

template <class M>
void stringToPackage(const String & input, M & m)
{
    ReadBufferFromString buffer(input);
    MarshallableUnPack pack(buffer);
    m.unmarshal(pack);
}


}
