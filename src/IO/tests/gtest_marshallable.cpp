#include <gtest/gtest.h>
#include <IO/Marshallable.h>

using namespace DB;


struct InnerMarshallableV1 : public Marshallable
{
    InnerMarshallableV1() = default;
    int x = 0;
    void marshal(MarshallablePack & p) const override { p << x; }
    void unmarshal(MarshallableUnPack & p) override { p >> x; }
};

struct InnerMarshallableV2 : public Marshallable
{
    InnerMarshallableV2() = default;
    int x = 0;
    int y = 0;
    void marshal(MarshallablePack & p) const override { p << x << y; }
    void unmarshal(MarshallableUnPack & p) override
    {
        p >> x;
        if (!p.eof())
            p >> y;
    }
};

struct MyMarshallableV1 : public Marshallable
{
    MyMarshallableV1() = default;
    bool b;
    UInt8 u8;
    Int8 i8;
    UInt16 u16;
    Int16 i16;
    UInt32 u32;
    Int32 i32;
    UInt64 u64;
    Int64 i64;
    String s;
    InnerMarshallableV1 mar;
    void marshal(MarshallablePack & p) const override { p << b << u8 << i8 << u16 << i16 << u32 << i32 << u64 << i64 << s << mar; }

    void unmarshal(MarshallableUnPack & p) override { p >> b >> u8 >> i8 >> u16 >> i16 >> u32 >> i32 >> u64 >> i64 >> s >> mar; }
};

struct MyMarshallableV2 : public Marshallable
{
    bool b;
    UInt8 u8;
    Int8 i8;
    UInt16 u16;
    Int16 i16;
    UInt32 u32;
    Int32 i32;
    UInt64 u64;
    Int64 i64;
    String s;
    InnerMarshallableV2 mar;
    String s1;
    MyMarshallableV2() = default;
    void marshal(MarshallablePack & p) const override { p << b << u8 << i8 << u16 << i16 << u32 << i32 << u64 << i64 << s << mar << s1; }

    void unmarshal(MarshallableUnPack & p) override
    {
        p >> b >> u8 >> i8 >> u16 >> i16 >> u32 >> i32 >> u64 >> i64 >> s >> mar;
        if (!p.eof())
            p >> s1;
    }
};

TEST(Marshallable, marshal)
{
    MyMarshallableV1 m1;
    m1.b = true;
    m1.i8 = 1;
    m1.u8 = 2;
    m1.u16 = 3;
    m1.i16 = 4;
    m1.u32 = 5;
    m1.i32 = 6;
    m1.u64 = 7;
    m1.i64 = 8;
    m1.s = "abc";
    m1.mar.x = 9;
    String buffer;
    packageToString(m1, buffer);

    MyMarshallableV1 m2;
    stringToPackage(buffer, m2);

    ASSERT_TRUE(
        m1.b == m2.b && m1.i8 == m2.i8 && m1.u8 == m2.u8 && m1.i16 == m2.i16 && m1.u16 == m2.u16 && m1.i32 == m2.i32 && m1.u32 == m2.u32
        && m1.i64 == m2.i64 && m1.u64 == m2.u64 && m1.s == m2.s && m1.mar.x == m2.mar.x);
}

TEST(Marshallable, compatibility1)
{
    MyMarshallableV1 m1;
    m1.b = true;
    m1.i8 = 1;
    m1.u8 = 2;
    m1.u16 = 3;
    m1.i16 = 4;
    m1.u32 = 5;
    m1.i32 = 6;
    m1.u64 = 7;
    m1.i64 = 8;
    m1.s = "abc";
    m1.mar.x = 9;
    String buffer;
    packageToString(m1, buffer);

    MyMarshallableV2 m2;
    stringToPackage(buffer, m2);

    ASSERT_TRUE(
        m1.b == m2.b && m1.i8 == m2.i8 && m1.u8 == m2.u8 && m1.i16 == m2.i16 && m1.u16 == m2.u16 && m1.i32 == m2.i32 && m1.u32 == m2.u32
        && m1.i64 == m2.i64 && m1.u64 == m2.u64 && m1.s == m2.s && m1.mar.x == m2.mar.x);

    ASSERT_TRUE(m2.s1.empty() && m2.mar.y == 0);
}

TEST(Marshallable, compatibility2)
{
    MyMarshallableV2 m2;
    m2.b = true;
    m2.i8 = 1;
    m2.u8 = 2;
    m2.u16 = 3;
    m2.i16 = 4;
    m2.u32 = 5;
    m2.i32 = 6;
    m2.u64 = 7;
    m2.i64 = 8;
    m2.s = "abc";
    m2.mar.x = 9;
    String buffer;
    packageToString(m2, buffer);

    MyMarshallableV1 m1;
    stringToPackage(buffer, m1);

    ASSERT_TRUE(
        m1.b == m2.b && m1.i8 == m2.i8 && m1.u8 == m2.u8 && m1.i16 == m2.i16 && m1.u16 == m2.u16 && m1.i32 == m2.i32 && m1.u32 == m2.u32
        && m1.i64 == m2.i64 && m1.u64 == m2.u64 && m1.s == m2.s && m1.mar.x == m2.mar.x);
}
