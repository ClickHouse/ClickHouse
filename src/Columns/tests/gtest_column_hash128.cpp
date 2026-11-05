/// The expected checksums below were computed by the master code that hashes one row at a time
/// (`updateHashWithValue` of each key column into one `SipHash`) and must not change.

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Arena.h>
#include <Common/ColumnsHashing.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>
#include <fmt/format.h>

#include <bit>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <string>
#include <vector>

using namespace DB;

namespace
{

struct Case
{
    const char * name;
    UInt64 expected_lo;
    UInt64 expected_hi;
};

const Case expected_checksums[] = {
    {"UInt8", 0x042cac2d6d6acd0d, 0xbe35cb2f2df35314},
    {"UInt16", 0xc41a439c13e45ee0, 0xc300e3fb49a8fe81},
    {"UInt32", 0xa8287df1ec4a5ad1, 0x95d54eb43d9d8c12},
    {"UInt64", 0x3da643f4475e78db, 0xb8ac48cd91b535d9},
    {"UInt128", 0x1a80fbeef97cb187, 0x5b95617f62177b42},
    {"UInt256", 0x7e543b02b7777ee9, 0x5e3f64c31ca130ea},
    {"Int8", 0x83e59d8a82dece89, 0xa77a4474de1512a6},
    {"Int16", 0xad822f5bba737b81, 0xe37869c855a36543},
    {"Int32", 0xce9b646f7150d4c3, 0xb0c2362767c93441},
    {"Int64", 0x5191d9a6974d17b7, 0x9031e65cdddb50b1},
    {"Int128", 0x0bb051d8f8681ea9, 0x8c4e90a2d10cf6df},
    {"Int256", 0x76d87d1b847d1036, 0x939e75504bffb819},
    {"Float32", 0x732f47302682a61f, 0xe07eaf5d68f37c37},
    {"Float64", 0x0f633b4da0ebc8ec, 0x19e1d15264863744},
    {"BFloat16", 0x2972952fcd2005ad, 0x64e6c16bdaf71cdf},
    {"Date", 0xaa40151bc1315f62, 0x12588b755343b919},
    {"DateTime", 0xdf4e18a72546860d, 0xa4cda3eaad15bf6a},
    {"IPv4", 0xed65379dbfc7160f, 0x5e4789ef2bd05497},
    {"Enum8", 0x67d323f6ae18c6c8, 0x8284deebe20532a3},
    {"UUID", 0x31a98f4df8a76ce9, 0xa03f0fd716d21a67},
    {"IPv6", 0xf02e18ab933bfe7d, 0x3650f483376aa702},
    {"Decimal32(4)", 0xb55f76f44f28cf10, 0x5c7ede699110450c},
    {"Decimal64(8)", 0x369a1feb5a2f386a, 0x11731b48c82742c1},
    {"Decimal128(20)", 0x39ffd4062dd930f1, 0x064effd8591148ee},
    {"Decimal256(40)", 0x66b5608d761a3ad5, 0x76ab7002c79468c4},
    {"DateTime64(3)", 0xa8b3e8f449de0ff4, 0x0d8980bfbc138d1a},
    {"FixedString(1)", 0x1828b73e4c4f78ad, 0x7d16634ac29c130e},
    {"FixedString(5)", 0x29139e94d76b436f, 0x2abc52d0a11593c1},
    {"FixedString(16)", 0xcd284b8b5362f009, 0x62d69acb94deff58},
    {"FixedString(33)", 0x96cacad1546e6e36, 0xfe3d7b5a15dc4eb4},
    {"String", 0x9547b6a0ed7c48a3, 0x9b58daff6e75ac77},
    {"Nullable(UInt32)", 0x1a537e6521b4c1f0, 0x14ce9ada559d3e1b},
    {"Nullable(String)", 0xcf2bf358fde2bc0f, 0xa2e541cb9d174732},
    {"Array(UInt8)", 0x6531842ca7e0202b, 0x73b837135e3041cb},
    {"Array(String)", 0x2ae729bfa733972f, 0x3058c5e9b4f08617},
    {"Array(Array(UInt32))", 0xa6b358c83bf1f320, 0xfe7a16647a3f09ad},
    {"Array(Nullable(String))", 0xd0dc700519aca289, 0xbb4c7a5c3d833e7c},
    {"Map(String, UInt64)", 0x945770ccde51eff0, 0x2bf175849d6e3958},
    {"Map(UInt32, Array(String))", 0xfc5136f002283576, 0x0a08fddb87cfdb9b},
    {"Tuple(UInt64, String)", 0x6f5575826e284285, 0xf71ffcda5995e580},
    {"Tuple()", 0x74b03d4b95442658, 0xa2f036c7729b8ae6},
    {"Variant(UInt64, String, Array(UInt32))", 0xd7ead19a5a6c34f5, 0xa00fd823f014eef1},
    {"Dynamic", 0x4999fbef8995b198, 0x629a8684fe2e1a5b},
    {"Dynamic(max_types=1)", 0x55b3773eeaa3954d, 0xc024c654c1c31c75},
    {"JSON(a UInt32, b.c String)", 0xf5b99c2bedf915c1, 0x15a74c7d0975ab35},
    {"JSON", 0x05092e626e80cb68, 0x8d8f5318edcb97b5},
    {"JSON(max_dynamic_paths=1, t Int64)", 0x605b6c31225b9cfc, 0xe80bb96be0bb908b},
    {"AggregateFunction(sum, UInt64)", 0x4d249e632372e7e9, 0xbc7259d952621992},
    {"AggregateFunction(uniq, String)", 0xd4640e5347f6b26d, 0x228df63fd8360cdb},
    {"LowCardinality(String)", 0x0654bfc202097e42, 0xa6780e9580b975e5},
    {"LowCardinality(Nullable(String))", 0x5484a4f8bc4756cd, 0x14c06ea45d9c2c7d},
    {"Const(String)", 0xbb115ff34dc27ce3, 0xee1f0547ec7c6236},
    {"Const(Nullable(UInt32))", 0xeb30d5aa43eb5f06, 0x2e82c026d87d6d7b},
    {"Sparse(UInt64)", 0x569c63079e9735a5, 0xcd39d85fda25f9a5},
    {"Sparse(String)", 0xf028207a663adb10, 0x1752bc6874745c5e},
    {"Replicated(String)", 0x2791ea74dedc06a8, 0x4a44b0bee1e75944},
    {"QBit(Float32, 8)", 0x0f7e1a5836c72707, 0x2caa5f013e1aeeda},
    {"QBit(Float64, 3)", 0xe85e7f35026408fc, 0xfd73a2a634fdfaf8},
    {"Nothing", 0x74b03d4b95442658, 0xa2f036c7729b8ae6},
    {"Nullable(Nothing)", 0xdcffaafbd3065076, 0x600218c08c4b4d6c},
    {"{UInt64, String}", 0x87b8d498371fbd3e, 0xf68aafb2b753d86c},
    {"{Nullable(String), Array(UInt32), LowCardinality(String)}", 0x4a6e3fddfdec2b6e, 0xb8154c31436e7ddf},
    {"{Tuple(UInt8, FixedString(3)), Decimal64(4), IPv6}", 0x9cb325747f8fe133, 0xc1a278d954d4ec4f},
    {"{Dynamic, JSON}", 0x30fef15827856065, 0x7c3fad8ce753b468},
    {"PARTITION BY (s, ns, lc, fs, u, d, dec, i128, t, a)", 0x72a396e8377c6673, 0x3b2f9a4fe409727d},
};

constexpr size_t num_rows = 1003;

UInt64 mix(UInt64 x)
{
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

/// `salt` changes only the mixed rows, so that classes that hash the same bytes do not share a checksum.
template <typename T>
T integerAt(size_t row, UInt64 salt = 0)
{
    using Limits = std::numeric_limits<T>;
    switch (row)
    {
        case 0: return Limits::min();
        case 1: return Limits::max();
        case 2: return T(0);
        case 3: return T(1);
        case 4: return static_cast<T>(Limits::min() + 1);
        case 5: return static_cast<T>(Limits::max() - 1);
        default: break;
    }
    if (row % 3 == 0)
        return static_cast<T>(static_cast<Int64>(row) - 500);
    const UInt64 seed = row + (salt << 32);
    if constexpr (sizeof(T) <= 8)
    {
        return static_cast<T>(mix(seed));
    }
    else
    {
        using Unsigned = std::conditional_t<sizeof(T) == 16, UInt128, UInt256>;
        Unsigned bits = 0;
        for (size_t k = 0; k < sizeof(T) / 8; ++k)
            bits = (bits << 64) | Unsigned(mix(seed * 4 + k));
        return static_cast<T>(bits);
    }
}

template <typename T>
T floatAt(size_t row)
{
    using Bits = std::conditional_t<sizeof(T) == 4, UInt32, UInt64>;
    using Limits = std::numeric_limits<T>;
    const Bits sign = Bits(1) << (sizeof(T) * 8 - 1);
    const Bits quiet = Bits(1) << (Limits::digits - 2);
    const Bits inf = std::bit_cast<Bits>(Limits::infinity());
    const T specials[] = {
        T(0), -T(0), Limits::infinity(), -Limits::infinity(),
        std::bit_cast<T>(inf | quiet), std::bit_cast<T>(sign | inf | quiet), std::bit_cast<T>(inf | quiet | 1), std::bit_cast<T>(inf | 1),
        Limits::denorm_min(), -Limits::denorm_min(), std::bit_cast<T>(quiet), std::bit_cast<T>(sign | (quiet - 1)),
        Limits::min(), Limits::max(), Limits::lowest(), Limits::epsilon(), T(1), T(-1)};
    if (row < std::size(specials))
        return specials[row];
    if (row % 2 == 0)
        return static_cast<T>(static_cast<Int64>(row) - 500) / T(8);
    return std::bit_cast<T>(static_cast<Bits>(mix(row)));
}

BFloat16 bfloat16At(size_t row)
{
    static constexpr UInt16 specials[] = {
        0x0000, 0x8000, 0x7f80, 0xff80, 0x7fc0, 0xffc0, 0x7fc1, 0x7f81, 0x0001, 0x8001, 0x0040, 0x807f,
        0x0080, 0x7f7f, 0xff7f, 0x3c00, 0x3f80, 0xbf80};
    if (row < std::size(specials))
        return BFloat16::fromBits(specials[row]);
    if (row % 2 == 0)
        return BFloat16(static_cast<Float32>(static_cast<Int64>(row) - 500) / 8);
    return BFloat16::fromBits(static_cast<UInt16>(mix(row)));
}

String bytesAt(size_t seed, size_t length)
{
    String res(length, '\0');
    for (size_t k = 0; k < length; ++k)
        res[k] = static_cast<char>((seed * 131 + k * 29 + (k >> 8)) & 0xFF);
    return res;
}

/// Every length 0..300 in turn, and about 4 KB in rows 249, 499, 749, 999.
String stringAt(size_t row)
{
    return bytesAt(row, row % 250 == 249 ? 4090 + row % 11 : row % 301);
}

template <typename ElementAt>
Array arrayOf(size_t length, ElementAt && element_at)
{
    Array res;
    for (size_t k = 0; k < length; ++k)
        res.push_back(element_at(k));
    return res;
}

Field nullableUInt32At(size_t row)
{
    return row % 7 == 0 ? Field() : Field(integerAt<UInt32>(row));
}

Field nullableStringAt(size_t row)
{
    return row % 7 == 0 ? Field() : Field(stringAt(row));
}

Field lowCardinalityStringAt(size_t row)
{
    return Field(stringAt(row % 37));
}

Field arrayUInt8At(size_t row)
{
    return arrayOf(row % 9, [row](size_t k) { return Field(integerAt<UInt8>(row + k * 1009)); });
}

Field arrayUInt32At(size_t row)
{
    return arrayOf(row % 6, [row](size_t k) { return Field(integerAt<UInt32>(row + k * 1009)); });
}

Field variantAt(size_t row)
{
    switch (row % 4)
    {
        case 0: return Field();
        case 1: return Field(integerAt<UInt64>(row));
        case 2: return Field(stringAt(row));
        default: return arrayUInt32At(row);
    }
}

/// A value goes into the first existing variant that accepts its `Field` type, and `ColumnVector::tryInsert` narrows
/// without a check, so the first value of each kind is the one that needs the widest type (Int64 min, UInt64 max).
Field dynamicValueAt(size_t row)
{
    const size_t seed = row / 9;
    switch (row % 9)
    {
        case 0: return Field();
        case 1: return Field(integerAt<Int64>(seed));
        case 2: return Field(integerAt<UInt64>(seed + 1));
        case 3: return Field(floatAt<Float64>(seed));
        case 4: return Field(stringAt(row));
        case 5: return arrayOf((seed + 1) % 4, [row](size_t k) { return Field(stringAt(row + k)); });
        case 6: return Tuple{Field(stringAt(seed % 31)), Field(floatAt<Float64>(seed + 3))};
        case 7: return Field(UUID(integerAt<UInt128>(seed)));
        default: return DecimalField<Decimal64>(Decimal64(integerAt<Int64>(seed) % 1000000000000000000), 3);
    }
}

Field typedObjectAt(size_t row)
{
    Object object;
    if (row % 4 != 0)
        object["a"] = Field(integerAt<UInt32>(row));
    if (row % 3 != 0)
        object["b.c"] = Field(stringAt(row));
    return object;
}

Field objectAt(size_t row)
{
    Object object;
    object["t"] = Field(integerAt<Int64>(row + 1));
    object["id"] = Field(integerAt<Int64>(row));
    if (row % 5 != 0)
        object["name"] = Field(stringAt(row % 97));
    if (row % 3 == 1)
        object["a.b"] = Field(floatAt<Float64>(row));
    object["mixed"] = dynamicValueAt(row);
    object["p" + std::to_string(row % 4)] = Field(stringAt(row % 13));
    return object;
}

String qbitTextAt(size_t row, size_t dimension)
{
    static const char * const specials[] = {"nan", "-nan", "inf", "-inf", "-0", "0", "1e-40", "1e-310", "3.4028235e38", "-1.5"};
    String res = "[";
    for (size_t k = 0; k < dimension; ++k)
    {
        const size_t seed = row * dimension + k;
        if (k != 0)
            res += ", ";
        if (seed % 5 == 0)
            res += specials[seed / 5 % std::size(specials)];
        else
            res += fmt::format("{}.{}", static_cast<Int64>(mix(seed) % 20001) - 10000, seed % 1000);
    }
    return res + "]";
}

DataTypePtr typeOf(const String & type_name)
{
    return DataTypeFactory::instance().get(type_name);
}

ColumnPtr fromFields(const String & type_name, const std::function<Field(size_t)> & field_at, size_t rows = num_rows)
{
    auto column = typeOf(type_name)->createColumn();
    for (size_t row = 0; row < rows; ++row)
        column->insert(field_at(row));
    return column;
}

template <typename ColumnType, typename ValueAt>
ColumnPtr fromValues(const String & type_name, ValueAt && value_at)
{
    auto column = typeOf(type_name)->createColumn();
    auto & data = assert_cast<ColumnType &>(*column).getData();
    for (size_t row = 0; row < num_rows; ++row)
        data.push_back(value_at(row));
    return column;
}

ColumnPtr fromText(const String & type_name, const std::function<String(size_t)> & text_at)
{
    auto type = typeOf(type_name);
    auto serialization = type->getDefaultSerialization();
    auto column = type->createColumn();
    for (size_t row = 0; row < num_rows; ++row)
    {
        const String text = text_at(row);
        ReadBufferFromString buf(text);
        serialization->deserializeWholeText(*column, buf, FormatSettings{});
    }
    return column;
}

template <typename T>
ColumnPtr integers(const String & type_name)
{
    return fromValues<ColumnVector<T>>(type_name, [](size_t row) { return integerAt<T>(row); });
}

template <typename T>
ColumnPtr decimals(const String & type_name, UInt64 salt)
{
    return fromValues<ColumnDecimal<T>>(type_name, [salt](size_t row) { return T(integerAt<typename T::NativeType>(row, salt)); });
}

ColumnPtr strings()
{
    return fromFields("String", [](size_t row) { return Field(stringAt(row)); });
}

ColumnPtr fixedStrings(size_t n)
{
    return fromFields(fmt::format("FixedString({})", n), [n](size_t row) { return Field(bytesAt(row, row % (n + 1))); });
}

ColumnPtr dynamicColumn(const String & type_name)
{
    auto column = fromFields(type_name, dynamicValueAt);
    const auto & dynamic = assert_cast<const ColumnDynamic &>(*column);
    if (dynamic.getMaxDynamicTypes() == 1)
        EXPECT_GT(dynamic.getSharedVariant().size(), 0);
    else
        EXPECT_EQ(dynamic.getSharedVariant().size(), 0);
    return column;
}

ColumnPtr objectColumn(const String & type_name, const std::function<Field(size_t)> & object_at)
{
    auto column = fromFields(type_name, object_at);
    const auto & object = assert_cast<const ColumnObject &>(*column);
    if (object.getMaxDynamicPaths() == 1)
        EXPECT_GT(object.getSharedDataPathsAndValues().first->size(), 0);
    else
        EXPECT_EQ(object.getSharedDataPathsAndValues().first->size(), 0);
    return column;
}

ColumnPtr aggregateStates(const String & type_name, const ColumnPtr & arguments, size_t max_values_per_row)
{
    auto column = typeOf(type_name)->createColumn();
    auto & states = assert_cast<ColumnAggregateFunction &>(*column);
    auto function = states.getAggregateFunction();
    Arena & arena = states.createOrGetArena();
    const IColumn * argument_columns[] = {arguments.get()};
    size_t next = 0;
    for (size_t row = 0; row < num_rows; ++row)
    {
        states.insertDefault();
        for (size_t k = 0; k < row % (max_values_per_row + 1); ++k)
            function->add(states.getData()[row], argument_columns, next++ % arguments->size(), &arena);
    }
    return column;
}

ColumnPtr sparse(const String & type_name, size_t period, const std::function<Field(size_t)> & value_at)
{
    auto column = ColumnSparse::create(typeOf(type_name)->createColumn());
    for (size_t row = 0; row < num_rows; ++row)
    {
        if (row % period == 3)
            column->insert(value_at(row));
        else
            column->insertDefault();
    }
    EXPECT_LT(column->getOffsetsData().size(), num_rows / 5);
    return column;
}

ColumnPtr replicatedStrings()
{
    MutableColumnPtr nested = typeOf("String")->createColumn();
    for (size_t k = 0; k < 41; ++k)
        nested->insert(Field(stringAt(k * 7)));
    auto indexes = ColumnUInt8::create();
    for (size_t row = 0; row < num_rows; ++row)
        indexes->getData().push_back(static_cast<UInt8>(row * 17 % 41));
    MutableColumnPtr indexes_column = std::move(indexes);
    return ColumnReplicated::create(std::move(nested), std::move(indexes_column));
}

ColumnPtr constOf(const String & type_name, const Field & value)
{
    return ColumnConst::create(fromFields(type_name, [&value](size_t) { return value; }, 1), num_rows);
}

/// The partition key columns of an INSERT block of a table with `PARTITION BY (s, ns, lc, fs, u, d, dec, i128, t, a)`,
/// as `buildScatterSelector` receives them.
Columns partitionKey()
{
    return {
        strings(),
        fromFields("Nullable(String)", nullableStringAt),
        fromFields("LowCardinality(String)", lowCardinalityStringAt),
        fixedStrings(3),
        fromValues<ColumnUUID>("UUID", [](size_t row) { return UUID(integerAt<UInt128>(row, 1)); }),
        fromValues<ColumnUInt16>("Date", [](size_t row) { return static_cast<UInt16>(19000 + row % 400); }),
        fromFields("Decimal64(2)", [](size_t row) { return Field(DecimalField<Decimal64>(Decimal64(integerAt<Int64>(row)), 2)); }),
        integers<Int128>("Int128"),
        fromFields("Tuple(UInt8, String)", [](size_t row) { return Tuple{Field(integerAt<UInt8>(row)), Field(stringAt(row % 50))}; }),
        fromFields("Array(UInt8)", arrayUInt8At),
    };
}

struct NamedKey
{
    std::string name;
    std::function<Columns()> build;
};

std::vector<NamedKey> namedKeys()
{
    return {
        {"UInt8", [] { return Columns{integers<UInt8>("UInt8")}; }},
        {"UInt16", [] { return Columns{integers<UInt16>("UInt16")}; }},
        {"UInt32", [] { return Columns{integers<UInt32>("UInt32")}; }},
        {"UInt64", [] { return Columns{integers<UInt64>("UInt64")}; }},
        {"UInt128", [] { return Columns{integers<UInt128>("UInt128")}; }},
        {"UInt256", [] { return Columns{integers<UInt256>("UInt256")}; }},
        {"Int8", [] { return Columns{integers<Int8>("Int8")}; }},
        {"Int16", [] { return Columns{integers<Int16>("Int16")}; }},
        {"Int32", [] { return Columns{integers<Int32>("Int32")}; }},
        {"Int64", [] { return Columns{integers<Int64>("Int64")}; }},
        {"Int128", [] { return Columns{integers<Int128>("Int128")}; }},
        {"Int256", [] { return Columns{integers<Int256>("Int256")}; }},
        {"Float32", [] { return Columns{fromValues<ColumnFloat32>("Float32", floatAt<Float32>)}; }},
        {"Float64", [] { return Columns{fromValues<ColumnFloat64>("Float64", floatAt<Float64>)}; }},
        {"BFloat16", [] { return Columns{fromValues<ColumnBFloat16>("BFloat16", bfloat16At)}; }},
        {"Date", [] { return Columns{fromValues<ColumnUInt16>("Date", [](size_t row) { return static_cast<UInt16>(row * 37); })}; }},
        {"DateTime", [] { return Columns{fromValues<ColumnUInt32>("DateTime", [](size_t row)
            {
                return static_cast<UInt32>(1700000000 + row * 3607);
            })}; }},
        {"IPv4", [] { return Columns{fromValues<ColumnIPv4>("IPv4", [](size_t row)
            {
                return IPv4(static_cast<UInt32>(mix(row + 5000)));
            })}; }},
        {"Enum8", [] { return Columns{fromValues<ColumnInt8>("Enum8('a' = -128, 'b' = 0, 'c' = 127)", [](size_t row)
            {
                static constexpr Int8 values[] = {-128, 0, 127};
                return values[row % 3];
            })}; }},
        {"UUID", [] { return Columns{fromValues<ColumnUUID>("UUID", [](size_t row) { return UUID(integerAt<UInt128>(row, 1)); })}; }},
        {"IPv6", [] { return Columns{fromValues<ColumnIPv6>("IPv6", [](size_t row) { return IPv6(integerAt<UInt128>(row, 2)); })}; }},
        {"Decimal32(4)", [] { return Columns{decimals<Decimal32>("Decimal32(4)", 1)}; }},
        {"Decimal64(8)", [] { return Columns{decimals<Decimal64>("Decimal64(8)", 1)}; }},
        {"Decimal128(20)", [] { return Columns{decimals<Decimal128>("Decimal128(20)", 1)}; }},
        {"Decimal256(40)", [] { return Columns{decimals<Decimal256>("Decimal256(40)", 1)}; }},
        {"DateTime64(3)", [] { return Columns{decimals<DateTime64>("DateTime64(3)", 2)}; }},
        {"FixedString(1)", [] { return Columns{fixedStrings(1)}; }},
        {"FixedString(5)", [] { return Columns{fixedStrings(5)}; }},
        {"FixedString(16)", [] { return Columns{fixedStrings(16)}; }},
        {"FixedString(33)", [] { return Columns{fixedStrings(33)}; }},
        {"String", [] { return Columns{strings()}; }},
        {"Nullable(UInt32)", [] { return Columns{fromFields("Nullable(UInt32)", nullableUInt32At)}; }},
        {"Nullable(String)", [] { return Columns{fromFields("Nullable(String)", nullableStringAt)}; }},
        {"Array(UInt8)", [] { return Columns{fromFields("Array(UInt8)", arrayUInt8At)}; }},
        {"Array(String)", [] { return Columns{fromFields("Array(String)", [](size_t row)
            {
                return arrayOf(row % 5, [row](size_t k) { return Field(stringAt(row * 5 + k)); });
            })}; }},
        {"Array(Array(UInt32))", [] { return Columns{fromFields("Array(Array(UInt32))", [](size_t row)
            {
                return arrayOf(row % 4, [row](size_t k)
                {
                    return arrayOf((row + k) % 3, [row, k](size_t j) { return Field(integerAt<UInt32>(row * 12 + k * 3 + j)); });
                });
            })}; }},
        {"Array(Nullable(String))", [] { return Columns{fromFields("Array(Nullable(String))", [](size_t row)
            {
                return arrayOf(row % 6, [row](size_t k) { return (row + k) % 3 == 0 ? Field() : Field(stringAt(row + k * 1009)); });
            })}; }},
        {"Map(String, UInt64)", [] { return Columns{fromFields("Map(String, UInt64)", [](size_t row)
            {
                Map map;
                for (size_t k = 0; k < row % 5; ++k)
                    map.push_back(Tuple{Field(bytesAt(row + k, (row + k) % 20)), Field(integerAt<UInt64>(row * 5 + k))});
                return map;
            })}; }},
        {"Map(UInt32, Array(String))", [] { return Columns{fromFields("Map(UInt32, Array(String))", [](size_t row)
            {
                Map map;
                for (size_t k = 0; k < row % 4; ++k)
                    map.push_back(Tuple{
                        Field(integerAt<UInt32>(row * 4 + k)),
                        arrayOf((row + k) % 3, [row, k](size_t j) { return Field(bytesAt(row + k + j, (row + j) % 40)); })});
                return map;
            })}; }},
        {"Tuple(UInt64, String)", [] { return Columns{fromFields("Tuple(UInt64, String)", [](size_t row)
            {
                return Tuple{Field(integerAt<UInt64>(row, 1)), Field(stringAt(row))};
            })}; }},
        {"Tuple()", [] { return Columns{fromFields("Tuple()", [](size_t) { return Tuple{}; })}; }},
        {"Variant(UInt64, String, Array(UInt32))", [] { return Columns{fromFields("Variant(UInt64, String, Array(UInt32))", variantAt)}; }},
        {"Dynamic", [] { return Columns{dynamicColumn("Dynamic")}; }},
        {"Dynamic(max_types=1)", [] { return Columns{dynamicColumn("Dynamic(max_types=1)")}; }},
        {"JSON(a UInt32, b.c String)", [] { return Columns{objectColumn("JSON(a UInt32, b.c String)", typedObjectAt)}; }},
        {"JSON", [] { return Columns{objectColumn("JSON", objectAt)}; }},
        {"JSON(max_dynamic_paths=1, t Int64)", [] { return Columns{objectColumn("JSON(max_dynamic_paths=1, t Int64)", objectAt)}; }},
        {"AggregateFunction(sum, UInt64)", []
            {
                return Columns{aggregateStates("AggregateFunction(sum, UInt64)", integers<UInt64>("UInt64"), 3)};
            }},
        {"AggregateFunction(uniq, String)", [] { return Columns{aggregateStates("AggregateFunction(uniq, String)", strings(), 5)}; }},
        {"LowCardinality(String)", [] { return Columns{fromFields("LowCardinality(String)", lowCardinalityStringAt)}; }},
        {"LowCardinality(Nullable(String))", [] { return Columns{fromFields("LowCardinality(Nullable(String))", [](size_t row)
            {
                return row % 7 == 0 ? Field() : lowCardinalityStringAt(row);
            })}; }},
        {"Const(String)", [] { return Columns{constOf("String", Field(stringAt(277)))}; }},
        {"Const(Nullable(UInt32))", [] { return Columns{constOf("Nullable(UInt32)", Field(UInt64(4000000000)))}; }},
        {"Sparse(UInt64)", [] { return Columns{sparse("UInt64", 13, [](size_t row) { return Field(integerAt<UInt64>(row)); })}; }},
        {"Sparse(String)", [] { return Columns{sparse("String", 11, [](size_t row) { return Field(stringAt(row)); })}; }},
        {"Replicated(String)", [] { return Columns{replicatedStrings()}; }},
        {"QBit(Float32, 8)", [] { return Columns{fromText("QBit(Float32, 8)", [](size_t row) { return qbitTextAt(row, 8); })}; }},
        {"QBit(Float64, 3)", [] { return Columns{fromText("QBit(Float64, 3)", [](size_t row) { return qbitTextAt(row, 3); })}; }},
        {"Nothing", [] { return Columns{ColumnNothing::create(num_rows)}; }},
        {"Nullable(Nothing)", [] { return Columns{fromFields("Nullable(Nothing)", [](size_t) { return Field(); })}; }},
        {"{UInt64, String}", [] { return Columns{integers<UInt64>("UInt64"), strings()}; }},
        {"{Nullable(String), Array(UInt32), LowCardinality(String)}", []
            {
                return Columns{
                    fromFields("Nullable(String)", nullableStringAt),
                    fromFields("Array(UInt32)", arrayUInt32At),
                    fromFields("LowCardinality(String)", lowCardinalityStringAt)};
            }},
        {"{Tuple(UInt8, FixedString(3)), Decimal64(4), IPv6}", []
            {
                return Columns{
                    fromFields("Tuple(UInt8, FixedString(3))", [](size_t row)
                    {
                        return Tuple{Field(integerAt<UInt8>(row)), Field(bytesAt(row, row % 4))};
                    }),
                    decimals<Decimal64>("Decimal64(4)", 0),
                    fromValues<ColumnIPv6>("IPv6", [](size_t row) { return IPv6(integerAt<UInt128>(row, 2)); })};
            }},
        {"{Dynamic, JSON}", [] { return Columns{dynamicColumn("Dynamic"), objectColumn("JSON", objectAt)}; }},
        {"PARTITION BY (s, ns, lc, fs, u, d, dec, i128, t, a)", [] { return partitionKey(); }},
    };
}

std::pair<UInt64, UInt64> checksumOfRowHashes(const Columns & keys)
{
    ColumnRawPtrs key_columns;
    for (const auto & key : keys)
        key_columns.push_back(key.get());

    SipHash checksum;
    for (size_t row = 0; row < num_rows; ++row)
    {
        checksum.update(static_cast<UInt64>(row));
        checksum.update(ColumnsHashing::hash128(row, key_columns.size(), key_columns));
    }
    UInt64 lo = 0;
    UInt64 hi = 0;
    checksum.get128(lo, hi);
    return {lo, hi};
}

}

TEST(ColumnsHashing, Hash128EveryColumnClass)
{
    tryRegisterAggregateFunctions();

    std::map<std::string_view, const Case *> expected;
    for (const auto & expected_case : expected_checksums)
        ASSERT_TRUE(expected.emplace(expected_case.name, &expected_case).second) << expected_case.name;

    const auto keys = namedKeys();
    String actual_table;
    bool all_equal = keys.size() == expected.size();
    for (const auto & key : keys)
    {
        const Columns columns = key.build();
        for (const auto & column : columns)
            ASSERT_EQ(column->size(), num_rows) << key.name;

        const auto [lo, hi] = checksumOfRowHashes(columns);
        actual_table += fmt::format("    {{\"{}\", 0x{:016x}, 0x{:016x}}},\n", key.name, lo, hi);

        const auto it = expected.find(key.name);
        if (it == expected.end() || it->second->expected_lo != lo || it->second->expected_hi != hi)
        {
            all_equal = false;
            ADD_FAILURE() << "hash128 changed for " << key.name;
        }
    }
    EXPECT_EQ(keys.size(), expected.size());

    if (!all_equal)
        std::cerr << "expected_checksums computed by this build:\n" << actual_table;
}
