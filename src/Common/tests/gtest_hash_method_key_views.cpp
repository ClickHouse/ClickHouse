#include <Columns/ColumnsNumber.h>
#include <Common/ColumnsHashing.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

using namespace DB;

namespace
{

using StringValue = std::pair<std::string_view, UInt64>;
using StringMethod = ColumnsHashing::HashMethodString<StringValue, UInt64, true, false>;
using FixedStringMethod = ColumnsHashing::HashMethodFixedString<StringValue, UInt64, true, false>;
using PackedStringMethod = ColumnsHashing::HashMethodPackedString<std::pair<PackedStringRef, UInt64>, UInt64, false>;

template <typename Method>
void checkKeyBytes(const Method & method, const IColumn & column)
{
    Arena pool;
    EXPECT_TRUE(method.keyViewsAreBlockStable());
    for (size_t row = 0; row < column.size(); ++row)
    {
        const auto key = method.getKeyBytes(row);
        const auto source = column.getDataAt(row);
        EXPECT_EQ(key, source);
        EXPECT_EQ(key.data(), source.data());
        auto holder = method.getKeyHolder(row, pool);
        EXPECT_EQ(static_cast<std::string_view>(keyHolderGetKey(holder)), key);
        keyHolderDiscardKey(holder);
        EXPECT_EQ(key, column.getDataAt(row));
    }
}

ColumnString::MutablePtr stringKeys()
{
    auto column = ColumnString::create();
    column->insertDefault();
    for (size_t size : {1, 8, 16, 24, 32, 128})
    {
        const std::string key(size, static_cast<char>('a' + size % 26));
        column->insertData(key.data(), key.size());
    }
    column->insertData("a\0b", 3);
    return column;
}

template <typename Method>
void checkDictionaryKeyBytes(const DataTypePtr & type)
{
    const DataTypeLowCardinality low_cardinality_type(type);
    auto column = low_cardinality_type.createColumn();
    /// Repeated rows make source positions differ from the dictionary positions that hold their bytes.
    for (std::string_view key : {"second", "first", "second", "third", ""})
        column->insertData(key.data(), key.size());

    using DictionaryMethod = ColumnsHashing::HashMethodSingleLowCardinalityColumn<Method, UInt64, false>;
    const DictionaryMethod method({column.get()}, {}, DictionaryMethod::createContext({}));
    checkKeyBytes(method, *column);
}

}

TEST(HashMethodKeyViews, StringBytes)
{
    const auto column = stringKeys();
    const StringMethod method({column.get()}, {}, nullptr);
    checkKeyBytes(method, *column);
    using ViewMethod = ColumnsHashing::HashMethodString<StringValue, UInt64, false, false>;
    const ViewMethod view_method({column.get()}, {}, nullptr);
    checkKeyBytes(view_method, *column);
}

TEST(HashMethodKeyViews, PackedStringBytes)
{
    const auto column = stringKeys();
    const PackedStringMethod method({column.get()}, {}, nullptr);
    checkKeyBytes(method, *column);
}

TEST(HashMethodKeyViews, FixedStringBytes)
{
    auto column = ColumnFixedString::create(8);
    column->insertData("first", 5);
    column->insertDefault();
    column->insertData("a\0b", 3);
    const FixedStringMethod method({column.get()}, {}, nullptr);
    checkKeyBytes(method, *column);
    using ViewMethod = ColumnsHashing::HashMethodFixedString<StringValue, UInt64, false, false>;
    const ViewMethod view_method({column.get()}, {}, nullptr);
    checkKeyBytes(view_method, *column);
}

TEST(HashMethodKeyViews, DictionaryStringBytes)
{
    checkDictionaryKeyBytes<StringMethod>(std::make_shared<DataTypeString>());
    checkDictionaryKeyBytes<PackedStringMethod>(std::make_shared<DataTypeString>());
    checkDictionaryKeyBytes<FixedStringMethod>(std::make_shared<DataTypeFixedString>(8));
}

TEST(HashMethodKeyViews, NumericDictionaryHasNoByteView)
{
    using NumericMethod = ColumnsHashing::HashMethodOneNumber<std::pair<UInt64, UInt64>, UInt64, UInt64, false>;
    using DictionaryMethod = ColumnsHashing::HashMethodSingleLowCardinalityColumn<NumericMethod, UInt64, false>;
    const auto has_key_bytes = []<typename Method>()
    {
        return requires (const Method & method) { method.getKeyBytes(0); };
    };
    EXPECT_FALSE(has_key_bytes.template operator()<NumericMethod>());
    EXPECT_FALSE(has_key_bytes.template operator()<DictionaryMethod>());
    auto column = ColumnUInt64::create();
    column->insertValue(1);
    const NumericMethod method({column.get()}, {}, nullptr);
    EXPECT_TRUE(method.keyViewsAreBlockStable());
}

TEST(HashMethodKeyViews, BatchSerializedBytesSurviveOtherRows)
{
    using Method = ColumnsHashing::HashMethodSerialized<StringValue, char *, false, true>;
    auto column = ColumnString::create();
    column->insertData("first", 5);
    column->insertData("second", 6);
    const Method method({column.get()}, {}, Method::createContext({}));
    ASSERT_TRUE(method.keyViewsAreBlockStable());
    Arena pool;
    auto first = method.getKeyHolder(0, pool);
    const auto key = keyHolderGetKey(first);
    const std::string original(key);
    keyHolderDiscardKey(first);
    auto second = method.getKeyHolder(1, pool);
    EXPECT_EQ(key, original);
    EXPECT_NE(key, keyHolderGetKey(second));
    keyHolderDiscardKey(second);
}

TEST(HashMethodKeyViews, ArenaSerializedBytesAreTemporary)
{
    using Method = ColumnsHashing::HashMethodSerialized<StringValue, char *, false, false>;
    const auto column = stringKeys();
    const Method method({column.get()}, {}, Method::createContext({}));
    EXPECT_FALSE(method.keyViewsAreBlockStable());
}

#if !defined(__aarch64__)
TEST(HashMethodKeyViews, ScratchSerializedBytesAreTemporary)
{
    using Method = ColumnsHashing::HashMethodSerialized<StringValue, char *, false, true>;
    auto column = ColumnString::create();
    for (char value : {'a', 'b'})
    {
        const std::string key(256, value);
        column->insertData(key.data(), key.size());
    }
    const Method method({column.get()}, {}, Method::createContext({}));
    EXPECT_FALSE(method.keyViewsAreBlockStable());
}
#endif
