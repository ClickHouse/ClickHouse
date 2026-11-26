#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

namespace
{

String writeNative(const ColumnPtr & column, const String & type_name)
{
    auto type = DataTypeFactory::instance().get(type_name);
    Block block{ColumnWithTypeAndName(column, type, "x")};
    WriteBufferFromOwnString out;
    NativeWriter writer(out, DBMS_TCP_PROTOCOL_VERSION, std::make_shared<const Block>(block.cloneEmpty()));
    writer.write(block);
    out.finalize();
    return out.str();
}

ColumnWithTypeAndName readNative(const String & data, const String & header_type_name, std::optional<FormatSettings> settings)
{
    auto type = DataTypeFactory::instance().get(header_type_name);
    Block header{ColumnWithTypeAndName(type->createColumn(), type, "x")};
    ReadBufferFromString in(data);
    NativeReader reader(in, header, DBMS_TCP_PROTOCOL_VERSION, settings);
    return reader.read().getByName("x");
}

std::optional<FormatSettings> withoutConversion()
{
    FormatSettings settings;
    settings.native.allow_types_conversion = false;
    return settings;
}

String rowsAsString(const ColumnWithTypeAndName & column)
{
    String result;
    for (size_t i = 0; i < column.column->size(); ++i)
        result += (i ? "," : "") + applyVisitor(FieldVisitorToString(), (*column.column)[i]);
    return result;
}

void expectTypeMismatch(const String & data, const String & header_type_name)
{
    try
    {
        readNative(data, header_type_name, withoutConversion());
        FAIL() << "expected TYPE_MISMATCH for " << header_type_name;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TYPE_MISMATCH) << e.message();
    }
}

}

/// The default kinds of `NativeReader` give `Replicated` and `Sparse` columns, also to a header of another type.
TEST(NativeReaderWrappedColumns, ReplicatedArray)
{
    auto array = ColumnArray::create(ColumnUInt8::create());
    array->insert(Array{Field(UInt64(1)), Field(UInt64(2))});
    auto indexes = ColumnUInt8::create();
    indexes->insertValue(0);
    indexes->insertValue(0);
    const String data = writeNative(ColumnReplicated::create(ColumnPtr(std::move(array)), ColumnPtr(std::move(indexes))), "Array(UInt8)");

    ASSERT_TRUE(readNative(data, "Array(UInt8)", FormatSettings{}).column->isReplicated());

    auto converted = readNative(data, "Array(UInt16)", FormatSettings{});
    EXPECT_EQ(converted.type->getName(), "Array(UInt16)");
    EXPECT_EQ(rowsAsString(converted), "[1, 2],[1, 2]");

    expectTypeMismatch(data, "Array(UInt16)");
}

TEST(NativeReaderWrappedColumns, SparseNullable)
{
    auto values = ColumnNullable::create(ColumnUInt8::create(), ColumnUInt8::create());
    values->insertDefault();
    values->insert(Field(UInt64(5)));
    auto offsets = ColumnUInt64::create();
    offsets->insertValue(3);
    const String data = writeNative(ColumnSparse::create(std::move(values), std::move(offsets), 4), "Nullable(UInt8)");

    ASSERT_TRUE(readNative(data, "Nullable(UInt8)", FormatSettings{}).column->isSparse());

    auto converted = readNative(data, "Nullable(UInt16)", FormatSettings{});
    EXPECT_EQ(converted.type->getName(), "Nullable(UInt16)");
    EXPECT_EQ(rowsAsString(converted), "NULL,NULL,NULL,5");

    FormatSettings null_as_default = *withoutConversion();
    null_as_default.null_as_default = true;
    auto not_null = readNative(data, "UInt8", null_as_default);
    EXPECT_EQ(not_null.type->getName(), "UInt8");
    EXPECT_EQ(rowsAsString(not_null), "0,0,0,5");

    expectTypeMismatch(data, "Nullable(UInt16)");
}

TEST(NativeReaderWrappedColumns, SparseTupleElement)
{
    auto values = ColumnNullable::create(ColumnUInt8::create(), ColumnUInt8::create());
    values->insertDefault();
    values->insert(Field(UInt64(5)));
    auto offsets = ColumnUInt64::create();
    offsets->insertValue(3);
    Columns elements{ColumnSparse::create(std::move(values), std::move(offsets), 4)};
    const String data = writeNative(ColumnTuple::create(elements), "Tuple(a Nullable(UInt8))");

    ASSERT_TRUE(assert_cast<const ColumnTuple &>(*readNative(data, "Tuple(a Nullable(UInt8))", FormatSettings{}).column)
                    .getColumn(0)
                    .isSparse());

    auto converted = readNative(data, "Tuple(a Nullable(UInt16))", FormatSettings{});
    EXPECT_EQ(converted.type->getName(), "Tuple(a Nullable(UInt16))");
    EXPECT_EQ(rowsAsString(converted), "tuple(NULL),tuple(NULL),tuple(NULL),tuple(5)");

    expectTypeMismatch(data, "Tuple(a Nullable(UInt16))");
}
