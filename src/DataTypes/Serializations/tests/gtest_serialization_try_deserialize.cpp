#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// A serialization whose deserializeText always throws the configured error code.
/// Used to check that the tryDeserializeText "try-pattern" only swallows parse errors
/// and rethrows everything else (e.g. MEMORY_LIMIT_EXCEEDED).
class ThrowingSerialization : public SimpleTextSerialization
{
public:
    explicit ThrowingSerialization(int code_) : code(code_) {}

    /// Opt out of pooling so it can be wrapped without a precomputed hash.
    bool supportsPooling() const override { return false; }

    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override
    {
        throw Exception(code, "injected by ThrowingSerialization");
    }

    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override {}

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override { notImplemented(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override { notImplemented(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { notImplemented(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override { notImplemented(); }

private:
    int code;

    [[noreturn]] static void notImplemented()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented in ThrowingSerialization");
    }
};

/// A serialization that appends `rows_to_append` rows and only then fails, either by throwing a
/// parse error or by returning false. Readers behave like this: they fill the column incrementally
/// and discover the value is malformed at the end, so a failed try-parse must undo their work.
class InsertingThenFailingSerialization : public SimpleTextSerialization
{
public:
    InsertingThenFailingSerialization(size_t rows_to_append_, bool throw_parse_error_)
        : rows_to_append(rows_to_append_), throw_parse_error(throw_parse_error_)
    {
    }

    bool supportsPooling() const override { return false; }

    void deserializeText(IColumn & column, ReadBuffer &, const FormatSettings &, bool) const override
    {
        for (size_t i = 0; i < rows_to_append; ++i)
            column.insertDefault();
        if (throw_parse_error)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "injected by InsertingThenFailingSerialization");
    }

    /// The non-throwing failure path: append, then report failure without an exception. When
    /// configured to throw, defer to the base wrapper instead - it is the code under test there.
    bool tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override
    {
        if (throw_parse_error)
            return SimpleTextSerialization::tryDeserializeText(column, istr, settings, whole);
        for (size_t i = 0; i < rows_to_append; ++i)
            column.insertDefault();
        return false;
    }

    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override {}

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override { notImplemented(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override { notImplemented(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { notImplemented(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override { notImplemented(); }

private:
    size_t rows_to_append;
    bool throw_parse_error;

    [[noreturn]] static void notImplemented()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented in InsertingThenFailingSerialization");
    }
};

}

/// A failed `FixedString` try-parse must leave the column byte-identical. Asserting on `size()` is
/// not enough: it is `chars.size() / n`, so bytes appended by a reader that then returned false are
/// invisible to it, and they silently shift every value read afterwards.
TEST(SerializationTryDeserialize, FixedStringFailedParseLeavesBytesUnchanged)
{
    FormatSettings settings;
    SerializationPtr serialization = SerializationFixedString::create(4);

    auto column = ColumnFixedString::create(4);
    column->insertData("abcd", 4);
    const size_t size_before = column->size();
    const size_t bytes_before = column->getChars().size();

    /// Quoted value with no closing quote: the reader appends "ab" and then reports failure.
    ReadBufferFromMemory quoted("'ab");
    ASSERT_FALSE(serialization->tryDeserializeTextQuoted(*column, quoted, settings));
    ASSERT_EQ(column->size(), size_before);
    ASSERT_EQ(column->getChars().size(), bytes_before);

    /// Same for the JSON reader.
    ReadBufferFromMemory json("\"ab");
    ASSERT_FALSE(serialization->tryDeserializeTextJSON(*column, json, settings));
    ASSERT_EQ(column->size(), size_before);
    ASSERT_EQ(column->getChars().size(), bytes_before);

    /// The row that was already there must still read back intact.
    ASSERT_EQ(column->getDataAt(0), std::string_view("abcd"));
}

/// `SimpleTextSerialization::tryDeserializeText` wraps a throwing `deserializeText`, which may have
/// inserted rows before it threw. Those rows must not survive a failed try-parse.
TEST(SerializationTryDeserialize, RestoresColumnWhenDeserializeTextInsertsThenThrows)
{
    FormatSettings settings;
    SerializationPtr serialization = std::make_shared<InsertingThenFailingSerialization>(/*rows_to_append=*/1, /*throw_parse_error=*/true);

    auto column = ColumnUInt8::create();
    column->insertValue(7);

    ReadBufferFromMemory istr("x");
    ASSERT_FALSE(serialization->tryDeserializeTextQuoted(*column, istr, settings));
    ASSERT_EQ(column->size(), 1u);
    ASSERT_EQ(column->getElement(0), 7u);
}

/// The non-throwing failure path through a composite: a nested serialization that appends and then
/// returns false must leave the array and its nested column consistent, with nothing left behind.
TEST(SerializationTryDeserialize, NestedReturningFalseAfterInsertLeavesColumnUnchanged)
{
    FormatSettings settings;

    SerializationPtr serialization = SerializationArray::create(
        std::make_shared<InsertingThenFailingSerialization>(/*rows_to_append=*/1, /*throw_parse_error=*/false));

    auto column = ColumnArray::create(ColumnUInt8::create());
    ReadBufferFromMemory istr("[1]");
    ASSERT_FALSE(serialization->tryDeserializeTextQuoted(*column, istr, settings));

    ASSERT_EQ(column->size(), 0u);
    ASSERT_EQ(column->getData().size(), 0u);
}

/// A parse error means "this value did not parse" -> tryDeserialize returns false.
/// Any other error (here MEMORY_LIMIT_EXCEEDED) is fatal and must propagate.
TEST(SerializationTryDeserialize, RethrowsNonParseErrors)
{
    FormatSettings settings;

    {
        SerializationPtr serialization = std::make_shared<ThrowingSerialization>(ErrorCodes::CANNOT_PARSE_NUMBER);
        auto column = ColumnUInt8::create();
        ReadBufferFromMemory istr("x");
        ASSERT_FALSE(serialization->tryDeserializeTextQuoted(*column, istr, settings));
    }

    {
        SerializationPtr serialization = std::make_shared<ThrowingSerialization>(ErrorCodes::MEMORY_LIMIT_EXCEEDED);
        auto column = ColumnUInt8::create();
        ReadBufferFromMemory istr("x");
        try
        {
            serialization->tryDeserializeTextQuoted(*column, istr, settings);
            FAIL() << "tryDeserialize swallowed MEMORY_LIMIT_EXCEEDED";
        }
        catch (const Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
        }
    }
}

/// Same contract through a composite serialization, whose restore-on-failure catch must
/// also rethrow fatal errors raised while deserializing a nested element.
TEST(SerializationTryDeserialize, RethrowsNonParseErrorsFromNested)
{
    FormatSettings settings;

    {
        SerializationPtr serialization = SerializationArray::create(std::make_shared<ThrowingSerialization>(ErrorCodes::CANNOT_PARSE_NUMBER));
        auto column = ColumnArray::create(ColumnUInt8::create());
        ReadBufferFromMemory istr("[1]");
        ASSERT_FALSE(serialization->tryDeserializeTextQuoted(*column, istr, settings));
    }

    {
        SerializationPtr serialization = SerializationArray::create(std::make_shared<ThrowingSerialization>(ErrorCodes::MEMORY_LIMIT_EXCEEDED));
        auto column = ColumnArray::create(ColumnUInt8::create());
        ReadBufferFromMemory istr("[1]");
        try
        {
            serialization->tryDeserializeTextQuoted(*column, istr, settings);
            FAIL() << "tryDeserialize swallowed MEMORY_LIMIT_EXCEEDED from a nested element";
        }
        catch (const Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
        }
    }
}
