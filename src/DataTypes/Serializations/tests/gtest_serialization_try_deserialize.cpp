#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <Columns/ColumnArray.h>
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
