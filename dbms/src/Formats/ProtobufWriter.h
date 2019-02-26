#pragma once

#include <Core/UUID.h>
#include <Common/UInt128.h>
#include <common/DayNum.h>

#include <Common/config.h>
#if USE_PROTOBUF

#include <Formats/ProtobufColumnMatcher.h>
#include <IO/WriteBufferFromString.h>
#include <boost/noncopyable.hpp>
#include <Common/PODArray.h>


namespace google
{
namespace protobuf
{
    class Descriptor;
    class FieldDescriptor;
}
}

namespace DB
{
class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;
using ConstAggregateDataPtr = const char *;


/** Serializes a protobuf, tries to cast types if necessarily.
  */
class ProtobufWriter : private boost::noncopyable
{
public:
    ProtobufWriter(WriteBuffer & out, const google::protobuf::Descriptor * message_type, const std::vector<String> & column_names);
    ~ProtobufWriter();

    /// Should be called at the beginning of writing a message.
    void startMessage();

    /// Should be called at the end of writing a message.
    void endMessage();

    /// Prepares for writing values of a field.
    /// Returns true and sets 'column_index' to the corresponding column's index.
    /// Returns false if there are no more fields to write in the message type (call endMessage() in this case).
    bool writeField(size_t & column_index);

    /// Writes a value. This function should be called one or multiple times after writeField().
    /// Returns false if there are no more place for the values in the protobuf's field.
    /// This can happen if the protobuf's field is not declared as repeated in the protobuf schema.
    void writeNumber(Int8 value) { writeValue(&IConverter::writeInt8, value); }
    void writeNumber(UInt8 value) { writeValue(&IConverter::writeUInt8, value); }
    void writeNumber(Int16 value) { writeValue(&IConverter::writeInt16, value); }
    void writeNumber(UInt16 value) { writeValue(&IConverter::writeUInt16, value); }
    void writeNumber(Int32 value) { writeValue(&IConverter::writeInt32, value); }
    void writeNumber(UInt32 value) { writeValue(&IConverter::writeUInt32, value); }
    void writeNumber(Int64 value) { writeValue(&IConverter::writeInt64, value); }
    void writeNumber(UInt64 value) { writeValue(&IConverter::writeUInt64, value); }
    void writeNumber(UInt128 value) { writeValue(&IConverter::writeUInt128, value); }
    void writeNumber(Float32 value) { writeValue(&IConverter::writeFloat32, value); }
    void writeNumber(Float64 value) { writeValue(&IConverter::writeFloat64, value); }
    void writeString(const StringRef & str) { writeValue(&IConverter::writeString, str); }
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & enum_values) { current_converter->prepareEnumMapping8(enum_values); }
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & enum_values) { current_converter->prepareEnumMapping16(enum_values); }
    void writeEnum(Int8 value) { writeValue(&IConverter::writeEnum8, value); }
    void writeEnum(Int16 value) { writeValue(&IConverter::writeEnum16, value); }
    void writeUUID(const UUID & uuid) { writeValue(&IConverter::writeUUID, uuid); }
    void writeDate(DayNum date) { writeValue(&IConverter::writeDate, date); }
    void writeDateTime(time_t tm) { writeValue(&IConverter::writeDateTime, tm); }
    void writeDecimal(Decimal32 decimal, UInt32 scale) { writeValue(&IConverter::writeDecimal32, decimal, scale); }
    void writeDecimal(Decimal64 decimal, UInt32 scale) { writeValue(&IConverter::writeDecimal64, decimal, scale); }
    void writeDecimal(const Decimal128 & decimal, UInt32 scale) { writeValue(&IConverter::writeDecimal128, decimal, scale); }
    void writeAggregateFunction(const AggregateFunctionPtr & function, ConstAggregateDataPtr place) { writeValue(&IConverter::writeAggregateFunction, function, place); }

private:
    class SimpleWriter
    {
    public:
        SimpleWriter(WriteBuffer & out_);
        ~SimpleWriter();

        void startMessage();
        void endMessage();

        void writeInt(UInt32 field_number, Int64 value);
        void writeUInt(UInt32 field_number, UInt64 value);
        void writeSInt(UInt32 field_number, Int64 value);
        template <typename T>
        void writeFixed(UInt32 field_number, T value);
        void writeString(UInt32 field_number, const StringRef & str);

        void startRepeatedPack();
        void addIntToRepeatedPack(Int64 value);
        void addUIntToRepeatedPack(UInt64 value);
        void addSIntToRepeatedPack(Int64 value);
        template <typename T>
        void addFixedToRepeatedPack(T value);
        void endRepeatedPack(UInt32 field_number);

    private:
        struct Piece
        {
            size_t start;
            size_t end;
            Piece(size_t start, size_t end) : start(start), end(end) {}
            Piece() = default;
        };

        WriteBuffer & out;
        PODArray<UInt8> buffer;
        std::vector<Piece> pieces;
        size_t current_piece_start;
        size_t num_bytes_skipped;
    };

    class IConverter
    {
    public:
        virtual ~IConverter() = default;
        virtual void writeString(const StringRef &) = 0;
        virtual void writeInt8(Int8) = 0;
        virtual void writeUInt8(UInt8) = 0;
        virtual void writeInt16(Int16) = 0;
        virtual void writeUInt16(UInt16) = 0;
        virtual void writeInt32(Int32) = 0;
        virtual void writeUInt32(UInt32) = 0;
        virtual void writeInt64(Int64) = 0;
        virtual void writeUInt64(UInt64) = 0;
        virtual void writeUInt128(const UInt128 &) = 0;
        virtual void writeFloat32(Float32) = 0;
        virtual void writeFloat64(Float64) = 0;
        virtual void prepareEnumMapping8(const std::vector<std::pair<std::string, Int8>> &) = 0;
        virtual void prepareEnumMapping16(const std::vector<std::pair<std::string, Int16>> &) = 0;
        virtual void writeEnum8(Int8) = 0;
        virtual void writeEnum16(Int16) = 0;
        virtual void writeUUID(const UUID &) = 0;
        virtual void writeDate(DayNum) = 0;
        virtual void writeDateTime(time_t) = 0;
        virtual void writeDecimal32(Decimal32, UInt32) = 0;
        virtual void writeDecimal64(Decimal64, UInt32) = 0;
        virtual void writeDecimal128(const Decimal128 &, UInt32) = 0;
        virtual void writeAggregateFunction(const AggregateFunctionPtr &, ConstAggregateDataPtr) = 0;
    };

    class ConverterBaseImpl;
    template <bool skip_null_value>
    class ConverterToString;
    template <int field_type_id, typename ToType, bool skip_null_value, bool pack_repeated>
    class ConverterToNumber;
    template <bool skip_null_value, bool pack_repeated>
    class ConverterToBool;
    template <bool skip_null_value, bool pack_repeated>
    class ConverterToEnum;

    struct ColumnMatcherTraits
    {
        struct FieldData
        {
            std::unique_ptr<IConverter> converter;
            bool is_required;
            bool is_repeatable;
            bool should_pack_repeated;
        };
        struct MessageData {};
    };
    using Message = ProtobufColumnMatcher::Message<ColumnMatcherTraits>;
    using Field = ProtobufColumnMatcher::Field<ColumnMatcherTraits>;

    void setTraitsDataAfterMatchingColumns(Message * message);

    template <int field_type_id>
    std::unique_ptr<IConverter> createConverter(const google::protobuf::FieldDescriptor * field);

    template <typename... Params>
    using WriteValueFunctionPtr = void (IConverter::*)(Params...);

    template <typename... Params, typename... Args>
    void writeValue(WriteValueFunctionPtr<Params...> func, Args &&... args)
    {
        (current_converter->*func)(std::forward<Args>(args)...);
        ++num_values;
    }

    void endWritingField();

    SimpleWriter simple_writer;
    std::unique_ptr<Message> root_message;

    bool writing_message = false;
    size_t current_field_index = 0;
    const Field * current_field = nullptr;
    IConverter * current_converter = nullptr;
    size_t num_values = 0;
};

}

#else

namespace DB
{
class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;
using ConstAggregateDataPtr = const char *;

class ProtobufWriter
{
public:
    void writeNumber(Int8 value) {}
    void writeNumber(UInt8 value) {}
    void writeNumber(Int16 value) {}
    void writeNumber(UInt16 value) {}
    void writeNumber(Int32 value) {}
    void writeNumber(UInt32 value) {}
    void writeNumber(Int64 value) {}
    void writeNumber(UInt64 value) {}
    void writeNumber(UInt128 value) {}
    void writeNumber(Float32 value) {}
    void writeNumber(Float64 value) {}
    void writeString(const StringRef & value) {}
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & name_value_pairs) {}
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & name_value_pairs) {}
    void writeEnum(Int8 value) {}
    void writeEnum(Int16 value) {}
    void writeUUID(const UUID & value) {}
    void writeDate(DayNum date) {}
    void writeDateTime(time_t tm) {}
    void writeDecimal(Decimal32 decimal, UInt32 scale) {}
    void writeDecimal(Decimal64 decimal, UInt32 scale) {}
    void writeDecimal(const Decimal128 & decimal, UInt32 scale) {}
    void writeAggregateFunction(const AggregateFunctionPtr & function, ConstAggregateDataPtr place) {}
};

}
#endif
