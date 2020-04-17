#pragma once

#include <Core/UUID.h>
#include <Common/UInt128.h>
#include <common/DayNum.h>
#include <memory>

#if !defined(ARCADIA_BUILD)
#    include "config_formats.h"
#endif

#if USE_PROTOBUF
#    include <IO/WriteBufferFromString.h>
#    include <boost/noncopyable.hpp>
#    include <Common/PODArray.h>
#    include "ProtobufColumnMatcher.h"


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
    bool writeNumber(Int8 value) { return writeValueIfPossible(&IConverter::writeInt8, value); }
    bool writeNumber(UInt8 value) { return writeValueIfPossible(&IConverter::writeUInt8, value); }
    bool writeNumber(Int16 value) { return writeValueIfPossible(&IConverter::writeInt16, value); }
    bool writeNumber(UInt16 value) { return writeValueIfPossible(&IConverter::writeUInt16, value); }
    bool writeNumber(Int32 value) { return writeValueIfPossible(&IConverter::writeInt32, value); }
    bool writeNumber(UInt32 value) { return writeValueIfPossible(&IConverter::writeUInt32, value); }
    bool writeNumber(Int64 value) { return writeValueIfPossible(&IConverter::writeInt64, value); }
    bool writeNumber(UInt64 value) { return writeValueIfPossible(&IConverter::writeUInt64, value); }
    bool writeNumber(UInt128 value) { return writeValueIfPossible(&IConverter::writeUInt128, value); }
    bool writeNumber(Float32 value) { return writeValueIfPossible(&IConverter::writeFloat32, value); }
    bool writeNumber(Float64 value) { return writeValueIfPossible(&IConverter::writeFloat64, value); }
    bool writeString(const StringRef & str) { return writeValueIfPossible(&IConverter::writeString, str); }
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & enum_values) { current_converter->prepareEnumMapping8(enum_values); }
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & enum_values) { current_converter->prepareEnumMapping16(enum_values); }
    bool writeEnum(Int8 value) { return writeValueIfPossible(&IConverter::writeEnum8, value); }
    bool writeEnum(Int16 value) { return writeValueIfPossible(&IConverter::writeEnum16, value); }
    bool writeUUID(const UUID & uuid) { return writeValueIfPossible(&IConverter::writeUUID, uuid); }
    bool writeDate(DayNum date) { return writeValueIfPossible(&IConverter::writeDate, date); }
    bool writeDateTime(time_t tm) { return writeValueIfPossible(&IConverter::writeDateTime, tm); }
    bool writeDateTime64(DateTime64 tm, UInt32 scale) { return writeValueIfPossible(&IConverter::writeDateTime64, tm, scale); }
    bool writeDecimal(Decimal32 decimal, UInt32 scale) { return writeValueIfPossible(&IConverter::writeDecimal32, decimal, scale); }
    bool writeDecimal(Decimal64 decimal, UInt32 scale) { return writeValueIfPossible(&IConverter::writeDecimal64, decimal, scale); }
    bool writeDecimal(const Decimal128 & decimal, UInt32 scale) { return writeValueIfPossible(&IConverter::writeDecimal128, decimal, scale); }
    bool writeAggregateFunction(const AggregateFunctionPtr & function, ConstAggregateDataPtr place) { return writeValueIfPossible(&IConverter::writeAggregateFunction, function, place); }

private:
    class SimpleWriter
    {
    public:
        SimpleWriter(WriteBuffer & out_);
        ~SimpleWriter();

        void startMessage();
        void endMessage();

        void startNestedMessage();
        void endNestedMessage(UInt32 field_number, bool is_group, bool skip_if_empty);

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
            Piece(size_t start_, size_t end_) : start(start_), end(end_) {}
            Piece() = default;
        };

        struct NestedInfo
        {
            size_t num_pieces_at_start;
            size_t num_bytes_skipped_at_start;
            NestedInfo(size_t num_pieces_at_start_, size_t num_bytes_skipped_at_start_)
                : num_pieces_at_start(num_pieces_at_start_), num_bytes_skipped_at_start(num_bytes_skipped_at_start_)
            {
            }
        };

        WriteBuffer & out;
        PODArray<UInt8> buffer;
        std::vector<Piece> pieces;
        size_t current_piece_start;
        size_t num_bytes_skipped;
        std::vector<NestedInfo> nested_infos;
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
        virtual void writeDateTime64(DateTime64, UInt32 scale) = 0;
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
            ProtobufColumnMatcher::Message<ColumnMatcherTraits> * repeatable_container_message;
        };
        struct MessageData
        {
            UInt32 parent_field_number;
            bool is_group;
            bool is_required;
            ProtobufColumnMatcher::Message<ColumnMatcherTraits> * repeatable_container_message;
            bool need_repeat;
        };
    };
    using Message = ProtobufColumnMatcher::Message<ColumnMatcherTraits>;
    using Field = ProtobufColumnMatcher::Field<ColumnMatcherTraits>;

    void setTraitsDataAfterMatchingColumns(Message * message);

    template <int field_type_id>
    std::unique_ptr<IConverter> createConverter(const google::protobuf::FieldDescriptor * field);

    template <typename... Params>
    using WriteValueFunctionPtr = void (IConverter::*)(Params...);

    template <typename... Params, typename... Args>
    bool writeValueIfPossible(WriteValueFunctionPtr<Params...> func, Args &&... args)
    {
        if (num_values && !current_field->data.is_repeatable)
        {
            setNestedMessageNeedsRepeat();
            return false;
        }
        (current_converter->*func)(std::forward<Args>(args)...);
        ++num_values;
        return true;
    }

    void setNestedMessageNeedsRepeat();
    void endWritingField();

    SimpleWriter simple_writer;
    std::unique_ptr<Message> root_message;

    Message * current_message;
    size_t current_field_index = 0;
    const Field * current_field = nullptr;
    IConverter * current_converter = nullptr;
    size_t num_values = 0;
};

}

#else
#    include <common/StringRef.h>


namespace DB
{
class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;
using ConstAggregateDataPtr = const char *;

class ProtobufWriter
{
public:
    bool writeNumber(Int8 /* value */) { return false; }
    bool writeNumber(UInt8 /* value */) { return false; }
    bool writeNumber(Int16 /* value */) { return false; }
    bool writeNumber(UInt16 /* value */) { return false; }
    bool writeNumber(Int32 /* value */) { return false; }
    bool writeNumber(UInt32 /* value */) { return false; }
    bool writeNumber(Int64 /* value */) { return false; }
    bool writeNumber(UInt64 /* value */) { return false; }
    bool writeNumber(UInt128 /* value */) { return false; }
    bool writeNumber(Float32 /* value */) { return false; }
    bool writeNumber(Float64 /* value */) { return false; }
    bool writeString(const StringRef & /* value */) { return false; }
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & /* name_value_pairs */) {}
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & /* name_value_pairs */) {}
    bool writeEnum(Int8 /* value */) { return false; }
    bool writeEnum(Int16 /* value */) { return false; }
    bool writeUUID(const UUID & /* value */) { return false; }
    bool writeDate(DayNum /* date */) { return false; }
    bool writeDateTime(time_t /* tm */) { return false; }
    bool writeDateTime64(DateTime64 /*tm*/, UInt32 /*scale*/) { return false; }
    bool writeDecimal(Decimal32 /* decimal */, UInt32 /* scale */) { return false; }
    bool writeDecimal(Decimal64 /* decimal */, UInt32 /* scale */) { return false; }
    bool writeDecimal(const Decimal128 & /* decimal */, UInt32 /* scale */) { return false; }
    bool writeAggregateFunction(const AggregateFunctionPtr & /* function */, ConstAggregateDataPtr /* place */) { return false; }
};

}
#endif
