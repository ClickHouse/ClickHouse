#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>


namespace DB
{

/// Serialization of combined literal+sub-object Object subcolumns (the `@` subcolumn).
/// For example, if we have type JSON and data {"a" : {"b" : {"c" : 42, "d" : "Hello"}}, "c" : [1, 2, 3]}
/// this class will be responsible for reading combined path 'a' and will return a Dynamic column
/// that contains the literal value at path 'a' (if present) or the sub-object at path 'a' (if not empty).
/// This class is never used for typed paths - the typed-path check is done in `getDynamicSubcolumnData`.
class SerializationObjectCombinedPath final : public SimpleTextSerialization
{
private:
    SerializationObjectCombinedPath(
        const SerializationPtr & literal_serialization_,
        const SerializationPtr & sub_object_serialization_,
        const DataTypePtr & dynamic_type_,
        const DataTypePtr & sub_object_type_);

public:
    static UInt128 getHash(
        const SerializationPtr & literal_serialization_,
        const SerializationPtr & sub_object_serialization_,
        const DataTypePtr & dynamic_type_,
        const DataTypePtr & sub_object_type_);

    static SerializationPtr create(
        const SerializationPtr & literal_serialization_,
        const SerializationPtr & sub_object_serialization_,
        const DataTypePtr & dynamic_type_,
        const DataTypePtr & sub_object_type_);

    size_t allocatedBytes() const override;
    bool supportsPooling() const override;

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }
    bool tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }

private:
    [[noreturn]] static void throwNoSerialization();

    SerializationPtr literal_serialization;
    SerializationPtr sub_object_serialization;
    DataTypePtr dynamic_type;
    DataTypePtr sub_object_type;
};

}
