#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/DataTypeDynamic.h>
#include <Columns/ColumnDynamic.h>

namespace DB
{

class SerializationDynamicElement;

class SerializationDynamic : public ISerialization
{
public:
    explicit SerializationDynamic(size_t max_dynamic_types_ = DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES) : max_dynamic_types(max_dynamic_types_)
    {
    }

    struct DynamicSerializationVersion
    {
        enum Value
        {
            /// V1 serialization:
            /// - DynamicStructure stream:
            ///     <max_dynamic_types parameter>
            ///     <actual number of dynamic types>
            ///     <list of dynamic types (list of variants in nested Variant column without SharedVariant)>
            ///     <statistics with number of values for each dynamic type> (only in MergeTree serialization)
            ///     <statistics with number of values for some types in SharedVariant> (only in MergeTree serialization)
            /// - DynamicData stream: contains the data of nested Variant column.
            V1 = 1,
            /// V2 serialization: the same as V1 but without max_dynamic_types parameter in DynamicStructure stream.
            V2 = 2,
            /// FLATTENED serialization:
            /// - DynamicStructure stream:
            ///     <list of all types stored in Dynamic column>
            /// - DynamicData stream:
            ///     <indexes of types stored in each row, have type UInt(8|16|32|64) depending on the total number of types>
            ///     <data for each type in order from the types list>
            ///
            /// This serialization is used in Native format only for easier support for Dynamic type in clients.
            FLATTENED = 3,
        };

        Value value;

        static void checkVersion(UInt64 version);

        explicit DynamicSerializationVersion(UInt64 version);
    };

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

    static DeserializeBinaryBulkStatePtr deserializeDynamicStructureStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        SubstreamsDeserializeStatesCache * cache);

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreamsAndCountTotalSizeOfVariants(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state,
        size_t & total_size_of_variants) const;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

private:
    friend SerializationDynamicElement;

    struct DeserializeBinaryBulkStateDynamicStructure : public ISerialization::DeserializeBinaryBulkState
    {
        DynamicSerializationVersion structure_version;
        DataTypePtr variant_type;
        size_t num_dynamic_types;
        ColumnDynamic::StatisticsPtr statistics;

        /// For flattened serialization only.
        DataTypes flattened_data_types;
        DataTypePtr flattened_indexes_type;

        explicit DeserializeBinaryBulkStateDynamicStructure(UInt64 structure_version_)
            : structure_version(structure_version_)
        {
        }

        ISerialization::DeserializeBinaryBulkStatePtr clone() const override
        {
            return std::make_shared<DeserializeBinaryBulkStateDynamicStructure>(*this);
        }
    };

    size_t max_dynamic_types;
};

}
