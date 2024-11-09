#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationVariantElementNullMap.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}


/// Class for serializing/deserializing column with Variant type.
/// It supports both text and binary bulk serializations/deserializations.
///
/// During text serialization it checks discriminator of the current row and
/// uses corresponding text serialization of this variant.
///
/// During text deserialization it tries all variants deserializations
/// (using tryDeserializeText* methods of ISerialization) in predefined order
/// and inserts data in the first variant with succeeded deserialization.
///
/// During binary bulk serialization it transforms local discriminators
/// to global and serializes them into a separate stream VariantDiscriminators.
/// There are 2 modes of serialising discriminators:
/// Basic mode, when all discriminators are serialized as is row by row.
/// Compact mode, when we avoid writing the same discriminators in granules when there is
/// only one variant (or only NULLs) in the granule.
/// In compact mode we serialize granules in the following format:
/// <number of rows in granule><granule format><granule data>
/// There are 2 different formats of granule - plain and compact.
/// Plain format is used when there are different discriminators in this granule,
/// in this format all discriminators are serialized as is row by row.
/// Compact format is used when all discriminators are the same in this granule,
/// in this case only this single discriminator is serialized.
/// Each variant is serialized into a separate stream with path VariantElements/VariantElement
/// (VariantElements stream is needed for correct sub-columns creation). We store and serialize
/// variants in a sparse form (the size of a variant column equals to the number of its discriminator
/// in the discriminators column), so during deserialization the limit for each variant is
/// calculated according to discriminators column.
/// Offsets column is not serialized and stored only in memory.
///
/// During binary bulk deserialization we first deserialize discriminators from corresponding stream
/// and use them to calculate the limit for each variant. Each variant is deserialized from
/// corresponding stream using calculated limit. Offsets column is not deserialized and constructed
/// according to discriminators.
class SerializationVariant : public ISerialization
{
public:
    struct DiscriminatorsSerializationMode
    {
        enum Value
        {
            BASIC = 0,    /// Store the whole discriminators column.
            COMPACT = 1,  /// Don't write discriminators in granule if all of them are the same.
        };

        static void checkMode(UInt64 mode)
        {
            if (mode > Value::COMPACT)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version for SerializationVariant discriminators column.");
        }

        explicit DiscriminatorsSerializationMode(UInt64 mode) : value(static_cast<Value>(mode)) { checkMode(mode); }

        Value value;
    };

    using VariantSerializations = std::vector<SerializationPtr>;

    explicit SerializationVariant(
        const VariantSerializations & variants_,
        const std::vector<String> & variant_names_,
        const std::vector<size_t> & deserialize_text_order_,
        const String & variant_name_)
        : variants(variants_), variant_names(variant_names_), deserialize_text_order(deserialize_text_order_), variant_name(variant_name_)
    {
    }

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

    void serializeBinaryBulkWithMultipleStreamsAndUpdateVariantStatistics(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state,
        std::unordered_map<String, size_t> & variants_statistics,
        size_t & total_size_of_variants) const;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
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

    /// Determine the order in which we should try to deserialize variants.
    /// In some cases the text representation of a value can be deserialized
    /// into several types (for example, almost all text values can be deserialized
    /// into String type), so we uses some heuristics to determine the more optimal order.
    static std::vector<size_t> getVariantsDeserializeTextOrder(const DataTypes & variant_types);

private:
    friend SerializationVariantElement;
    friend SerializationVariantElementNullMap;

    void addVariantElementToPath(SubstreamPath & path, size_t i) const;

    enum CompactDiscriminatorsGranuleFormat
    {
        PLAIN = 0,   /// Granule has different discriminators and they are serialized as is row by row.
        COMPACT = 1, /// Granule has single discriminator for all rows and it is serialized as single value.
    };

    struct DeserializeBinaryBulkStateVariantDiscriminators : public ISerialization::DeserializeBinaryBulkState
    {
        explicit DeserializeBinaryBulkStateVariantDiscriminators(UInt64 mode_) : mode(mode_)
        {
        }

        DiscriminatorsSerializationMode mode;

        /// Deserialize state of currently read granule in compact mode.
        CompactDiscriminatorsGranuleFormat granule_format = CompactDiscriminatorsGranuleFormat::PLAIN;
        size_t remaining_rows_in_granule = 0;
        ColumnVariant::Discriminator compact_discr = 0;
    };

    static DeserializeBinaryBulkStatePtr deserializeDiscriminatorsStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        SubstreamsDeserializeStatesCache * cache);

    std::vector<size_t> deserializeCompactDiscriminators(
        ColumnPtr & discriminators_column,
        size_t limit,
        ReadBuffer * stream,
        bool continuous_reading,
        DeserializeBinaryBulkStateVariantDiscriminators & state) const;

    static void readDiscriminatorsGranuleStart(DeserializeBinaryBulkStateVariantDiscriminators & state, ReadBuffer * stream);

    bool tryDeserializeTextEscapedImpl(IColumn & column, const String & field, const FormatSettings & settings) const;
    bool tryDeserializeTextQuotedImpl(IColumn & column, const String & field, const FormatSettings & settings) const;
    bool tryDeserializeWholeTextImpl(IColumn & column, const String & field, const FormatSettings & settings) const;
    bool tryDeserializeTextCSVImpl(IColumn & column, const String & field, const FormatSettings & settings) const;
    bool tryDeserializeTextJSONImpl(IColumn & column, const String & field, const FormatSettings & settings) const;
    bool tryDeserializeTextRawImpl(IColumn & column, const String & field, const FormatSettings & settings) const;

    bool tryDeserializeImpl(
        IColumn & column,
        const String & field,
        std::function<bool(ReadBuffer &)> check_for_null,
        std::function<bool(IColumn & variant_columm, const SerializationPtr & nested, ReadBuffer &)> try_deserialize_nested) const;

    VariantSerializations variants;
    std::vector<String> variant_names;
    std::vector<size_t> deserialize_text_order;
    /// Name of Variant data type for better exception messages.
    String variant_name;
};

}
