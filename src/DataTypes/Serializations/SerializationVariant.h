#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>

namespace DB
{

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
        std::unordered_map<String, size_t> & variants_statistics) const;

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
    void addVariantElementToPath(SubstreamPath & path, size_t i) const;

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
