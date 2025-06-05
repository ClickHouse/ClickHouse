#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVariant.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class SerializationVariant;
class SerializationVariantElement;

/// Serialization for Variant element null map when we read it as a subcolumn.
/// For example, variant.UInt64.null.
/// It requires separate serialization because there is no actual Nullable column
/// and we should construct null map from variant discriminators.
/// The implementation of deserializeBinaryBulk* methods is similar to SerializationVariantElement,
/// but differs in that there is no need to read the actual data of the variant, only discriminators.
class SerializationVariantElementNullMap final : public SimpleTextSerialization
{
public:
    SerializationVariantElementNullMap(const String & variant_element_name_, ColumnVariant::Discriminator variant_discriminator_)
        : variant_element_name(variant_element_name_), variant_discriminator(variant_discriminator_)
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

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
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

    struct VariantNullMapSubcolumnCreator : public ISubcolumnCreator
    {
        const ColumnPtr local_discriminators;
        const String variant_element_name;
        const ColumnVariant::Discriminator global_variant_discriminator;
        const ColumnVariant::Discriminator local_variant_discriminator;

        VariantNullMapSubcolumnCreator(
            const ColumnPtr & local_discriminators_,
            const String & variant_element_name_,
            ColumnVariant::Discriminator global_variant_discriminator_,
            ColumnVariant::Discriminator local_variant_discriminator_);

        DataTypePtr create(const DataTypePtr & prev) const override;
        ColumnPtr create(const ColumnPtr & prev) const override;
        SerializationPtr create(const SerializationPtr & prev) const override;
    };
private:
    [[noreturn]] static void throwNoSerialization()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for variant element null map subcolumn");
    }

    friend SerializationVariant;
    friend SerializationVariantElement;

    /// To be able to deserialize Variant element null map as a subcolumn
    /// we need variant element type name and global discriminator.
    String variant_element_name;
    ColumnVariant::Discriminator variant_discriminator;

};

}
