#include <Columns/canonicalizeNegativeZero.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/TypeTree.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <base/normalizeNegativeZero.h>
#include <base/unaligned.h>


namespace DB
{

namespace
{

template <typename T>
void normalizeNegativeZerosInRawValue(char * data, size_t size)
{
    for (size_t offset = 0; offset + sizeof(T) <= size; offset += sizeof(T))
    {
        const T value = unalignedLoad<T>(data + offset);
        if (isNegativeZero(value))
            unalignedStore<T>(data + offset, T{});
    }
}

template <typename T>
ColumnPtr canonicalizeNegativeZeroInVector(const IColumn & column)
{
    using Container = typename ColumnVector<T>::Container;
    const Container & data = assert_cast<const ColumnVector<T> &>(column).getData();
    const size_t size = data.size();

    /// This check is branchless and vectorizable, unlike the loop below, which is almost never executed.
    bool has_negative_zero = false;
    for (size_t i = 0; i < size; ++i)
        has_negative_zero |= isNegativeZero(data[i]);

    if (!has_negative_zero)
        return nullptr;

    auto res = ColumnVector<T>::create(size);
    Container & res_data = res->getData();
    for (size_t i = 0; i < size; ++i)
        res_data[i] = normalizeNegativeZero(data[i]);

    return res;
}

/// Whether a floating point value can be found somewhere inside a value of this type.
bool mayContainNegativeZero(const IDataType & type)
{
    /// `anyInTypeTree` checks `type` itself and every type nested in it.
    return anyInTypeTree(type, [](const IDataType & nested)
    {
        WhichDataType which(nested);
        /// `Dynamic` and `JSON` do not report their nested types, so they can contain anything.
        return which.isFloat() || which.isDynamic() || which.isObject();
    });
}

/// The same, for a `String` column of binary encoded values.
ColumnPtr canonicalizeNegativeZeroInEncodedValues(const IColumn & column)
{
    const auto & column_string = assert_cast<const ColumnString &>(column);
    const size_t size = column_string.size();

    auto res = ColumnString::create();
    res->reserve(size);
    bool canonicalized = false;

    for (size_t i = 0; i < size; ++i)
    {
        std::string_view value = column_string.getDataAt(i);
        if (std::optional<String> canonical_value = canonicalizeNegativeZeroInEncodedValue(value))
        {
            res->insertData(canonical_value->data(), canonical_value->size());
            canonicalized = true;
        }
        else
        {
            res->insertData(value.data(), value.size());
        }
    }

    if (!canonicalized)
        return nullptr;

    return res;
}

/// The shared data of a `JSON` column is an `Array(Tuple(String, String))` column of the paths
/// that are not stored as a separate typed or dynamic path, and their binary encoded values.
ColumnPtr canonicalizeNegativeZeroInSharedData(const IColumn & column)
{
    const auto & column_array = assert_cast<const ColumnArray &>(column);
    const auto & column_tuple = assert_cast<const ColumnTuple &>(column_array.getData());

    ColumnPtr canonical_values = canonicalizeNegativeZeroInEncodedValues(column_tuple.getColumn(1));
    if (!canonical_values)
        return nullptr;

    return ColumnArray::create(
        ColumnTuple::create(Columns{column_tuple.getColumnPtr(0), canonical_values}), column_array.getOffsetsPtr());
}

/// `encoded_values_discriminator` is the local discriminator of the variant that stores
/// binary encoded values instead of a typed column - the shared variant of a `Dynamic` column.
ColumnPtr canonicalizeNegativeZeroInVariant(const ColumnVariant & column, std::optional<size_t> encoded_values_discriminator)
{
    const size_t num_variants = column.getNumVariants();

    Columns variants;
    variants.reserve(num_variants);
    bool canonicalized = false;

    for (size_t i = 0; i < num_variants; ++i)
    {
        const ColumnPtr & variant = column.getVariantPtrByLocalDiscriminator(i);

        ColumnPtr canonical_variant = encoded_values_discriminator == i
            ? canonicalizeNegativeZeroInEncodedValues(*variant)
            : canonicalizeNegativeZero(*variant);

        if (canonical_variant)
        {
            variants.push_back(std::move(canonical_variant));
            canonicalized = true;
        }
        else
        {
            variants.push_back(variant);
        }
    }

    if (!canonicalized)
        return nullptr;

    return ColumnVariant::create(
        column.getLocalDiscriminatorsPtr(), column.getOffsetsPtr(), variants, column.getLocalToGlobalDiscriminatorsMapping());
}

}

std::optional<String> canonicalizeNegativeZeroInEncodedValue(std::string_view encoded_value)
{
    ReadBufferFromMemory read_buffer(encoded_value);
    DataTypePtr type = decodeDataType(read_buffer);

    if (!mayContainNegativeZero(*type))
        return {};

    /// A value can be encoded with a `LowCardinality` type, e.g. in the shared variant of a `Dynamic` column.
    /// `canonicalizeNegativeZero` does not look into `LowCardinality`, but the binary format of a value
    /// of `LowCardinality(T)` is the binary format of a value of `T`, so the value is decoded without it.
    /// The type is encoded back as it was.
    DataTypePtr value_type = recursiveRemoveLowCardinality(type);
    SerializationPtr serialization = value_type->getDefaultSerialization();
    auto value_column = value_type->createColumn();
    serialization->deserializeBinary(*value_column, read_buffer, ColumnDynamic::getBinaryEncodedValueFormatSettings());

    ColumnPtr canonical_column = canonicalizeNegativeZero(*value_column);
    if (!canonical_column)
        return {};

    WriteBufferFromOwnString write_buffer;
    encodeDataType(type, write_buffer);
    serialization->serializeBinary(*canonical_column, 0, write_buffer, ColumnDynamic::getBinaryEncodedValueFormatSettings());
    return write_buffer.str();
}

ColumnPtr canonicalizeNegativeZero(const IColumn & column)
{
    if (typeid_cast<const ColumnFloat64 *>(&column))
        return canonicalizeNegativeZeroInVector<Float64>(column);

    if (typeid_cast<const ColumnFloat32 *>(&column))
        return canonicalizeNegativeZeroInVector<Float32>(column);

    if (typeid_cast<const ColumnBFloat16 *>(&column))
        return canonicalizeNegativeZeroInVector<BFloat16>(column);

    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (ColumnPtr nested = canonicalizeNegativeZero(column_nullable->getNestedColumn()))
            return ColumnNullable::create(nested, column_nullable->getNullMapColumnPtr());
        return nullptr;
    }

    if (const auto * column_array = typeid_cast<const ColumnArray *>(&column))
    {
        if (ColumnPtr data = canonicalizeNegativeZero(column_array->getData()))
            return ColumnArray::create(data, column_array->getOffsetsPtr());
        return nullptr;
    }

    if (const auto * column_map = typeid_cast<const ColumnMap *>(&column))
    {
        if (ColumnPtr nested = canonicalizeNegativeZero(column_map->getNestedColumn()))
            return ColumnMap::create(nested);
        return nullptr;
    }

    if (const auto * column_variant = typeid_cast<const ColumnVariant *>(&column))
        return canonicalizeNegativeZeroInVariant(*column_variant, {});

    if (const auto * column_dynamic = typeid_cast<const ColumnDynamic *>(&column))
    {
        const ColumnVariant & variant_column = column_dynamic->getVariantColumn();
        const size_t shared_variant = variant_column.localDiscriminatorByGlobal(column_dynamic->getSharedVariantDiscriminator());

        if (ColumnPtr canonical_variant_column = canonicalizeNegativeZeroInVariant(variant_column, shared_variant))
            return ColumnDynamic::create(
                canonical_variant_column,
                column_dynamic->getVariantInfo(),
                column_dynamic->getMaxDynamicTypes(),
                column_dynamic->getGlobalMaxDynamicTypes(),
                column_dynamic->getStatistics());
        return nullptr;
    }

    if (const auto * column_object = typeid_cast<const ColumnObject *>(&column))
    {
        bool canonicalized = false;

        /// `ColumnObject::PathToColumnMap` is a private type, hence `auto`.
        auto canonicalize_paths = [&](const auto & paths)
        {
            UnorderedMapWithMemoryTracking<String, ColumnPtr> res;
            for (const auto & [path, path_column] : paths)
            {
                if (ColumnPtr canonical_path_column = canonicalizeNegativeZero(*path_column))
                {
                    res[path] = std::move(canonical_path_column);
                    canonicalized = true;
                }
                else
                {
                    res[path] = path_column;
                }
            }
            return res;
        };

        auto typed_paths = canonicalize_paths(column_object->getTypedPaths());
        auto dynamic_paths = canonicalize_paths(column_object->getDynamicPaths());

        ColumnPtr shared_data = canonicalizeNegativeZeroInSharedData(*column_object->getSharedDataPtr());
        if (shared_data)
            canonicalized = true;
        else
            shared_data = column_object->getSharedDataPtr();

        if (!canonicalized)
            return nullptr;

        return ColumnObject::create(
            typed_paths,
            dynamic_paths,
            shared_data,
            column_object->getMaxDynamicPaths(),
            column_object->getMaxDynamicPathsUpperBound(),
            column_object->getGlobalMaxDynamicPaths(),
            column_object->getMaxDynamicTypes(),
            column_object->getStatistics());
    }

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        auto elements = column_tuple->getColumns();
        bool canonicalized = false;

        for (auto & element : elements)
        {
            if (ColumnPtr canonical_element = canonicalizeNegativeZero(*element))
            {
                element = std::move(canonical_element);
                canonicalized = true;
            }
        }

        if (canonicalized)
            return ColumnTuple::create(elements);
        return nullptr;
    }

    return nullptr;
}

ColumnPtr canonicalizeNegativeZeroThroughLowCardinality(const IColumn & column)
{
    const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(&column);
    if (!column_low_cardinality)
        return canonicalizeNegativeZero(column);

    /// Look at the dictionary first: it is usually much smaller than the column.
    if (!canonicalizeNegativeZero(*column_low_cardinality->getDictionary().getNestedColumn()))
        return nullptr;

    /// The dictionary may keep entries that no row references anymore (`filter` and `cut` keep the dictionary
    /// and only filter the indexes), so the negative zero found in it may be absent from the rows.
    ColumnPtr canonical_full = canonicalizeNegativeZero(*column_low_cardinality->convertToFullColumn());
    if (!canonical_full)
        return nullptr;

    auto res = column_low_cardinality->cloneEmpty();
    assert_cast<ColumnLowCardinality &>(*res).insertRangeFromFullColumn(*canonical_full, 0, canonical_full->size());
    return res;
}

size_t rawFloatValueWidth(const IColumn & column)
{
    /// This is called for every row on the paths that key on the raw bytes of a value,
    /// so it is a single virtual call and a jump table in the common case.
    switch (column.getDataType())
    {
        case TypeIndex::Float64:
            return sizeof(Float64);
        case TypeIndex::Float32:
            return sizeof(Float32);
        case TypeIndex::BFloat16:
            return sizeof(BFloat16);
        /// A value of an array of fixed size values is represented as the sequence of the values.
        case TypeIndex::Array:
            return rawFloatValueWidth(assert_cast<const ColumnArray &>(column).getData());
        /// A non-NULL value of a `Nullable` column is represented as the value of its nested column.
        case TypeIndex::Nullable:
            return rawFloatValueWidth(assert_cast<const ColumnNullable &>(column).getNestedColumn());
        /// A value of a `LowCardinality` column is represented as the value in its dictionary.
        case TypeIndex::LowCardinality:
            return rawFloatValueWidth(*assert_cast<const ColumnLowCardinality &>(column).getDictionary().getNestedColumn());
        default:
            return 0;
    }
}

void canonicalizeNegativeZeroInRawValue(std::string_view value, size_t width, char * res)
{
    memcpy(res, value.data(), value.size());

    if (width == sizeof(Float64))
        normalizeNegativeZerosInRawValue<Float64>(res, value.size());
    else if (width == sizeof(Float32))
        normalizeNegativeZerosInRawValue<Float32>(res, value.size());
    else if (width == sizeof(BFloat16))
        normalizeNegativeZerosInRawValue<BFloat16>(res, value.size());
}

std::optional<String> serializeValueIfContainsNegativeZero(const IColumn & column, size_t n)
{
    /// Look at the single value of a constant without materializing the constant.
    const IColumn * source = &column;
    if (const auto * column_const = typeid_cast<const ColumnConst *>(&column))
    {
        source = &column_const->getDataColumn();
        n = 0;
    }

    /// `canonicalizeNegativeZero` does not look into `LowCardinality`, whose dictionary can hold a negative zero.
    ColumnPtr value_column = recursiveRemoveLowCardinality(source->cut(n, 1)->convertToFullIfWrapped());
    if (!canonicalizeNegativeZero(*value_column))
        return {};

    Arena arena;
    const char * begin = nullptr;
    return String(value_column->serializeValueIntoArena(0, arena, begin, nullptr));
}

void updateHashWithRawValue(const IColumn & column, size_t n, SipHash & hash)
{
    if (const auto * column_float64 = typeid_cast<const ColumnFloat64 *>(&column))
    {
        hash.update(column_float64->getData()[n]);
    }
    else if (const auto * column_float32 = typeid_cast<const ColumnFloat32 *>(&column))
    {
        hash.update(column_float32->getData()[n]);
    }
    else if (const auto * column_bfloat16 = typeid_cast<const ColumnBFloat16 *>(&column))
    {
        hash.update(column_bfloat16->getData()[n]);
    }
    else if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        const UInt8 is_null = column_nullable->getNullMapData()[n];
        hash.update(is_null);
        if (!is_null)
            updateHashWithRawValue(column_nullable->getNestedColumn(), n, hash);
    }
    else if (const auto * column_array = typeid_cast<const ColumnArray *>(&column))
    {
        /// The offsets of `ColumnArray` are padded on the left with zeros, so `offsets[-1]` is 0 for the first row.
        const auto & offsets = column_array->getOffsets();
        const size_t offset = offsets[n - 1];
        const size_t array_size = offsets[n] - offset;
        hash.update(array_size);
        for (size_t i = 0; i < array_size; ++i)
            updateHashWithRawValue(column_array->getData(), offset + i, hash);
    }
    else if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        for (const auto & element : column_tuple->getColumns())
            updateHashWithRawValue(*element, n, hash);
    }
    else if (const auto * column_map = typeid_cast<const ColumnMap *>(&column))
    {
        updateHashWithRawValue(column_map->getNestedColumn(), n, hash);
    }
    else if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(&column))
    {
        updateHashWithRawValue(*column_low_cardinality->getDictionary().getNestedColumn(), column_low_cardinality->getIndexes().getUInt(n), hash);
    }
    else if (const auto * column_const = typeid_cast<const ColumnConst *>(&column))
    {
        updateHashWithRawValue(column_const->getDataColumn(), 0, hash);
    }
    else if (const auto * column_variant = typeid_cast<const ColumnVariant *>(&column))
    {
        /// The same framing as in `ColumnVariant::updateHashWithValue`.
        const ColumnVariant::Discriminator global_discr = column_variant->globalDiscriminatorAt(n);
        hash.update(global_discr);
        if (global_discr != ColumnVariant::NULL_DISCRIMINATOR)
            updateHashWithRawValue(column_variant->getVariantByGlobalDiscriminator(global_discr), column_variant->offsetAt(n), hash);
    }
    else if (const auto * column_dynamic = typeid_cast<const ColumnDynamic *>(&column))
    {
        /// The same framing as in `ColumnDynamic::updateHashWithValue`.
        const auto & variant_column = column_dynamic->getVariantColumn();
        const ColumnVariant::Discriminator discr = variant_column.globalDiscriminatorAt(n);
        if (discr == ColumnVariant::NULL_DISCRIMINATOR)
        {
            hash.update(discr);
        }
        else if (discr == column_dynamic->getSharedVariantDiscriminator())
        {
            auto value = column_dynamic->getSharedVariant().getDataAt(variant_column.offsetAt(n));
            ReadBufferFromMemory buf(value);
            auto type = decodeDataType(buf);
            hash.update(type->getName());
            auto tmp_column = type->createColumn();
            type->getDefaultSerialization()->deserializeBinary(*tmp_column, buf, ColumnDynamic::getBinaryEncodedValueFormatSettings());
            updateHashWithRawValue(*tmp_column, 0, hash);
        }
        else
        {
            hash.update(column_dynamic->getVariantInfo().variant_names[discr]);
            updateHashWithRawValue(variant_column.getVariantByGlobalDiscriminator(discr), variant_column.offsetAt(n), hash);
        }
    }
    else if (const auto * column_object = typeid_cast<const ColumnObject *>(&column))
    {
        /// The same order of the paths and the same framing as in `ColumnObject::updateHashWithValue`.
        column_object->updateHashWithValue(n, hash, &updateHashWithRawValue);
    }
    else
    {
        column.updateHashWithValue(n, hash);
    }
}

void canonicalizeNegativeZeroInKeyColumns(ColumnRawPtrs & key_columns, Columns & holder)
{
    for (auto & key_column : key_columns)
    {
        if (ColumnPtr canonical = canonicalizeNegativeZero(*key_column))
        {
            holder.emplace_back(std::move(canonical));
            key_column = holder.back().get();
        }
    }
}

}
