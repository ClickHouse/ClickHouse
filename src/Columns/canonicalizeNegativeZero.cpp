#include <Columns/canonicalizeNegativeZero.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/IDataType.h>
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
    auto is_floating_point_type = [](const IDataType & nested)
    {
        WhichDataType which(nested);
        /// `Dynamic` and `JSON` do not report their nested types, so they can contain anything.
        return which.isFloat() || which.isDynamic() || which.isObject();
    };

    if (is_floating_point_type(type))
        return true;

    /// `forEachChild` is recursive.
    bool res = false;
    type.forEachChild([&](const IDataType & child) { res = res || is_floating_point_type(child); });
    return res;
}

/// A value in the shared variant of a `Dynamic` column and in the shared data of a `JSON` column
/// is stored as the binary encoding of its type followed by the value in binary format
/// (see `SerializationDynamic::serializeBinary`), so it cannot be canonicalized in place -
/// it has to be decoded and encoded back.
/// Returns `nullopt` if the value contains no negative zeros.
std::optional<String> canonicalizeNegativeZeroInEncodedValue(std::string_view encoded_value)
{
    ReadBufferFromMemory read_buffer(encoded_value);
    DataTypePtr type = decodeDataType(read_buffer);

    if (!mayContainNegativeZero(*type))
        return {};

    SerializationPtr serialization = type->getDefaultSerialization();
    auto value_column = type->createColumn();
    serialization->deserializeBinary(*value_column, read_buffer, ColumnDynamic::getBinaryEncodedValueFormatSettings());

    ColumnPtr canonical_column = canonicalizeNegativeZero(*value_column);
    if (!canonical_column)
        return {};

    WriteBufferFromOwnString write_buffer;
    encodeDataType(type, write_buffer);
    serialization->serializeBinary(*canonical_column, 0, write_buffer, ColumnDynamic::getBinaryEncodedValueFormatSettings());
    return write_buffer.str();
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

size_t rawFloatValueWidth(const IColumn & column)
{
    const IColumn * nested = &column;

    /// A value of an array of fixed size values is represented as the sequence of the values.
    while (const auto * column_array = typeid_cast<const ColumnArray *>(nested))
        nested = &column_array->getData();

    /// A value of a `LowCardinality` column is represented as the value in its dictionary.
    if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(nested))
        nested = column_low_cardinality->getDictionary().getNestedColumn().get();

    if (typeid_cast<const ColumnFloat64 *>(nested))
        return sizeof(Float64);

    if (typeid_cast<const ColumnFloat32 *>(nested))
        return sizeof(Float32);

    if (typeid_cast<const ColumnBFloat16 *>(nested))
        return sizeof(BFloat16);

    return 0;
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
