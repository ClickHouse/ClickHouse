#include <Functions/UserDefined/UserDefinedWebAssemblyColumnarCodec.h>

#include <algorithm>
#include <cstring>
#include <span>
#include <functional>
#include <utility>
#include <vector>

#include <base/unaligned.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/Exception.h>
#include <limits>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace ColumnarWire
{


/// Read and validate a frame header from `src`, which must hold at least FRAME_HEADER_BYTES.
static void readFrameHeader(const uint8_t * src, uint32_t & num_rows, uint32_t & num_cols)
{
    uint32_t magic = 0;
    uint16_t version = 0;
    uint16_t reserved = 0;
    std::memcpy(&magic,    src,      4);
    std::memcpy(&version,  src + 4,  2);
    std::memcpy(&reserved, src + 6,  2);

    if (magic != FRAME_MAGIC)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: bad frame magic {:#010x}, expected {:#010x}", magic, FRAME_MAGIC);
    if (version != FRAME_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: unsupported frame version {}, this server writes and reads version {}",
            version, FRAME_VERSION);
    if (reserved != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: reserved frame header field is {}, expected 0; the frame uses a "
            "feature this version does not implement", reserved);

    std::memcpy(&num_rows, src + 8,  4);
    std::memcpy(&num_cols, src + 12, 4);
}


/// Round `cursor` up to `align` (a power of two), recording the skipped bytes as a gap.
static uint64_t alignWriteCursor(uint64_t cursor, uint64_t align, PaddingGaps * gaps)
{
    uint64_t aligned = (cursor + align - 1ull) & ~(align - 1ull);
    if (gaps && aligned != cursor)
        gaps->emplace_back(cursor, aligned - cursor);
    return aligned;
}

/// Element counts are uint32 on the wire; a nested count (flattened Array, LowCardinality
/// dictionary) is not bounded by the block's row count, so check it. UINT32_MAX itself is
/// excluded because `num_rows + 1` offsets would wrap.
static uint32_t checkFitsUint32(uint64_t value, const char * what)
{
    if (value >= std::numeric_limits<uint32_t>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: {} ({}) exceeds the maximum representable element count ({})",
            what, value, std::numeric_limits<uint32_t>::max() - 1);
    return static_cast<uint32_t>(value);
}

void validateColumnarWireSupportedType(const DataTypePtr & type, bool is_nested)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const DataTypePtr & nested_type = nullable_type->getNestedType();
        /// COL_VARIANT has no slot for a top-level null map (NULL is discriminator 0xFF).
        /// Nullable(Array) cannot be constructed; rejected defensively.
        if (!is_nested
            && (typeid_cast<const DataTypeArray *>(nested_type.get())
                || typeid_cast<const DataTypeVariant *>(nested_type.get())))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: Nullable(Array/Variant) is not supported: {}", type->getName());
        validateColumnarWireSupportedType(nested_type, is_nested);
        return;
    }
    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        if (is_nested)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: nested Variant inside Array/Tuple is not supported: {}", type->getName());
        for (const auto & alt : variant_type->getVariants())
            validateColumnarWireSupportedType(alt, is_nested);
        return;
    }
    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        validateColumnarWireSupportedType(map_type->getNestedType(), is_nested);
        return;
    }
    if (const auto * lowcard_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        /// Top-level: COL_LOWCARD. Nested: materialized to T.
        validateColumnarWireSupportedType(lowcard_type->getDictionaryType(), is_nested);
        return;
    }
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        validateColumnarWireSupportedType(array_type->getNestedType(), /* is_nested */ true);
        return;
    }
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        /// An empty tuple has no element to carry the row count, so it cannot be decoded.
        if (tuple_type->getElements().empty())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: empty Tuple type is not supported: {}", type->getName());
        for (const auto & elem : tuple_type->getElements())
            validateColumnarWireSupportedType(elem, /* is_nested */ true);
        return;
    }
    if (typeid_cast<const DataTypeString *>(type.get()))
        return;
    if (type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        return;
    /// Deny by default: Dynamic, JSON, AggregateFunction, ...
    throw Exception(ErrorCodes::INCORRECT_DATA,
        "COLUMNAR_V1: type is not supported: {}", type->getName());
}

namespace
{

/// Remap `src` indexes in place to positions in a dictionary holding only the referenced rows, in
/// order of first reference, and return the old position of each new row. The special slots
/// (NULL and the default) keep their positions even when unreferenced.
template <typename T>
PaddedPODArray<UInt64> remapLowCardinalityIndexes(PaddedPODArray<T> & indexes, size_t dictionary_size, size_t num_special)
{
    PaddedPODArray<UInt64> old_positions;
    PaddedPODArray<T> new_position(dictionary_size, 0);
    for (size_t i = 0; i < num_special; ++i)
    {
        old_positions.push_back(i);
        new_position[i] = static_cast<T>(i);
    }
    for (auto & index : indexes)
    {
        if (index >= num_special && new_position[index] == 0)
        {
            new_position[index] = static_cast<T>(old_positions.size());
            old_positions.push_back(index);
        }
        index = new_position[index];
    }
    return old_positions;
}

/// Unlike `ColumnLowCardinality::cutAndCompact`, which inserts the referenced values again and so
/// merges values that compare equal but differ in bytes (`-0.0` becomes `+0.0`), copy them byte for
/// byte. It also skips hashing them.
ColumnPtr compactLowCardinality(const ColumnLowCardinality & lc_col, const ColumnPtr & col, const DataTypeLowCardinality & lc_type)
{
    const IColumnUnique & dictionary = lc_col.getDictionary();
    const size_t num_special = dictionary.nestedColumnIsNullable() ? 2 : 1;
    MutableColumnPtr indexes = IColumn::mutate(lc_col.getIndexesPtr());
    PaddedPODArray<UInt64> old_positions;
    auto remap = [&]<typename T>(ColumnVector<T> & column)
    {
        old_positions = remapLowCardinalityIndexes(column.getData(), dictionary.size(), num_special);
    };
    if (auto * u8 = typeid_cast<ColumnUInt8 *>(indexes.get()))
        remap(*u8);
    else if (auto * u16 = typeid_cast<ColumnUInt16 *>(indexes.get()))
        remap(*u16);
    else if (auto * u32 = typeid_cast<ColumnUInt32 *>(indexes.get()))
        remap(*u32);
    else
        remap(assert_cast<ColumnUInt64 &>(*indexes));

    if (old_positions.size() == dictionary.size())
        return col;

    auto positions = ColumnUInt64::create();
    positions->getData().swap(old_positions);
    MutableColumnPtr keys = IColumn::mutate(dictionary.getNestedNotNullableColumn()->index(*positions, 0));
    MutableColumnPtr new_dictionary = DataTypeLowCardinality::createColumnUnique(*lc_type.getDictionaryType(), std::move(keys));
    return ColumnLowCardinality::create(std::move(new_dictionary), std::move(indexes), /*is_shared=*/false);
}

}

ColumnPtr compactLowCardinalityDictionaries(const ColumnPtr & col, const DataTypePtr & type)
{
    if (const auto * const_col = typeid_cast<const ColumnConst *>(col.get()))
    {
        ColumnPtr data = compactLowCardinalityDictionaries(const_col->getDataColumnPtr(), type);
        if (data == const_col->getDataColumnPtr())
            return col;
        return ColumnConst::create(std::move(data), const_col->size());
    }
    if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(col.get()))
        return compactLowCardinality(*lc_col, col, assert_cast<const DataTypeLowCardinality &>(*type));
    if (const auto * var_col = typeid_cast<const ColumnVariant *>(col.get()))
    {
        const auto & variant_types = assert_cast<const DataTypeVariant &>(*type).getVariants();
        Columns variants;
        variants.reserve(var_col->getNumVariants());
        bool changed = false;
        for (size_t local = 0; local < var_col->getNumVariants(); ++local)
        {
            const ColumnPtr & variant = var_col->getVariantPtrByLocalDiscriminator(local);
            const auto & variant_type = variant_types[var_col->globalDiscriminatorByLocal(static_cast<ColumnVariant::Discriminator>(local))];
            variants.push_back(compactLowCardinalityDictionaries(variant, variant_type));
            changed |= variants.back() != variant;
        }
        if (!changed)
            return col;
        return ColumnVariant::create(
            var_col->getLocalDiscriminatorsPtr(), var_col->getOffsetsPtr(), variants,
            var_col->getLocalToGlobalDiscriminatorsMapping());
    }
    return col;
}

namespace
{

/// Index column of the narrowest width that addresses `dictionary_size` entries, filled by
/// `fill(PaddedPODArray<T> &)`.
template <typename Fill>
MutableColumnPtr createLowCardinalityIndexes(size_t dictionary_size, Fill && fill)
{
    auto create = [&]<typename T>(T)
    {
        auto indexes = ColumnVector<T>::create();
        fill(indexes->getData());
        return MutableColumnPtr(std::move(indexes));
    };
    if (dictionary_size <= std::numeric_limits<UInt8>::max() + 1ULL)
        return create(UInt8{});
    if (dictionary_size <= std::numeric_limits<UInt16>::max() + 1ULL)
        return create(UInt16{});
    if (dictionary_size <= std::numeric_limits<UInt32>::max() + 1ULL)
        return create(UInt32{});
    return create(UInt64{});
}

/// Call `f` with the index array of `lc_col`, whatever its width.
template <typename F>
void withLowCardinalityIndexes(const ColumnLowCardinality & lc_col, F && f)
{
    const IColumn & indexes = lc_col.getIndexes();
    if (const auto * u8 = typeid_cast<const ColumnUInt8 *>(&indexes))
        f(u8->getData());
    else if (const auto * u16 = typeid_cast<const ColumnUInt16 *>(&indexes))
        f(u16->getData());
    else if (const auto * u32 = typeid_cast<const ColumnUInt32 *>(&indexes))
        f(u32->getData());
    else
        f(assert_cast<const ColumnUInt64 &>(indexes).getData());
}

/// Every row of `values` becomes its own dictionary entry, copied byte for byte. Unlike
/// `ColumnLowCardinality::insertRangeFromFullColumn`, this does not merge a value that compares
/// equal to the default, such as `-0.0`, into the default slot.
MutableColumnPtr lowCardinalityFromFullColumn(const IColumn & values, const DataTypeLowCardinality & lc_type)
{
    const IDataType & dictionary_type = *lc_type.getDictionaryType();
    const size_t num_special = dictionary_type.isNullable() ? 2 : 1;
    const IColumn * nested = &values;
    const NullMap * null_map = nullptr;
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&values))
    {
        nested = &nullable->getNestedColumn();
        null_map = &nullable->getNullMapData();
    }

    MutableColumnPtr keys = nested->cloneEmpty();
    keys->insertManyDefaults(num_special);
    keys->insertRangeFrom(*nested, 0, nested->size());

    MutableColumnPtr indexes = createLowCardinalityIndexes(keys->size(), [&]<typename T>(PaddedPODArray<T> & data)
    {
        data.resize(values.size());
        for (size_t i = 0; i < values.size(); ++i)
            data[i] = (null_map && (*null_map)[i]) ? 0 : static_cast<T>(i + num_special);
    });
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(dictionary_type, std::move(keys));
    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), /*is_shared=*/false);
}

/// The dictionary is the special slots of the first part followed by the other entries of every
/// part in turn, so no entry is reinserted by value.
MutableColumnPtr concatLowCardinality(const Columns & parts, const DataTypeLowCardinality & lc_type)
{
    const auto & first = assert_cast<const ColumnLowCardinality &>(*parts.front());
    const size_t num_special = first.getDictionary().nestedColumnIsNullable() ? 2 : 1;
    const IColumn & first_keys = *first.getDictionary().getNestedNotNullableColumn();
    MutableColumnPtr keys = first_keys.cloneEmpty();
    keys->insertRangeFrom(first_keys, 0, num_special);

    std::vector<size_t> bases;
    bases.reserve(parts.size());
    size_t rows = 0;
    for (const auto & part : parts)
    {
        const IColumn & part_keys = *assert_cast<const ColumnLowCardinality &>(*part).getDictionary().getNestedNotNullableColumn();
        bases.push_back(keys->size());
        keys->insertRangeFrom(part_keys, num_special, part_keys.size() - num_special);
        rows += part->size();
    }

    MutableColumnPtr indexes = createLowCardinalityIndexes(keys->size(), [&]<typename T>(PaddedPODArray<T> & data)
    {
        data.reserve(rows);
        for (size_t p = 0; p < parts.size(); ++p)
        {
            const UInt64 shift = bases[p] - num_special;
            withLowCardinalityIndexes(assert_cast<const ColumnLowCardinality &>(*parts[p]), [&](const auto & src)
            {
                for (const auto index : src)
                    data.push_back(static_cast<T>(index < num_special ? index : index + shift));
            });
        }
    });
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*lc_type.getDictionaryType(), std::move(keys));
    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), /*is_shared=*/false);
}

bool containsLowCardinality(const DataTypePtr & type)
{
    if (typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return true;
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return containsLowCardinality(array_type->getNestedType());
    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        return containsLowCardinality(map_type->getNestedType());
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        return std::ranges::any_of(tuple_type->getElements(), containsLowCardinality);
    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
        return std::ranges::any_of(variant_type->getVariants(), containsLowCardinality);
    return false;
}

}

ColumnPtr concatColumnarResults(const Columns & parts, const DataTypePtr & type)
{
    if (parts.empty())
        return type->createColumn();
    if (parts.size() == 1)
        return parts.front();

    if (!containsLowCardinality(type))
    {
        MutableColumnPtr result = parts.front()->cloneEmpty();
        for (const auto & part : parts)
            result->insertRangeFrom(*part, 0, part->size());
        return result;
    }

    if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return concatLowCardinality(parts, *lc_type);

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        Columns nested;
        auto offsets = ColumnArray::ColumnOffsets::create();
        UInt64 shift = 0;
        for (const auto & part : parts)
        {
            const auto & array = assert_cast<const ColumnArray &>(*part);
            nested.push_back(array.getDataPtr());
            for (const auto offset : array.getOffsets())
                offsets->getData().push_back(offset + shift);
            shift += array.getData().size();
        }
        return ColumnArray::create(concatColumnarResults(nested, array_type->getNestedType()), std::move(offsets));
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        Columns nested;
        for (const auto & part : parts)
            nested.push_back(assert_cast<const ColumnMap &>(*part).getNestedColumnPtr());
        return ColumnMap::create(concatColumnarResults(nested, map_type->getNestedType()));
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & element_types = tuple_type->getElements();
        Columns elements;
        for (size_t i = 0; i < element_types.size(); ++i)
        {
            Columns element_parts;
            for (const auto & part : parts)
                element_parts.push_back(assert_cast<const ColumnTuple &>(*part).getColumnPtr(i));
            elements.push_back(concatColumnarResults(element_parts, element_types[i]));
        }
        return ColumnTuple::create(elements);
    }

    /// Parts may order their alternatives differently, so the result uses global discriminators.
    const auto & variant_types = assert_cast<const DataTypeVariant &>(*type).getVariants();
    auto discriminators = ColumnVariant::ColumnDiscriminators::create();
    auto offsets = ColumnVariant::ColumnOffsets::create();
    std::vector<UInt64> variant_sizes(variant_types.size(), 0);
    for (const auto & part : parts)
    {
        const auto & variant = assert_cast<const ColumnVariant &>(*part);
        const auto & part_discriminators = variant.getLocalDiscriminators();
        const auto & part_offsets = variant.getOffsets();
        for (size_t i = 0; i < part_discriminators.size(); ++i)
        {
            const auto local = part_discriminators[i];
            if (local == ColumnVariant::NULL_DISCRIMINATOR)
            {
                discriminators->getData().push_back(ColumnVariant::NULL_DISCRIMINATOR);
                offsets->getData().push_back(0);
                continue;
            }
            const auto global = variant.globalDiscriminatorByLocal(local);
            discriminators->getData().push_back(global);
            offsets->getData().push_back(part_offsets[i] + variant_sizes[global]);
        }
        for (size_t global = 0; global < variant_types.size(); ++global)
            variant_sizes[global] += variant.getVariantByGlobalDiscriminator(global).size();
    }
    Columns variants;
    for (size_t global = 0; global < variant_types.size(); ++global)
    {
        Columns variant_parts;
        for (const auto & part : parts)
            variant_parts.push_back(assert_cast<const ColumnVariant &>(*part).getVariantPtrByGlobalDiscriminator(global));
        variants.push_back(concatColumnarResults(variant_parts, variant_types[global]));
    }
    return ColumnVariant::create(std::move(discriminators), std::move(offsets), variants);
}

/// COL_COMPLEX data, recursive:
///   Array(T):    uint64 offsets[n + 1], then T's data for all elements
///   Tuple(T...): each element's data in turn
///   String:      uint64 offsets[n + 1], then chars
///   Nullable(T): u8 null_map[n], then T's data
///   Fixed:       n values
/// Sizes are uint64: one row may hold more than 4 GiB.
static uint64_t complexDataSize(const IColumn & col, uint32_t n)
{
    if (const auto * map_col = typeid_cast<const ColumnMap *>(&col))
        return complexDataSize(map_col->getNestedColumn(), n);
    if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(&col))
        return complexDataSize(*lc_col->convertToFullColumn(), n);
    if (const auto * arr = typeid_cast<const ColumnArray *>(&col))
    {
        uint32_t total = checkFitsUint32(arr->getData().size(), "flattened Array element count");
        return (static_cast<uint64_t>(n) + 1u) * 8u + complexDataSize(arr->getData(), total);
    }
    if (const auto * tup = typeid_cast<const ColumnTuple *>(&col))
    {
        uint64_t sz = 0;
        for (const auto & field : tup->getColumns())
            sz += complexDataSize(*field, n);
        return sz;
    }
    if (typeid_cast<const ColumnString *>(&col))
    {
        const auto & str = assert_cast<const ColumnString &>(col);
        uint64_t chars = str.getChars().size();
        return (static_cast<uint64_t>(n) + 1u) * 8u + chars;
    }
    if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
        return static_cast<uint64_t>(n) + complexDataSize(null_col->getNestedColumn(), n);
    if (typeid_cast<const ColumnVariant *>(&col))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: nested Variant inside Array/Tuple is not supported in COL_COMPLEX; "
            "use a flat variant column or a different format");
    return static_cast<uint64_t>(n) * col.sizeOfValueIfFixed();
}

void writeComplexData(const IColumn & col, uint32_t n, uint8_t * dst)
{
    if (const auto * map_col = typeid_cast<const ColumnMap *>(&col))
    {
        writeComplexData(map_col->getNestedColumn(), n, dst);
        return;
    }
    if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(&col))
    {
        writeComplexData(*lc_col->convertToFullColumn(), n, dst);
        return;
    }
    if (const auto * arr = typeid_cast<const ColumnArray *>(&col))
    {
        const auto & ch_offs = arr->getOffsets();
        unalignedStore<uint64_t>(dst, 0ull);
        for (uint32_t i = 0; i < n; ++i)
            unalignedStore<uint64_t>(dst + (static_cast<uint64_t>(i) + 1u) * 8u, static_cast<uint64_t>(ch_offs[i]));
        uint32_t total = checkFitsUint32(arr->getData().size(), "flattened Array element count");
        writeComplexData(arr->getData(), total, dst + (static_cast<uint64_t>(n) + 1u) * 8u);
        return;
    }
    if (const auto * tup = typeid_cast<const ColumnTuple *>(&col))
    {
        uint64_t pos = 0;
        for (const auto & field : tup->getColumns())
        {
            uint64_t field_sz = complexDataSize(*field, n);
            writeComplexData(*field, n, dst + pos);
            pos += field_sz;
        }
        return;
    }
    if (const auto * str = typeid_cast<const ColumnString *>(&col))
    {
        const auto & ch_offs = str->getOffsets();
        const auto & chars   = str->getChars();
        uint8_t  * chars_dst = dst + (static_cast<uint64_t>(n) + 1u) * 8u;
        unalignedStore<uint64_t>(dst, 0ull);
        uint64_t wire_pos = 0ull;
        uint64_t ch_pos   = 0ull;
        for (uint32_t i = 0; i < n; ++i)
        {
            uint64_t end = ch_offs[i];
            uint64_t len = end - ch_pos;
            std::memcpy(chars_dst + wire_pos, chars.data() + ch_pos, len);
            wire_pos += len;
            unalignedStore<uint64_t>(dst + (static_cast<uint64_t>(i) + 1u) * 8u, wire_pos);
            ch_pos = end;
        }
        return;
    }
    if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
    {
        const auto & nm = null_col->getNullMapData();
        std::memcpy(dst, nm.data(), n);
        writeComplexData(null_col->getNestedColumn(), n, dst + n);
        return;
    }
    if (typeid_cast<const ColumnVariant *>(&col))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: nested Variant inside Array/Tuple is not supported in COL_COMPLEX; "
            "use a flat variant column or a different format");
    std::memcpy(dst, col.getRawData().data(), n * col.sizeOfValueIfFixed());
}

uint64_t buildColDescriptor(
    const IColumn * col,
    bool is_const,
    bool is_nullable,
    uint32_t num_rows,
    uint64_t write_cursor,
    ColDescriptor & desc,
    PaddingGaps * gaps)
{
    /// The null map is handled via `is_nullable`.
    if (const auto * top_null_col = typeid_cast<const ColumnNullable *>(col))
        col = &top_null_col->getNestedColumn();

    if (const auto * map_col = typeid_cast<const ColumnMap *>(col))
        col = &map_col->getNestedColumn();

    /// COL_VARIANT:
    ///   null_offset    -> u8 discriminators[num_rows], 0xFF = NULL
    ///   offsets_offset -> uint32 row offsets within the sub-column
    ///   data_offset    -> uint32 K, K x { u8 discriminator, 3 pad, ColDescriptor }, sub-column data
    if (const auto * var_col = typeid_cast<const ColumnVariant *>(col))
    {
        desc.type = COL_VARIANT | (is_const ? COL_IS_CONST : 0u);

        desc.null_offset = write_cursor;
        write_cursor += num_rows;

        write_cursor = alignWriteCursor(write_cursor, 4ull, gaps);
        desc.offsets_offset = write_cursor;
        write_cursor += num_rows * sizeof(uint32_t);

        write_cursor = alignWriteCursor(write_cursor, 4ull, gaps);
        desc.data_offset = write_cursor;

        uint32_t k = 0;
        uint32_t num_variants = static_cast<uint32_t>(var_col->getNumVariants());
        for (uint32_t local = 0; local < num_variants; ++local)
            if (!var_col->getVariantByLocalDiscriminator(local).empty())
                ++k;

        write_cursor += 4u + k * (4u + COL_DESC_BYTES);

        for (uint32_t local = 0; local < num_variants; ++local)
        {
            const IColumn & sub = var_col->getVariantByLocalDiscriminator(local);
            if (sub.empty())
                continue;
            ColDescriptor inner_desc{};
            uint32_t sub_rows = checkFitsUint32(sub.size(), "Variant sub-column row count");
            write_cursor = buildColDescriptor(&sub, false, false, sub_rows, write_cursor, inner_desc, gaps);
        }

        desc.data_size = write_cursor - desc.data_offset;
        return write_cursor;
    }

    /// COL_LOWCARD (top level only):
    ///   offsets_offset -> indexes[num_rows], index_elem_width bytes each
    ///   data_offset    -> uint32 dict_row_count, u8 index_elem_width, 3 pad, ColDescriptor, dictionary data
    /// null_offset is unused.
    if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(col))
    {
        desc.type        = COL_LOWCARD | (is_const ? COL_IS_CONST : 0u);
        desc.null_offset = 0;

        const IColumn & dict_col = *lc_col->getDictionary().getNestedColumn();
        const IColumn & idx_col  = lc_col->getIndexes();
        uint32_t dict_rows    = checkFitsUint32(dict_col.size(), "LowCardinality dictionary row count");
        uint32_t idx_elem_sz  = static_cast<uint32_t>(idx_col.sizeOfValueIfFixed());

        desc.offsets_offset = write_cursor;
        write_cursor += static_cast<uint64_t>(num_rows) * idx_elem_sz;

        write_cursor = alignWriteCursor(write_cursor, 4ull, gaps);
        desc.data_offset = write_cursor;
        write_cursor += 4u + 4u + COL_DESC_BYTES;

        ColDescriptor dict_desc{};
        write_cursor = buildColDescriptor(&dict_col, false, false, dict_rows, write_cursor, dict_desc, gaps);

        desc.data_size = write_cursor - desc.data_offset;
        return write_cursor;
    }

    if (const auto * arr_col = typeid_cast<const ColumnArray *>(col))
    {
        desc.type           = COL_COMPLEX | (is_const ? COL_IS_CONST : 0u) | (is_nullable ? COL_IS_NULLABLE : 0u);
        desc.offsets_offset = 0;

        if (is_nullable)
        {
            desc.null_offset = write_cursor;
            write_cursor += num_rows;
        }
        else
        {
            desc.null_offset = 0;
        }

        write_cursor = alignWriteCursor(write_cursor, 8ull, gaps);
        desc.data_offset = write_cursor;

        const IColumn & nested = arr_col->getData();
        uint32_t total_elems = checkFitsUint32(nested.size(), "flattened Array element count");

        desc.data_size = (num_rows + 1u) * sizeof(uint64_t) + complexDataSize(nested, total_elems);
        write_cursor  += desc.data_size;
        return write_cursor;
    }

    if (const auto * tup_col = typeid_cast<const ColumnTuple *>(col))
    {
        desc.type           = COL_COMPLEX | (is_const ? COL_IS_CONST : 0u) | (is_nullable ? COL_IS_NULLABLE : 0u);
        desc.offsets_offset = 0;

        if (is_nullable)
        {
            desc.null_offset = write_cursor;
            write_cursor += num_rows;
            write_cursor = alignWriteCursor(write_cursor, 8ull, gaps);
        }
        else
        {
            desc.null_offset = 0;
        }

        desc.data_offset    = write_cursor;
        desc.data_size      = complexDataSize(*tup_col, num_rows);
        write_cursor       += desc.data_size;
        return write_cursor;
    }

    const ColumnString * str_col = typeid_cast<const ColumnString *>(col);
    const ColumnNullable * null_col = typeid_cast<const ColumnNullable *>(col);

    if (null_col)
        str_col = typeid_cast<const ColumnString *>(&null_col->getNestedColumn());

    if (str_col)
    {
        uint32_t base_type = COL_BYTES | (is_nullable ? COL_IS_NULLABLE : 0u);

        desc.type = base_type | (is_const ? COL_IS_CONST : 0u);

        if (is_nullable)
        {
            desc.null_offset = write_cursor;
            write_cursor += num_rows;
        }
        else
        {
            desc.null_offset = 0;
        }

        write_cursor = alignWriteCursor(write_cursor, 8ull, gaps);
        desc.offsets_offset = write_cursor;
        write_cursor += (num_rows + 1u) * sizeof(uint64_t);

        desc.data_offset = write_cursor;
        uint64_t total_chars = str_col->getChars().size();
        desc.data_size = total_chars;
        write_cursor += total_chars;
        return write_cursor;
    }

    /// ColumnNullable::sizeOfValueIfFixed() includes the null byte.
    const IColumn * inner_col = null_col ? &null_col->getNestedColumn() : col;
    uint32_t elem_size = static_cast<uint32_t>(inner_col->sizeOfValueIfFixed());
    uint32_t wire_elem_size = elem_size;
    uint32_t base_type = 0;
    if      (wire_elem_size == 1) base_type = COL_FIXED8  | (is_nullable ? COL_IS_NULLABLE : 0u);
    else if (wire_elem_size == 2) base_type = COL_FIXED16 | (is_nullable ? COL_IS_NULLABLE : 0u);
    else if (wire_elem_size == 4) base_type = COL_FIXED32 | (is_nullable ? COL_IS_NULLABLE : 0u);
    else if (wire_elem_size == 8) base_type = COL_FIXED64 | (is_nullable ? COL_IS_NULLABLE : 0u);
    else base_type = COL_FIXEDN | (is_nullable ? COL_IS_NULLABLE : 0u);

    desc.type = base_type | (is_const ? COL_IS_CONST : 0u);
    if (is_nullable)
    {
        desc.null_offset = write_cursor;
        write_cursor += num_rows;
    }
    else
    {
        desc.null_offset = 0;
    }
    /// Natural alignment so the guest can read values in place.
    write_cursor = alignWriteCursor(write_cursor, std::min<uint64_t>(wire_elem_size & (~wire_elem_size + 1u), 16), gaps);
    desc.offsets_offset = 0;
    desc.data_offset    = write_cursor;
    desc.data_size      = static_cast<uint64_t>(num_rows) * wire_elem_size;
    write_cursor       += desc.data_size;
    return write_cursor;
}

void writeColData(
    const IColumn * col,
    bool is_nullable,
    uint32_t num_rows,
    const ColDescriptor & desc,
    std::span<uint8_t> buf)
{
    if (const auto * top_null_col = typeid_cast<const ColumnNullable *>(col))
    {
        col = &top_null_col->getNestedColumn();
        if (is_nullable && desc.null_offset)
        {
            const auto & nm = top_null_col->getNullMapData();
            std::memcpy(buf.data() + desc.null_offset, nm.data(), num_rows);
        }
    }

    if (const auto * map_col = typeid_cast<const ColumnMap *>(col))
        col = &map_col->getNestedColumn();

    if (const auto * var_col = typeid_cast<const ColumnVariant *>(col))
    {
        uint8_t * disc_dst = buf.data() + desc.null_offset;
        for (uint32_t i = 0; i < num_rows; ++i)
            disc_dst[i] = var_col->globalDiscriminatorAt(i);

        /// The guest's allocation need not be aligned, so the frame offset says nothing about the host address.
        uint8_t * offs_dst = buf.data() + desc.offsets_offset;
        const auto & row_offs = var_col->getOffsets();
        for (uint32_t i = 0; i < num_rows; ++i)
            unalignedStore<uint32_t>(offs_dst + static_cast<uint64_t>(i) * 4u, static_cast<uint32_t>(row_offs[i]));

        uint8_t * block = buf.data() + desc.data_offset;

        uint32_t num_variants = static_cast<uint32_t>(var_col->getNumVariants());

        uint32_t k = 0;
        for (uint32_t local = 0; local < num_variants; ++local)
            if (!var_col->getVariantByLocalDiscriminator(local).empty())
                ++k;

        std::memcpy(block, &k, 4u);
        uint8_t * record_ptr = block + 4u;

        uint64_t sub_cursor = desc.data_offset + 4u + k * (4u + COL_DESC_BYTES);

        for (uint32_t local = 0; local < num_variants; ++local)
        {
            const IColumn & sub = var_col->getVariantByLocalDiscriminator(local);
            if (sub.empty())
                continue;

            uint8_t global_d = var_col->globalDiscriminatorByLocal(static_cast<ColumnVariant::Discriminator>(local));
            uint32_t sub_rows = checkFitsUint32(sub.size(), "Variant sub-column row count");

            ColDescriptor inner_desc{};
            sub_cursor = buildColDescriptor(&sub, false, false, sub_rows, sub_cursor, inner_desc);
            inner_desc.null_offset = sub_rows;  /// Carries the sub-column row count

            std::memcpy(record_ptr,     &global_d,   1u);
            std::memset(record_ptr + 1, 0,           3u);
            std::memcpy(record_ptr + 4, &inner_desc, COL_DESC_BYTES);
            record_ptr += 4u + COL_DESC_BYTES;

            writeColData(&sub, false, sub_rows, inner_desc, buf);
        }
        return;
    }

    if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(col))
    {
        const IColumn & dict_col = *lc_col->getDictionary().getNestedColumn();
        const IColumn & idx_col  = lc_col->getIndexes();
        uint32_t dict_rows   = checkFitsUint32(dict_col.size(), "LowCardinality dictionary row count");
        uint32_t idx_elem_sz = static_cast<uint32_t>(idx_col.sizeOfValueIfFixed());

        /// Same layout buildColDescriptor computed.
        ColDescriptor dict_desc{};
        buildColDescriptor(&dict_col, false, false, dict_rows, desc.data_offset + 4u + 4u + COL_DESC_BYTES, dict_desc);

        uint8_t * header = buf.data() + desc.data_offset;
        std::memcpy(header, &dict_rows, 4u);
        uint8_t idx_elem_sz_byte = static_cast<uint8_t>(idx_elem_sz);
        std::memcpy(header + 4u, &idx_elem_sz_byte, 1u);
        std::memset(header + 5u, 0, 3u);
        std::memcpy(header + 8u, &dict_desc, COL_DESC_BYTES);

        writeColData(&dict_col, false, dict_rows, dict_desc, buf);

        std::memcpy(buf.data() + desc.offsets_offset, idx_col.getRawData().data(),
                    static_cast<uint64_t>(num_rows) * idx_elem_sz);
        return;
    }

    if (const auto * arr_col = typeid_cast<const ColumnArray *>(col))
    {
        const auto & ch_offsets = arr_col->getOffsets();
        const IColumn & nested  = arr_col->getData();
        uint32_t total_elems = checkFitsUint32(nested.size(), "flattened Array element count");

        uint8_t * wire_outer = buf.data() + desc.data_offset;
        unalignedStore<uint64_t>(wire_outer, 0ull);
        /// Widen first: (i + 1) * 8 wraps in uint32 for 2^29 rows.
        for (uint32_t i = 0; i < num_rows; ++i)
            unalignedStore<uint64_t>(wire_outer + (static_cast<uint64_t>(i) + 1u) * 8u,
                                     static_cast<uint64_t>(ch_offsets[i]));

        writeComplexData(nested, total_elems, buf.data() + desc.data_offset + (num_rows + 1u) * sizeof(uint64_t));
        return;
    }

    if (const auto * tup_col = typeid_cast<const ColumnTuple *>(col))
    {
        writeComplexData(*tup_col, num_rows, buf.data() + desc.data_offset);
        return;
    }

    const ColumnString * str_col = typeid_cast<const ColumnString *>(col);
    if (str_col)
    {
        const auto & ch_offsets = str_col->getOffsets();
        const auto & chars = str_col->getChars();

        uint8_t * wire_offsets = buf.data() + desc.offsets_offset;
        uint8_t * data_dst = buf.data() + desc.data_offset;

        unalignedStore<uint64_t>(wire_offsets, 0ull);
        uint64_t wire_pos = 0ull;
        uint64_t ch_pos = 0ull;
        for (uint32_t i = 0; i < num_rows; ++i)
        {
            uint64_t str_end = ch_offsets[i];
            uint64_t str_len = str_end - ch_pos;
            std::memcpy(data_dst + wire_pos, chars.data() + ch_pos, str_len);
            wire_pos += str_len;
            unalignedStore<uint64_t>(wire_offsets + (static_cast<uint64_t>(i) + 1u) * 8u, wire_pos);
            ch_pos = str_end;
        }
        return;
    }

    const auto * raw      = col->getRawData().data();
    uint32_t     elem_sz  = static_cast<uint32_t>(col->sizeOfValueIfFixed());

    /// 64-bit size: num_rows * elem_sz may wrap in uint32.
    std::memcpy(buf.data() + desc.data_offset, raw, static_cast<uint64_t>(num_rows) * elem_sz);
}

/// Size of one value of a fixed-width type, or 0. getSizeOfValueInMemory throws LOGICAL_ERROR
/// for variable-width types, but a mismatch with the frame must be INCORRECT_DATA.
static size_t fixedValueSizeOrZero(const IDataType & type)
{
    return type.isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() ? type.getSizeOfValueInMemory() : 0;
}

/// Decode one column from its descriptor. All offsets in `desc` are absolute in `buf`.
/// The frame comes from a guest and is untrusted: every mismatch must be INCORRECT_DATA.
MutableColumnPtr readColumnFromDesc(
    std::span<const uint8_t> buf,
    const ColDescriptor & desc,
    uint32_t num_rows,
    const DataTypePtr & result_type)
{
    bool is_nullable_wire = (desc.type & COL_IS_NULLABLE) != 0;
    uint32_t raw_type     = desc.type & ~(COL_IS_CONST | COL_IS_NULLABLE);
    bool is_const         = (desc.type & COL_IS_CONST) != 0;
    uint32_t rows_to_dec  = is_const ? 1u : num_rows;

    /// COL_IS_NULLABLE on a non-Nullable declared type.
    const auto * nullable_result_type = typeid_cast<const DataTypeNullable *>(result_type.get());
    if (is_nullable_wire && !nullable_result_type)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: descriptor sets COL_IS_NULLABLE but declared type {} is not Nullable",
            result_type->getName());
    /// Nullable declared type without COL_IS_NULLABLE.
    if (!is_nullable_wire && nullable_result_type)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: declared type {} is Nullable but the descriptor does not set "
            "COL_IS_NULLABLE, so the frame carries no null map for it",
            result_type->getName());
    const DataTypePtr & base_type = is_nullable_wire
        ? nullable_result_type->getNestedType()
        : result_type;

    /// Bounds-check before forming any pointer from the descriptor.
    if (desc.data_offset > buf.size() || desc.data_size > buf.size() - desc.data_offset)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1: column data range out of bounds: offset={}, size={}, buf={}",
            desc.data_offset, desc.data_size, buf.size());

    const uint8_t * const data_end = buf.data() + desc.data_offset + desc.data_size;

    std::function<MutableColumnPtr(const uint8_t *&, const DataTypePtr &, uint32_t)> decode;
    decode = [&](const uint8_t *& p, const DataTypePtr & type, uint32_t n) -> MutableColumnPtr
    {
        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
            return ColumnMap::create(decode(p, map_type->getNestedType(), n));
        if (const auto * lowcard_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            auto full_col = decode(p, lowcard_type->getDictionaryType(), n);
            return lowCardinalityFromFullColumn(*full_col, *lowcard_type);
        }
        if (const auto * arr_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            /// Widen before + 1: n may be UINT32_MAX.
            uint64_t outer_bytes = (static_cast<uint64_t>(n) + 1u) * sizeof(uint64_t);
            /// Compare against remaining space; p + outer_bytes may overflow.
            if (outer_bytes > static_cast<uint64_t>(data_end - p))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX nested Array outer offsets out of bounds");
            const uint8_t * outer_offs = p;
            p += outer_bytes;
            uint32_t total_elems = checkFitsUint32(
                unalignedLoad<uint64_t>(outer_offs + static_cast<uint64_t>(n) * 8u),
                "flattened Array element count read from frame");
            /// Offsets must start at 0 and be non-decreasing.
            if (unalignedLoad<uint64_t>(outer_offs) != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX Array offsets must start at 0");
            auto nested_col = decode(p, arr_type->getNestedType(), total_elems);
            auto offsets_col = ColumnUInt64::create(n);
            uint64_t prev_off = 0;
            for (uint32_t i = 0; i < n; ++i)
            {
                uint64_t off = unalignedLoad<uint64_t>(outer_offs + (static_cast<uint64_t>(i) + 1u) * 8u);
                if (off < prev_off)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "COLUMNAR_V1: COL_COMPLEX Array offsets must be non-decreasing");
                offsets_col->getData()[i] = off;
                prev_off = off;
            }
            return ColumnArray::create(std::move(nested_col), std::move(offsets_col));
        }
        if (const auto * tup_type = typeid_cast<const DataTypeTuple *>(type.get()))
        {
            const auto & field_types = tup_type->getElements();
            Columns fields;
            fields.reserve(field_types.size());
            for (const auto & ft : field_types)
                fields.push_back(decode(p, ft, n));
            return ColumnTuple::create(std::move(fields))->assumeMutable();
        }
        if (typeid_cast<const DataTypeString *>(type.get()))
        {
            uint64_t off_bytes = (static_cast<uint64_t>(n) + 1u) * sizeof(uint64_t);
            if (off_bytes > static_cast<uint64_t>(data_end - p))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX String offsets out of bounds");
            const uint8_t * wire_offs = p;
            p += off_bytes;
            uint64_t total_chars = unalignedLoad<uint64_t>(wire_offs + static_cast<uint64_t>(n) * 8u);
            if (total_chars > static_cast<uint64_t>(data_end - p))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX String chars out of bounds");
            const uint8_t * chars_src = p;
            p += total_chars;
            /// Offsets must start at 0 and be non-decreasing.
            if (unalignedLoad<uint64_t>(wire_offs) != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX String offsets must start at 0");
            auto col_str = ColumnString::create();
            auto & chars   = col_str->getChars();
            auto & offsets = col_str->getOffsets();
            offsets.resize(n);
            uint64_t ch_pos = 0ull;
            uint64_t prev_wire_end = 0;
            for (uint32_t i = 0; i < n; ++i)
            {
                uint64_t wire_end   = unalignedLoad<uint64_t>(wire_offs + (static_cast<uint64_t>(i) + 1u) * 8u);
                uint64_t wire_start = unalignedLoad<uint64_t>(wire_offs + static_cast<uint64_t>(i) * 8u);
                /// Bound by total_chars here: a later decrease is detected only after this copy.
                if (wire_start != prev_wire_end || wire_end < wire_start || wire_end > total_chars)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "COLUMNAR_V1: COL_COMPLEX String offsets must be non-decreasing, contiguous and within {} chars",
                        total_chars);
                prev_wire_end = wire_end;
                uint64_t str_len    = wire_end - wire_start;
                chars.resize(ch_pos + str_len);
                std::memcpy(chars.data() + ch_pos, chars_src + wire_start, str_len);
                ch_pos += str_len;
                offsets[i] = ch_pos;
            }
            return col_str;
        }
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            /// u8 null_map[n], then the nested data.
            if (static_cast<uint64_t>(n) > static_cast<uint64_t>(data_end - p))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX nested Nullable null map out of bounds");
            auto null_map_col = ColumnUInt8::create(n);
            std::memcpy(null_map_col->getData().data(), p, n);
            p += n;
            auto inner = decode(p, nullable_type->getNestedType(), n);
            return ColumnNullable::create(std::move(inner), std::move(null_map_col));
        }
        if (typeid_cast<const DataTypeVariant *>(type.get()))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: nested Variant inside Array/Tuple is not supported in COL_COMPLEX; "
                "use a flat variant column or a different format");
        uint32_t elem_bytes = static_cast<uint32_t>(type->getSizeOfValueInMemory());
        if (static_cast<uint64_t>(n) * elem_bytes > static_cast<uint64_t>(data_end - p))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_COMPLEX fixed data out of bounds");
        /// 64-bit size: n * elem_bytes may wrap in uint32.
        const uint64_t fixed_bytes = static_cast<uint64_t>(n) * elem_bytes;
        auto col = type->createColumn();
        col->insertManyDefaults(n);
        std::memcpy(const_cast<char *>(col->getRawData().data()), p, fixed_bytes);
        p += fixed_bytes;
        return col;
    };

    auto maybe_nullable = [&](MutableColumnPtr inner) -> MutableColumnPtr
    {
        if (!is_nullable_wire)
            return inner;
        /// 0 is never a valid null map offset: the header comes first.
        if (desc.null_offset == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_IS_NULLABLE is set but null_offset is 0 (missing null map)");
        /// Check offset and length separately; their sum may wrap.
        if (desc.null_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) > buf.size() - desc.null_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: null map out of bounds: offset={}, rows={}, buf={}",
                desc.null_offset, rows_to_dec, buf.size());
        auto null_col = ColumnUInt8::create(rows_to_dec);
        std::memcpy(null_col->getData().data(), buf.data() + desc.null_offset, rows_to_dec);
        return ColumnNullable::create(std::move(inner), std::move(null_col));
    };

    MutableColumnPtr col;

    if (raw_type == COL_BYTES)
    {
        /// The writer emits COL_BYTES only for String.
        if (!typeid_cast<const ColumnString *>(base_type->createColumn().get()))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_BYTES descriptor does not match declared type {}", base_type->getName());

        /// Widen before + 1; check offset and length separately.
        const uint64_t offsets_bytes = (static_cast<uint64_t>(rows_to_dec) + 1u) * 8u;
        if (desc.offsets_offset > buf.size() || offsets_bytes > buf.size() - desc.offsets_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_BYTES offsets array out of bounds: offset={}, rows={}, buf={}",
                desc.offsets_offset, rows_to_dec, buf.size());
        if (desc.data_offset > buf.size() || desc.data_size > buf.size() - desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_BYTES data out of bounds: offset={}, size={}, buf={}",
                desc.data_offset, desc.data_size, buf.size());

        const uint8_t  * wire_offsets = buf.data() + desc.offsets_offset;
        const uint8_t  * data         = buf.data() + desc.data_offset;
        auto col_str = ColumnString::create();
        auto & chars   = col_str->getChars();
        auto & offsets = col_str->getOffsets();
        offsets.resize(rows_to_dec);
        uint64_t ch_pos = 0ull;
        /// Offsets must start at 0, be non-decreasing and end at data_size.
        uint64_t expected_start = 0ull;
        for (uint32_t i = 0; i < rows_to_dec; ++i)
        {
            uint64_t wire_end   = unalignedLoad<uint64_t>(wire_offsets + (static_cast<uint64_t>(i) + 1u) * 8u);
            uint64_t wire_start = unalignedLoad<uint64_t>(wire_offsets + static_cast<uint64_t>(i) * 8u);
            if (wire_start != expected_start)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_BYTES offsets must be contiguous starting at 0: row {} expected "
                    "start {}, got {}",
                    i, expected_start, wire_start);
            if (wire_end < wire_start || wire_end > desc.data_size)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_BYTES invalid string offsets at row {}: [{}, {}), data_size={}",
                    i, wire_start, wire_end, desc.data_size);
            uint64_t str_len    = wire_end - wire_start;
            chars.resize(ch_pos + str_len);
            std::memcpy(chars.data() + ch_pos, data + wire_start, str_len);
            ch_pos += str_len;
            offsets[i] = ch_pos;
            expected_start = wire_end;
        }
        if (expected_start != desc.data_size)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_BYTES offsets do not cover the full data block: consumed {}, data_size={}",
                expected_start, desc.data_size);
        col = maybe_nullable(std::move(col_str));
    }
    else if (raw_type == COL_FIXED8)
    {
        if (fixedValueSizeOrZero(*base_type) != 1)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED8 type width mismatch: declared type has {} bytes",
                fixedValueSizeOrZero(*base_type));
        /// data_size must match the row count exactly.
        if (desc.data_size != static_cast<uint64_t>(rows_to_dec))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED8 data_size {} does not match row count {}",
                desc.data_size, rows_to_dec);
        if (desc.data_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) > buf.size() - desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED8 data out of bounds: offset={}, rows={}, buf={}",
                desc.data_offset, rows_to_dec, buf.size());
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, rows_to_dec);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXED16)
    {
        if (fixedValueSizeOrZero(*base_type) != 2)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED16 type width mismatch: declared type has {} bytes",
                fixedValueSizeOrZero(*base_type));
        /// 64-bit size: rows * width may wrap in uint32.
        const uint64_t data_bytes = static_cast<uint64_t>(rows_to_dec) * 2u;
        if (desc.data_size != data_bytes)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED16 data_size {} does not match row count {}",
                desc.data_size, rows_to_dec);
        if (desc.data_offset > buf.size() || data_bytes > buf.size() - desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED16 data out of bounds: offset={}, rows={}, buf={}",
                desc.data_offset, rows_to_dec, buf.size());
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, data_bytes);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXED32)
    {
        if (fixedValueSizeOrZero(*base_type) != 4)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED32 type width mismatch: declared type has {} bytes",
                fixedValueSizeOrZero(*base_type));
        /// 64-bit size: rows * width may wrap in uint32.
        const uint64_t data_bytes = static_cast<uint64_t>(rows_to_dec) * 4u;
        if (desc.data_size != data_bytes)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED32 data_size {} does not match row count {}",
                desc.data_size, rows_to_dec);
        if (desc.data_offset > buf.size() || data_bytes > buf.size() - desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED32 data out of bounds: offset={}, rows={}, buf={}",
                desc.data_offset, rows_to_dec, buf.size());
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, data_bytes);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXED64)
    {
        if (fixedValueSizeOrZero(*base_type) != 8)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED64 type width mismatch: declared type has {} bytes",
                fixedValueSizeOrZero(*base_type));
        /// 64-bit size: rows * width may wrap in uint32.
        const uint64_t data_bytes = static_cast<uint64_t>(rows_to_dec) * 8u;
        if (desc.data_size != data_bytes)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED64 data_size {} does not match row count {}",
                desc.data_size, rows_to_dec);
        if (desc.data_offset > buf.size() || data_bytes > buf.size() - desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXED64 data out of bounds: offset={}, rows={}, buf={}",
                desc.data_offset, rows_to_dec, buf.size());
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(inner->getRawData().data()),
                    buf.data() + desc.data_offset, data_bytes);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_FIXEDN)
    {
        /// Check the declared type first: a variable-width type with data_size 0 would
        /// otherwise pass the size check and allocate rows that carry no payload.
        const size_t elem_size = fixedValueSizeOrZero(*base_type);
        if (elem_size == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXEDN descriptor does not match declared type {}", base_type->getName());
        /// 64-bit size: rows * width may wrap in uint32.
        if (desc.data_size != static_cast<uint64_t>(rows_to_dec) * elem_size)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_FIXEDN data_size {} does not match {} rows of {} bytes",
                desc.data_size, rows_to_dec, elem_size);
        auto inner = base_type->createColumn();
        inner->insertManyDefaults(rows_to_dec);
        if (desc.data_size != 0)
            std::memcpy(const_cast<char *>(inner->getRawData().data()),
                        buf.data() + desc.data_offset, desc.data_size);
        col = maybe_nullable(std::move(inner));
    }
    else if (raw_type == COL_COMPLEX)
    {
        /// Map is decoded as Array(Tuple(K, V)) and wrapped at the end.
        const auto * map_type_ptr = typeid_cast<const DataTypeMap *>(base_type.get());
        const DataTypePtr & complex_type = map_type_ptr ? map_type_ptr->getNestedType() : base_type;
        if (const auto * arr_type = typeid_cast<const DataTypeArray *>(complex_type.get()))
        {
            /// uint64 offsets[rows + 1], then the nested data. Widen before + 1.
            const uint64_t outer_offset_bytes = (static_cast<uint64_t>(rows_to_dec) + 1u) * sizeof(uint64_t);
            if (outer_offset_bytes > desc.data_size)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX outer offsets exceed data_size: need={}, data_size={}",
                    outer_offset_bytes, desc.data_size);
            const uint8_t * p = buf.data() + desc.data_offset;
            const uint8_t * outer_offs = p;
            p += outer_offset_bytes;
            uint32_t total_elems = checkFitsUint32(
                unalignedLoad<uint64_t>(outer_offs + static_cast<uint64_t>(rows_to_dec) * 8u),
                "flattened Array element count read from frame");
            /// Offsets must start at 0 and be non-decreasing.
            if (unalignedLoad<uint64_t>(outer_offs) != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_COMPLEX Array offsets must start at 0");
            uint64_t prev_off = 0;
            for (uint32_t i = 0; i < rows_to_dec; ++i)
            {
                uint64_t off = unalignedLoad<uint64_t>(outer_offs + (static_cast<uint64_t>(i) + 1u) * 8u);
                if (off < prev_off)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "COLUMNAR_V1: COL_COMPLEX Array offsets must be non-decreasing");
                prev_off = off;
            }
            auto nested_col = decode(p, arr_type->getNestedType(), total_elems);
            auto offsets_col = ColumnUInt64::create(rows_to_dec);
            for (uint32_t i = 0; i < rows_to_dec; ++i)
                offsets_col->getData()[i] = unalignedLoad<uint64_t>(outer_offs + (static_cast<uint64_t>(i) + 1u) * 8u);
            col = maybe_nullable(ColumnArray::create(std::move(nested_col), std::move(offsets_col)));
        }
        else
        {
            const uint8_t * data_ptr = buf.data() + desc.data_offset;
            col = maybe_nullable(decode(data_ptr, complex_type, rows_to_dec));
        }
        if (map_type_ptr)
            col = ColumnMap::create(std::move(col));
    }
    else if (raw_type == COL_VARIANT)
    {
        const auto * variant_type = typeid_cast<const DataTypeVariant *>(base_type.get());
        if (!variant_type)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT descriptor does not match declared type {}", base_type->getName());

        const auto & alt_types = variant_type->getVariants();
        if (alt_types.size() > ColumnVariant::MAX_NESTED_COLUMNS)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT declared type has too many alternatives: {}", alt_types.size());

        /// Both arrays are mandatory; 0 would point at the frame header.
        if (desc.null_offset == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT descriptor has no discriminators (null_offset is 0)");
        if (desc.offsets_offset == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT descriptor has no row offsets (offsets_offset is 0)");

        if (desc.null_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) > buf.size() - desc.null_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT discriminators out of bounds: offset={}, rows={}, buf={}",
                desc.null_offset, rows_to_dec, buf.size());
        const uint8_t * disc_src = buf.data() + desc.null_offset;

        if (desc.offsets_offset > buf.size() || static_cast<uint64_t>(rows_to_dec) * 4u > buf.size() - desc.offsets_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT row offsets out of bounds: offset={}, rows={}, buf={}",
                desc.offsets_offset, rows_to_dec, buf.size());
        const uint8_t * offs_src = buf.data() + desc.offsets_offset;

        /// Header: uint32 K, K x { u8 discriminator, 3 pad, ColDescriptor }.
        if (desc.data_size < 4u)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT header truncated: data_size={}", desc.data_size);
        const uint8_t * header = buf.data() + desc.data_offset;
        uint32_t k = unalignedLoad<uint32_t>(header);
        constexpr uint64_t record_bytes = 4u + COL_DESC_BYTES;
        if (static_cast<uint64_t>(k) * record_bytes > desc.data_size - 4u)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_VARIANT header declares {} sub-variants, exceeding data_size={}",
                k, desc.data_size);

        VectorWithMemoryTracking<std::pair<ColDescriptor, uint32_t>> sub_by_global(alt_types.size(), {ColDescriptor{}, 0u});
        VectorWithMemoryTracking<bool> sub_present(alt_types.size(), false);
        const uint8_t * record_ptr = header + 4u;
        /// Sub-columns split the parent rows; cap them before any sub-column is decoded.
        uint64_t total_sub_rows = 0;
        for (uint32_t r = 0; r < k; ++r)
        {
            uint8_t global_d = record_ptr[0];
            if (global_d >= alt_types.size())
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_VARIANT sub-variant global discriminator {} out of range [0, {})",
                    static_cast<uint32_t>(global_d), alt_types.size());
            if (sub_present[global_d])
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_VARIANT duplicate global discriminator {}", static_cast<uint32_t>(global_d));

            ColDescriptor inner_desc{};
            std::memcpy(&inner_desc, record_ptr + 4u, COL_DESC_BYTES);
            /// The writer stores the sub-column row count in null_offset. The other ranges must
            /// stay inside this column's payload, after the header.
            uint32_t sub_rows = checkFitsUint32(inner_desc.null_offset, "Variant sub-column row count read from frame");
            total_sub_rows += sub_rows;
            if (total_sub_rows > rows_to_dec)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_VARIANT sub-variants declare {} rows in total, more than the {} parent rows",
                    total_sub_rows, rows_to_dec);
            uint64_t sub_region_end = desc.data_offset + desc.data_size;
            uint64_t payload_start = desc.data_offset + 4u + static_cast<uint64_t>(k) * record_bytes;
            if (inner_desc.offsets_offset != 0
                && (inner_desc.offsets_offset < payload_start || inner_desc.offsets_offset > sub_region_end))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_VARIANT sub-variant offsets_offset {} outside variant data region [{}, {})",
                    inner_desc.offsets_offset, payload_start, sub_region_end);
            if (inner_desc.data_offset < payload_start
                || inner_desc.data_offset > sub_region_end
                || inner_desc.data_size > sub_region_end - inner_desc.data_offset)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_VARIANT sub-variant data range [{}, {}) outside variant data region [{}, {})",
                    inner_desc.data_offset, inner_desc.data_offset + inner_desc.data_size,
                    payload_start, sub_region_end);
            sub_by_global[global_d] = {inner_desc, sub_rows};
            sub_present[global_d] = true;
            record_ptr += record_bytes;
        }

        MutableColumns variant_cols;
        variant_cols.reserve(alt_types.size());
        for (size_t g = 0; g < alt_types.size(); ++g)
        {
            if (sub_present[g])
            {
                const auto & [inner_desc, sub_rows] = sub_by_global[g];
                variant_cols.push_back(readColumnFromDesc(
                    buf.subspan(0, desc.data_offset + desc.data_size), inner_desc, sub_rows, alt_types[g]));
            }
            else
            {
                variant_cols.push_back(alt_types[g]->createColumn());
            }
        }

        /// Local and global discriminators match: variant_cols is in declared order.
        auto discr_col = ColumnVariant::ColumnDiscriminators::create(rows_to_dec);
        auto offs_col  = ColumnVariant::ColumnOffsets::create(rows_to_dec);
        VectorWithMemoryTracking<uint32_t> next_offset(alt_types.size(), 0);
        for (uint32_t i = 0; i < rows_to_dec; ++i)
        {
            uint8_t d = disc_src[i];
            uint32_t row_off = unalignedLoad<uint32_t>(offs_src + static_cast<uint64_t>(i) * 4u);
            if (d != ColumnVariant::NULL_DISCRIMINATOR)
            {
                if (d >= alt_types.size() || row_off != next_offset[d] || row_off >= sub_by_global[d].second)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "COLUMNAR_V1: COL_VARIANT row {} has invalid discriminator/offset: discr={}, offset={}",
                        i, static_cast<uint32_t>(d), row_off);
                ++next_offset[d];
            }
            discr_col->getData()[i] = d;
            offs_col->getData()[i] = row_off;
        }

        /// Every sub-column row must be referenced.
        for (size_t g = 0; g < alt_types.size(); ++g)
        {
            if (sub_present[g] && next_offset[g] != sub_by_global[g].second)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_VARIANT alternative {} declares {} rows but only {} are referenced",
                    g, sub_by_global[g].second, next_offset[g]);
        }

        col = ColumnVariant::create(std::move(discr_col), std::move(offs_col), std::move(variant_cols));
    }
    else if (raw_type == COL_LOWCARD)
    {
        const auto * lowcard_type = typeid_cast<const DataTypeLowCardinality *>(base_type.get());
        if (!lowcard_type)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_LOWCARD descriptor does not match declared type {}", base_type->getName());

        /// Header: uint32 dict_row_count, u8 index_elem_width, 3 pad, ColDescriptor.
        constexpr uint64_t header_bytes = 4u + 4u + COL_DESC_BYTES;
        if (desc.data_size < header_bytes)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_LOWCARD header truncated: data_size={}", desc.data_size);
        const uint8_t * header = buf.data() + desc.data_offset;
        uint32_t dict_row_count = unalignedLoad<uint32_t>(header);
        uint8_t index_elem_width = header[4];
        if (index_elem_width != 1 && index_elem_width != 2 && index_elem_width != 4 && index_elem_width != 8)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_LOWCARD index element width {} is not one of 1/2/4/8",
                static_cast<uint32_t>(index_elem_width));

        ColDescriptor dict_desc{};
        std::memcpy(&dict_desc, header + 8u, COL_DESC_BYTES);

        /// The dictionary must stay inside this column's payload, after the header.
        uint64_t region_end = desc.data_offset + desc.data_size;
        /// The writer never sets COL_IS_CONST on the dictionary.
        if (dict_desc.type & COL_IS_CONST)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_LOWCARD dictionary descriptor must not set COL_IS_CONST");
        uint64_t payload_start = desc.data_offset + header_bytes;
        if (dict_desc.null_offset != 0
            && (dict_desc.null_offset < payload_start || dict_desc.null_offset > region_end))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_LOWCARD dictionary null_offset {} outside data region [{}, {})",
                dict_desc.null_offset, payload_start, region_end);
        if (dict_desc.offsets_offset != 0
            && (dict_desc.offsets_offset < payload_start || dict_desc.offsets_offset > region_end))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_LOWCARD dictionary offsets_offset {} outside data region [{}, {})",
                dict_desc.offsets_offset, payload_start, region_end);
        if (dict_desc.data_offset < payload_start
            || dict_desc.data_offset > region_end
            || dict_desc.data_size > region_end - dict_desc.data_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_LOWCARD dictionary data range [{}, {}) outside data region [{}, {})",
                dict_desc.data_offset, dict_desc.data_offset + dict_desc.data_size, payload_start, region_end);

        /// Decode within this column's payload only. The dictionary of LowCardinality(Nullable(T))
        /// is sent without a null map: NULL is ColumnUnique's slot 0.
        auto dict_col = readColumnFromDesc(
            buf.subspan(0, region_end), dict_desc, dict_row_count, removeNullable(lowcard_type->getDictionaryType()));

        /// The index array is mandatory; 0 would point at the frame header.
        if (desc.offsets_offset == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_LOWCARD descriptor has no index array (offsets_offset is 0)");
        if (desc.offsets_offset > buf.size()
            || static_cast<uint64_t>(rows_to_dec) * index_elem_width > buf.size() - desc.offsets_offset)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_LOWCARD index array out of bounds: offset={}, rows={}, width={}, buf={}",
                desc.offsets_offset, rows_to_dec, static_cast<uint32_t>(index_elem_width), buf.size());
        const uint8_t * idx_src = buf.data() + desc.offsets_offset;

        MutableColumnPtr idx_col;
        switch (index_elem_width)
        {
            case 1: idx_col = ColumnUInt8::create(); break;
            case 2: idx_col = ColumnUInt16::create(); break;
            case 4: idx_col = ColumnUInt32::create(); break;
            default: idx_col = ColumnUInt64::create(); break;
        }
        idx_col->insertManyDefaults(rows_to_dec);
        std::memcpy(const_cast<char *>(idx_col->getRawData().data()), idx_src,
                    static_cast<uint64_t>(rows_to_dec) * index_elem_width);

        /// Every index must be a valid dictionary row.
        for (uint32_t i = 0; i < rows_to_dec; ++i)
            if (idx_col->getUInt(i) >= dict_row_count)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "COLUMNAR_V1: COL_LOWCARD index {} at row {} exceeds dictionary size {}",
                    idx_col->getUInt(i), i, dict_row_count);

        /// ColumnUnique needs its reserved leading slots: 1, or 2 when nullable.
        const auto & dictionary_type = *lowcard_type->getDictionaryType();
        const uint32_t num_special_values = dictionary_type.isNullable() ? 2 : 1;
        if (dict_row_count < num_special_values)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1: COL_LOWCARD dictionary of type {} has {} rows, but its reserved leading "
                "slots require at least {}", dictionary_type.getName(), dict_row_count, num_special_values);
        auto unique_col = DataTypeLowCardinality::createColumnUnique(dictionary_type, std::move(dict_col));
        col = ColumnLowCardinality::create(std::move(unique_col), std::move(idx_col), /* is_shared */ false);
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "COLUMNAR_V1: unsupported output ColType {}", raw_type);
    }

    if (is_const)
    {
        /// A const column always carries one row; return 0 rows when num_rows is 0.
        if (num_rows == 0)
            return col->cloneEmpty();
        return ColumnConst::create(std::move(col), num_rows);
    }
    return col;
}

/// Decode the one-column frame a guest returned.
MutableColumnPtr readColumnarOutput(
    std::span<const uint8_t> buf,
    const DataTypePtr & result_type,
    size_t expected_rows)
{
    if (buf.size() < FRAME_HEADER_BYTES + COL_DESC_BYTES)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1 output buffer too small: {} bytes", buf.size());

    uint32_t num_rows = 0;
    uint32_t num_cols = 0;
    readFrameHeader(buf.data(), num_rows, num_cols);

    if (num_rows != expected_rows)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1 output row count mismatch: expected {}, got {}", expected_rows, num_rows);
    if (num_cols != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1 output must have exactly 1 column, got {}", num_cols);

    ColDescriptor desc{};
    std::memcpy(&desc, buf.data() + FRAME_HEADER_BYTES, sizeof(desc));

    /// Every offset must point past the header and descriptor; 0 means absent where allowed.
    constexpr uint64_t hdr_desc_size = FRAME_HEADER_BYTES + COL_DESC_BYTES; // num_cols == 1
    for (uint64_t off : {desc.null_offset, desc.offsets_offset})
        if (off != 0 && off < hdr_desc_size)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "COLUMNAR_V1 output descriptor offset {} points inside the header/descriptor table (< {})",
                off, hdr_desc_size);
    if (desc.data_offset < hdr_desc_size)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1 output descriptor data_offset {} points inside the header/descriptor table (< {})",
            desc.data_offset, hdr_desc_size);

    /// Decode within this column's declared payload only.
    if (desc.data_size > std::numeric_limits<uint64_t>::max() - desc.data_offset)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1 output descriptor data_offset + data_size overflows: offset={}, size={}",
            desc.data_offset, desc.data_size);
    const uint64_t region_end = desc.data_offset + desc.data_size;
    if (region_end > buf.size())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "COLUMNAR_V1 output descriptor data range [{}, {}) exceeds buffer size {}",
            desc.data_offset, region_end, buf.size());
    return readColumnFromDesc(buf.subspan(0, region_end), desc, num_rows, result_type);
}

}
}
