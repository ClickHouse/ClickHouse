#include <Processors/Formats/Impl/ArrowBatchRowLimit.h>

#if USE_ARROW || USE_PARQUET

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/IColumnUnique.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/assert_cast.h>

#include <algorithm>
#include <vector>

namespace DB
{

namespace
{

/// The largest `n <= end - begin` for which the non-decreasing `offsets` grow by at most `limit` over
/// rows [begin, begin + n).
size_t maxRowsByOffsets(const IColumn::Offsets & offsets, size_t begin, size_t end, UInt64 limit)
{
    const UInt64 base = offsets[static_cast<ssize_t>(begin) - 1];
    const auto * first = offsets.data() + begin;
    const auto * it = std::upper_bound(first, offsets.data() + end, base + limit);
    return static_cast<size_t>(it - first);
}

/// A writer either materializes the column or dictionary-encodes it; measuring it as materialized bounds
/// both, since a batch's dictionary holds at most the distinct values of its rows. `canBeInsideLowCardinality`
/// allows only fixed-width types, `String` and `FixedString`, and of those only the two string types reach
/// a 32-bit-offset buffer.
size_t maxRowsForLowCardinality(
    const IColumn & column, const DataTypePtr & type, size_t begin, size_t end, bool fixed_string_as_fixed_byte_array)
{
    const size_t num_rows = end - begin;
    const DataTypePtr values_type = removeNullable(removeLowCardinality(type));
    const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(column);
    const IColumn & values_column = *low_cardinality.getDictionary().getNestedNotNullableColumn();

    if (isFixedString(values_type))
    {
        if (fixed_string_as_fixed_byte_array)
            return num_rows;
        const size_t n = assert_cast<const ColumnFixedString &>(values_column).getN();
        return n == 0 ? num_rows : std::min<size_t>(num_rows, MAX_ARROW_BUFFER_SIZE / n);
    }

    if (!isString(values_type))
        return num_rows;

    const auto & values = assert_cast<const ColumnString &>(values_column);
    const auto & value_offsets = values.getOffsets();

    UInt64 max_value_size = 0;
    for (size_t i = 0; i != values.size(); ++i)
        max_value_size = std::max(max_value_size, value_offsets[i] - value_offsets[static_cast<ssize_t>(i) - 1]);
    /// Scanning the indexes is only needed when the largest value repeated on every row could overflow.
    if (max_value_size == 0 || num_rows <= MAX_ARROW_BUFFER_SIZE / max_value_size)
        return num_rows;

    const auto & indexes = low_cardinality.getIndexes();
    UInt64 total = 0;
    for (size_t row = begin; row != end; ++row)
    {
        const auto index = static_cast<ssize_t>(indexes.getUInt(row));
        total += value_offsets[index] - value_offsets[index - 1];
        if (total > MAX_ARROW_BUFFER_SIZE)
            return row - begin;
    }
    return num_rows;
}

/// A `Variant` becomes an Arrow dense union, whose own buffers are bounded by the row count; only its
/// children, holding one alternative's rows each, can overflow.
size_t maxRowsForVariant(
    const IColumn & column, const DataTypePtr & type, size_t begin, size_t end, bool fixed_string_as_fixed_byte_array)
{
    const size_t num_rows = end - begin;
    const auto & variant_column = assert_cast<const ColumnVariant &>(column);
    const auto & variant_type = assert_cast<const DataTypeVariant &>(*type);
    const size_t num_variants = variant_column.getNumVariants();
    const auto & local_discriminators = variant_column.getLocalDiscriminators();
    const auto & variant_offsets = variant_column.getOffsets();

    /// Within a contiguous row range the offsets of each alternative are contiguous too (see
    /// `ColumnVariant::updateHashWithValueRange`), so [begin, end) maps to the range
    /// [nested_begin, nested_begin + nested_rows) of every alternative.
    std::vector<size_t> nested_begin(num_variants, 0);
    std::vector<size_t> nested_rows(num_variants, 0);
    for (size_t row = begin; row != end; ++row)
    {
        const auto local_discr = local_discriminators[row];
        if (local_discr == ColumnVariant::NULL_DISCRIMINATOR)
            continue;
        if (nested_rows[local_discr] == 0)
            nested_begin[local_discr] = variant_offsets[row];
        ++nested_rows[local_discr];
    }

    std::vector<size_t> nested_rows_that_fit(num_variants);
    bool all_fit = true;
    for (size_t local_discr = 0; local_discr != num_variants; ++local_discr)
    {
        const auto global_discr
            = variant_column.globalDiscriminatorByLocal(static_cast<ColumnVariant::Discriminator>(local_discr));
        nested_rows_that_fit[local_discr] = maxRowsFittingOneArrowBatch(
            variant_column.getVariantByLocalDiscriminator(local_discr),
            variant_type.getVariant(global_discr),
            nested_begin[local_discr],
            nested_begin[local_discr] + nested_rows[local_discr],
            fixed_string_as_fixed_byte_array);
        all_fit &= nested_rows_that_fit[local_discr] == nested_rows[local_discr];
    }
    if (all_fit)
        return num_rows;

    /// Some alternative cannot take all of its rows: cut at the row where it runs out.
    std::vector<size_t> nested_rows_taken(num_variants, 0);
    for (size_t row = begin; row != end; ++row)
    {
        const auto local_discr = local_discriminators[row];
        if (local_discr == ColumnVariant::NULL_DISCRIMINATOR)
            continue;
        if (nested_rows_taken[local_discr] == nested_rows_that_fit[local_discr])
            return row - begin;
        ++nested_rows_taken[local_discr];
    }
    return num_rows;
}

}

size_t maxRowsFittingOneArrowBatch(
    const IColumn & column, const DataTypePtr & type, size_t begin, size_t end, bool fixed_string_as_fixed_byte_array)
{
    const size_t num_rows = end - begin;
    if (num_rows == 0)
        return 0;

    if (type->lowCardinality())
        return maxRowsForLowCardinality(column, type, begin, end, fixed_string_as_fixed_byte_array);

    if (isVariant(type))
        return maxRowsForVariant(column, type, begin, end, fixed_string_as_fixed_byte_array);

    /// A null row is written as a zero-length slot, so measuring the nested column only overestimates.
    if (type->isNullable())
    {
        return maxRowsFittingOneArrowBatch(
            assert_cast<const ColumnNullable &>(column).getNestedColumn(),
            removeNullable(type),
            begin,
            end,
            fixed_string_as_fixed_byte_array);
    }

    switch (WhichDataType(type).idx)
    {
        case TypeIndex::String:
        {
            return maxRowsByOffsets(assert_cast<const ColumnString &>(column).getOffsets(), begin, end, MAX_ARROW_BUFFER_SIZE);
        }
        case TypeIndex::FixedString:
        {
            if (fixed_string_as_fixed_byte_array)
                return num_rows;
            const size_t n = assert_cast<const ColumnFixedString &>(column).getN();
            return n == 0 ? num_rows : std::min<size_t>(num_rows, MAX_ARROW_BUFFER_SIZE / n);
        }
        case TypeIndex::Array:
        {
            const auto & array = assert_cast<const ColumnArray &>(column);
            const auto & offsets = array.getOffsets();
            const size_t nested_begin = offsets[static_cast<ssize_t>(begin) - 1];
            const size_t nested_rows = maxRowsFittingOneArrowBatch(
                array.getData(),
                assert_cast<const DataTypeArray &>(*type).getNestedType(),
                nested_begin,
                offsets[end - 1],
                fixed_string_as_fixed_byte_array);
            return maxRowsByOffsets(offsets, begin, end, std::min<UInt64>(nested_rows, MAX_ARROW_BUFFER_SIZE));
        }
        case TypeIndex::Map:
        {
            /// Both writers encode a Map exactly as its nested `Array(Tuple(key, value))`.
            return maxRowsFittingOneArrowBatch(
                assert_cast<const ColumnMap &>(column).getNestedColumn(),
                assert_cast<const DataTypeMap &>(*type).getNestedType(),
                begin,
                end,
                fixed_string_as_fixed_byte_array);
        }
        case TypeIndex::Tuple:
        {
            const auto & tuple = assert_cast<const ColumnTuple &>(column);
            const auto & elements = assert_cast<const DataTypeTuple &>(*type).getElements();
            size_t rows = num_rows;
            for (size_t i = 0; i != elements.size(); ++i)
            {
                rows = std::min(
                    rows,
                    maxRowsFittingOneArrowBatch(
                        tuple.getColumn(i), elements[i], begin, begin + rows, fixed_string_as_fixed_byte_array));
            }
            return rows;
        }
        /// Fixed-width Arrow types: a single values buffer, no offsets, nothing to exceed.
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
        case TypeIndex::UInt256:
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
        case TypeIndex::Time:
        case TypeIndex::Time64:
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
        case TypeIndex::IPv4:
        case TypeIndex::IPv6:
        case TypeIndex::UUID:
        case TypeIndex::Interval:
        {
            return num_rows;
        }
        default:
        {
            /// A type with no Arrow equivalent becomes a `Binary` column of the per-row `getDataAt` bytes.
            /// Unrecognized types land here too, so a type added later splits rather than overflows.
            if (type->haveMaximumSizeOfValue())
            {
                const size_t value_size = type->getMaximumSizeOfValueInMemory();
                return value_size == 0 ? num_rows : std::min<size_t>(num_rows, MAX_ARROW_BUFFER_SIZE / value_size);
            }
            UInt64 total = 0;
            for (size_t row = begin; row != end; ++row)
            {
                total += column.getDataAt(row).size();
                if (total > MAX_ARROW_BUFFER_SIZE)
                    return row - begin;
            }
            return num_rows;
        }
    }
}

}

#endif
