#include <Functions/GatherUtils/Algorithms.h>

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Common/typeid_cast.h>

namespace DB::GatherUtils
{

size_t wrappedRangeBytes(const IColumn & column, size_t begin, size_t end)
{
    const size_t rows = end - begin;
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
        return rows + rangeBytes(nullable->getNestedColumn(), begin, end);

    if (const auto * tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        size_t bytes = 0;
        for (const auto & element : tuple->getColumns())
            bytes += rangeBytes(*element, begin, end);
        return bytes;
    }

    if (const auto * map = typeid_cast<const ColumnMap *>(&column))
        return rangeBytes(map->getNestedColumn(), begin, end);

    if (const auto * variant = typeid_cast<const ColumnVariant *>(&column))
    {
        size_t bytes = rows * (sizeof(ColumnVariant::Offset) + sizeof(ColumnVariant::Discriminator));
        const auto & discriminators = variant->getLocalDiscriminators();
        const auto & offsets = variant->getOffsets();
        if (rows == 1)
        {
            if (discriminators[begin] != ColumnVariant::NULL_DISCRIMINATOR)
                bytes += rangeBytes(variant->getVariantByLocalDiscriminator(discriminators[begin]), offsets[begin], offsets[begin] + 1);
            return bytes;
        }

        /// The rows of one variant in a range of the column are a range of that variant.
        VectorWithMemoryTracking<std::pair<size_t, size_t>> variant_ranges(variant->getNumVariants(), {0, 0});
        for (size_t i = begin; i < end; ++i)
        {
            if (discriminators[i] == ColumnVariant::NULL_DISCRIMINATOR)
                continue;
            auto & [first, count] = variant_ranges[discriminators[i]];
            if (count++ == 0)
                first = offsets[i];
        }

        for (size_t i = 0; i < variant_ranges.size(); ++i)
        {
            const auto & [first, count] = variant_ranges[i];
            if (count)
                bytes += rangeBytes(variant->getVariantByLocalDiscriminator(i), first, first + count);
        }
        return bytes;
    }

    if (const auto * dynamic = typeid_cast<const ColumnDynamic *>(&column))
        return rangeBytes(dynamic->getVariantColumn(), begin, end);

    if (const auto * object = typeid_cast<const ColumnObject *>(&column))
    {
        size_t bytes = rangeBytes(*object->getSharedDataPtr(), begin, end);
        for (const auto & [_, path] : object->getTypedPaths())
            bytes += rangeBytes(*path, begin, end);
        for (const auto & [_, path] : object->getDynamicPathsPtrs())
            bytes += rangeBytes(*path, begin, end);
        return bytes;
    }

    size_t bytes = 0;
    for (size_t i = begin; i < end; ++i)
        bytes += column.byteSizeAt(i);
    return bytes;
}

}
