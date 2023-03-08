#include <vector>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Core/Field.h>
#include <DataTypes/Serializations/SerializationInfo.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String IColumn::dumpStructure() const
{
    WriteBufferFromOwnString res;
    res << getFamilyName() << "(size = " << size();

    forEachSubcolumn([&](const auto & subcolumn)
    {
        res << ", " << subcolumn->dumpStructure();
    });

    res << ")";
    return res.str();
}

void IColumn::insertFrom(const IColumn & src, size_t n)
{
    insert(src[n]);
}

ColumnPtr IColumn::createWithOffsets(const Offsets & offsets, const Field & default_field, size_t total_rows, size_t shift) const
{
    if (offsets.size() + shift != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Incompatible sizes of offsets ({}), shift ({}) and size of column {}", offsets.size(), shift, size());

    auto res = cloneEmpty();
    res->reserve(total_rows);

    ssize_t current_offset = -1;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        ssize_t offsets_diff = static_cast<ssize_t>(offsets[i]) - current_offset;
        current_offset = offsets[i];

        if (offsets_diff > 1)
            res->insertMany(default_field, offsets_diff - 1);

        res->insertFrom(*this, i + shift);
    }

    ssize_t offsets_diff = static_cast<ssize_t>(total_rows) - current_offset;
    if (offsets_diff > 1)
        res->insertMany(default_field, offsets_diff - 1);

    return res;
}

void IColumn::forEachSubcolumn(MutableColumnCallback callback)
{
    std::as_const(*this).forEachSubcolumn([&callback](const WrappedPtr & subcolumn)
    {
        callback(const_cast<WrappedPtr &>(subcolumn));
    });
}

void IColumn::forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    std::as_const(*this).forEachSubcolumnRecursively([&callback](const IColumn & subcolumn)
    {
        callback(const_cast<IColumn &>(subcolumn));
    });
}

bool isColumnNullable(const IColumn & column)
{
    return checkColumn<ColumnNullable>(column);
}

bool isColumnConst(const IColumn & column)
{
    return checkColumn<ColumnConst>(column);
}

const PartitionSelector::PartitionInfo & PartitionSelector::getPartitionInfo(size_t buckets, bool force_update) const
{
    if (partition_info.offsets_map.size() != selector.size() || partition_info.offsets_map.empty() || force_update) [[unlikely]]
    {
        auto rows = selector.size();
        std::vector<size_t> partitions_offset(buckets + 1, 0);
        Array offsets_map(rows, 0);
        for (size_t i = 0; i < rows; ++i)
        {
            partitions_offset[selector[i]]++;
        }
        for (size_t i = 1; i <= buckets; ++i)
        {
            partitions_offset[i] += partitions_offset[i - 1];
        }
        for (size_t i = rows; i-- > 0;)
        {
            offsets_map[partitions_offset[selector[i]] - 1] = i;
            partitions_offset[selector[i]]--;
        }
        partition_info.partitions_offset.swap(partitions_offset);
        partition_info.offsets_map.swap(offsets_map);
    }

    if (partition_info.partitions_offset.size() != buckets + 1) [[unlikely]]
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "buckets({}) is not match with {}. selector size:{}", buckets, partition_info.partitions_offset.size(), selector.size());
    }

    return partition_info;
}

}
