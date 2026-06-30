#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <Columns/ColumnNullable.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace UniqueKeyEncoding
{

void encodeBlock(
    const Columns & columns,
    const IColumn::Permutation * permutation,
    size_t max_size,
    std::vector<String> & out)
{
    out.clear();
    if (columns.empty())
        return;

    const size_t num_rows = columns.front()->size();
    for (size_t c = 1; c < columns.size(); ++c)
    {
        const size_t sz = columns[c]->size();
        if (sz != num_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "UNIQUE KEY encoding: column[{}] size {} != column[0] size {}",
                            c, sz, num_rows);
    }

    out.resize(num_rows);
    if (num_rows == 0)
        return;

    /// Materialize ColumnConst so comparable serialization works.
    Columns materialized;
    materialized.reserve(columns.size());
    for (const auto & col : columns)
        materialized.push_back(col->convertToFullColumnIfConst());

    /// Pre-check all columns support comparable serialization.
    for (const auto & col_ptr : materialized)
    {
        const IColumn * col = col_ptr.get();
        const IColumn * inner = col;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(col))
            inner = &nullable->getNestedColumn();
        if (!inner->supportsComparableSerialization())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "UNIQUE KEY encoding: column type {} is not supported",
                            inner->getName());
    }

    /// Compute per-row serialized sizes (in original row order).
    PaddedPODArray<UInt64> row_sizes;
    for (const auto & col_ptr : materialized)
        col_ptr->collectComparableSerializedRowSizes(row_sizes);

    /// Pre-allocate per-row buffers in original row order.
    /// memories[i] points to the buffer for source row i.
    std::vector<String> row_buffers(num_rows);
    PaddedPODArray<char *> memories(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (row_sizes[i] > max_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "UNIQUE KEY encoded size exceeds unique_key_max_encoded_size={} bytes",
                            max_size);
        row_buffers[i].resize(row_sizes[i]);
        memories[i] = row_buffers[i].data();
    }

    /// Column-outer serialization: one virtual dispatch per column, zero extra copy.
    /// No permutation needed — serializes in original row order like community batchSerializeValueIntoMemory.
    for (const auto & col_ptr : materialized)
        col_ptr->batchSerializeComparableIntoMemory(memories);

    /// Output in permutation order.
    if (permutation)
    {
        for (size_t i = 0; i < num_rows; ++i)
            out[i] = std::move(row_buffers[(*permutation)[i]]);
    }
    else
    {
        for (size_t i = 0; i < num_rows; ++i)
            out[i] = std::move(row_buffers[i]);
    }
}

}

}
