#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <Columns/ColumnNullable.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace UniqueKeyEncoding
{

void encodeBlock(
    const Columns & columns,
    const IColumn::Permutation * permutation,
    size_t max_size,
    VectorWithMemoryTracking<String> & out)
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

    if (permutation && permutation->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "UNIQUE KEY encoding: permutation size {} != number of rows {}",
                        permutation->size(), num_rows);

    if (permutation && std::any_of(permutation->begin(), permutation->end(), [&](size_t src) { return src >= num_rows; }))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "UNIQUE KEY encoding: permutation contains an out-of-range index (number of rows {})",
                        num_rows);

    if (num_rows == 0)
        return;

    for (const auto & col_ptr : columns)
    {
        col_ptr->batchSerializeAsComparable(num_rows, out, permutation, nullptr);

        for (size_t r = 0; r < num_rows; ++r)
        {
            if (out[r].size() > max_size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "UNIQUE KEY encoded size exceeds unique_key_max_encoded_size={} bytes",
                                max_size);
        }
    }
}

}

}
