#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <Columns/ColumnNullable.h>
#include <Common/Exception.h>

#include <cstddef>


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

    /// Materialize ColumnConst so batchSerializeAsComparable sees concrete column types.
    Columns materialized;
    materialized.reserve(columns.size());
    for (const auto & col : columns)
        materialized.push_back(col->convertToFullColumnIfConst());

    for (const auto & col_ptr : materialized)
    {
        col_ptr->batchSerializeAsComparable(num_rows, out, permutation);

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
