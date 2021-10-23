#include "ClickHouseLibrarySource.h"
#include <IO/WriteHelpers.h>
#include <base/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_LIBRARY_ERROR;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


ClickHouseLibrarySource::ClickHouseLibrarySource(
        LibraryStatePtr state_,
        const Block & sample_block_,
        size_t max_block_size_)
    : SourceWithProgress(sample_block_.cloneEmpty())
    , sample_block(sample_block_)
    , max_block_size(max_block_size_)
    , state(std::move(state_))
{
    if (!state->data)
        throw Exception("LibraryDictionarySource: No data returned", ErrorCodes::EXTERNAL_LIBRARY_ERROR);

    columns_received = static_cast<const ClickHouseLibrary::Table *>(state->data);

    if (columns_received->error_code)
        throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR,
                        "LibraryDictionarySource: Returned error with code: {}, message: {}",
                        std::to_string(columns_received->error_code),
                        (columns_received->error_string ? columns_received->error_string : "None"));
}

Chunk ClickHouseLibrarySource::generate()
{
    if (row_idx == columns_received->size)
        return {};

    MutableColumns columns = sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (row_idx < columns_received->size)
    {
        if (columns.size() != columns_received->data[row_idx].size)
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
                "LibraryDictionarySource: Returned unexpected number of columns: {}, must be {}",
                toString(columns_received->data[row_idx].size), toString(columns.size()));

        for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx)
        {
            const auto & field = columns_received->data[row_idx].data[col_idx];
            if (field.data)
            {
                columns[col_idx]->insertData(static_cast<const char *>(field.data), field.size);
            }
            else
            {
                /// sample_block contains null_value (from config) inside corresponding column
                const auto & col = sample_block.getByPosition(col_idx);
                columns[col_idx]->insertFrom(*(col.column), 0);
            }
        }

        ++row_idx;
        if (++num_rows == max_block_size)
            break;
    }

    LOG_TRACE(&Poco::Logger::get("KSSENII"), "Sending rows: {}", num_rows);
    return Chunk(std::move(columns), num_rows);
}

}
