#pragma once

#include "config.h"

#if USE_LANCE

#    include "LanceTable.h"

#    include <iostream>

namespace DB
{

class LanceTableReader
{
public:
    LanceTableReader(LanceTablePtr table_, size_t max_rows_in_block_);

    LanceTableReader(const LanceTableReader &) = delete;
    LanceTableReader(LanceTableReader && reader_);

    bool readNextBatch();

    template <typename T>
    std::span<T> getColumnFromCurrentBatch(const String & name, bool nullable)
    {
        lance::Column & column = getColumn(name, nullable);
        T * data_ptr = reinterpret_cast<T *>(column.data);
        std::span<T> result(data_ptr + rows_begin, data_ptr + rows_end);
        return result;
    }

    std::span<bool> getNulls(const String & name);

    ~LanceTableReader();

private:
    lance::Column & getColumn(const String & name, bool nullable);
    bool * getNullsFromColumn(const String & name);

    void FreeColumns();

private:
    LanceTablePtr table;
    lance::Reader * reader = nullptr;
    std::vector<lance::Column> columns_in_batch;
    std::vector<bool *> nulls;
    size_t max_rows_in_block;
    size_t rows_in_batch{0};
    size_t rows_begin{0};
    size_t rows_end{0};
};

} // namespace DB

#endif
