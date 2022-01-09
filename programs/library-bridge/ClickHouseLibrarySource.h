#pragma once

#include <Processors/Sources/SourceWithProgress.h>
#include "LibraryUtils.h"


namespace DB
{

struct LibraryResultState
{
    ClickHouseLibrary::LibraryData data_ptr = nullptr;
    ClickHouseLibrary::LibraryDataDeleteFunc data_delete_func = nullptr;
    void * lib_data = nullptr;

    ClickHouseLibrary::RawClickHouseLibraryTable data = nullptr;

    ~LibraryResultState()
    {
        data_delete_func(lib_data, data_ptr);
    }
};

using LibraryStatePtr = std::unique_ptr<LibraryResultState>;


class ClickHouseLibrarySource : public SourceWithProgress
{
public:
    ClickHouseLibrarySource(
        LibraryStatePtr state_,
        const Block & sample_block_,
        size_t max_block_size_);

    ~ClickHouseLibrarySource() override = default;

    String getName() const override { return "ClickHouseLibrarySource"; }

    Chunk generate() override;

private:
    Block sample_block;

    size_t max_block_size;

    const ClickHouseLibrary::Table * columns_received;

    size_t row_idx = 0;

    LibraryStatePtr state;
};

using ClickHouseLibrarySourcePtr = std::shared_ptr<ClickHouseLibrarySource>;

}
