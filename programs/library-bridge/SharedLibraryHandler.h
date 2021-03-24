#pragma once

#include <Common/SharedLibrary.h>
#include <common/logger_useful.h>
#include <DataStreams/OneBlockInputStream.h>
#include "LibraryUtils.h"


namespace DB
{

/// A class that manages all operations with library dictionary.
/// Every library dictionary source has its own object of this class, accessed by UUID.
class SharedLibraryHandler
{

public:
    SharedLibraryHandler(const std::string & library_path_, const std::vector<std::string> & library_settings);

    SharedLibraryHandler(const SharedLibraryHandler & other);

    ~SharedLibraryHandler();

    BlockInputStreamPtr loadAll(const Block & sample_block, size_t num_attributes);

    BlockInputStreamPtr loadIds(const std::vector<uint64_t> & ids, const Block & sample_block, size_t num_attributes);

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const Block & sample_block);

    bool isModified();

    bool supportsSelectiveLoad();

private:
    static Block dataToBlock(const Block & sample_block, const ClickHouseLibrary::RawClickHouseLibraryTable data);

    std::string library_path;
    SharedLibraryPtr library;
    std::shared_ptr<CStringsHolder> settings_holder;
    void * lib_data;
};

using SharedLibraryHandlerPtr = std::shared_ptr<SharedLibraryHandler>;

}
