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
    SharedLibraryHandler(const std::string & library_path_, const std::vector<std::string> & library_settings, const Block & sample_block);

    SharedLibraryHandler(const SharedLibraryHandler & other);

    ~SharedLibraryHandler();

    BlockInputStreamPtr loadAll(size_t num_attributes);

    BlockInputStreamPtr loadIds(const std::vector<uint64_t> & ids, size_t num_attributes);

    BlockInputStreamPtr loadKeys(const Columns & key_columns);

    bool isModified();

    bool supportsSelectiveLoad();

    const Block & getSampleBlock() { return sample_block; }

private:
    Block dataToBlock(const ClickHouseLibrary::RawClickHouseLibraryTable data);

    std::string library_path;
    SharedLibraryPtr library;
    const Block sample_block;
    std::shared_ptr<CStringsHolder> settings_holder;
    void * lib_data;
};

using SharedLibraryHandlerPtr = std::shared_ptr<SharedLibraryHandler>;

}
