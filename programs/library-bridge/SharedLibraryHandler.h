#pragma once

#include <Common/SharedLibrary.h>
#include <base/logger_useful.h>
#include "LibraryUtils.h"


namespace DB
{

class ClickHouseLibrarySource;
using ClickHouseLibrarySourcePtr = std::shared_ptr<ClickHouseLibrarySource>;

/// A class that manages all operations with library dictionary.
/// Every library dictionary source has its own object of this class, accessed by UUID.
class SharedLibraryHandler
{

public:
    SharedLibraryHandler(
        const std::string & library_path_,
        const std::vector<std::string> & library_settings,
        const Block & sample_block_,
        size_t max_block_size_,
        const std::vector<std::string> & attributes_names_);

    SharedLibraryHandler(const SharedLibraryHandler & other);

    SharedLibraryHandler & operator=(const SharedLibraryHandler & other) = delete;

    ~SharedLibraryHandler();

    ClickHouseLibrarySourcePtr loadAll();

    ClickHouseLibrarySourcePtr loadIds(const std::vector<uint64_t> & ids);

    ClickHouseLibrarySourcePtr loadKeys(const Columns & key_columns);

    bool isModified();

    bool supportsSelectiveLoad();

    const Block & getSampleBlock() { return sample_block; }

private:
    std::string library_path;
    const Block sample_block;
    size_t max_block_size;
    std::vector<std::string> attributes_names;

    SharedLibraryPtr library;
    std::shared_ptr<CStringsHolder> settings_holder;
    void * lib_data;
};

using SharedLibraryHandlerPtr = std::shared_ptr<SharedLibraryHandler>;

}
