#pragma once

#include <Common/SharedLibrary.h>
#include <Common/logger_useful.h>
#include "ExternalDictionaryLibraryUtils.h"


namespace DB
{

/// A class that manages all operations with library dictionary.
/// Every library dictionary source has its own object of this class, accessed by UUID.
class ExternalDictionaryLibraryHandler
{

public:
    ExternalDictionaryLibraryHandler(
        const std::string & library_path_,
        const std::vector<std::string> & library_settings,
        const Block & sample_block_,
        const std::vector<std::string> & attributes_names_);

    ExternalDictionaryLibraryHandler(const ExternalDictionaryLibraryHandler & other);

    ExternalDictionaryLibraryHandler & operator=(const ExternalDictionaryLibraryHandler & other) = delete;

    ~ExternalDictionaryLibraryHandler();

    Block loadAll();

    Block loadIds(const std::vector<uint64_t> & ids);

    Block loadKeys(const Columns & key_columns);

    bool isModified();

    bool supportsSelectiveLoad();

    const Block & getSampleBlock() { return sample_block; }

private:
    Block dataToBlock(ExternalDictionaryLibraryAPI::RawClickHouseLibraryTable data);

    std::string library_path;
    const Block sample_block;
    std::vector<std::string> attributes_names;

    SharedLibraryPtr library;
    std::shared_ptr<CStringsHolder> settings_holder;
    void * lib_data;
};

using SharedLibraryHandlerPtr = std::shared_ptr<ExternalDictionaryLibraryHandler>;

}
