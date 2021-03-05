#pragma once

#include <Common/SharedLibrary.h>
#include <common/logger_useful.h>
#include <DataStreams/OneBlockInputStream.h>

#include "LibraryUtils.h"


namespace DB
{

class SharedLibraryHandler
{

public:
    SharedLibraryHandler() {}

    SharedLibraryHandler(const SharedLibraryHandler & other);

    ~SharedLibraryHandler();

    void libNew(const std::string & path, const std::string & settings);

    BlockInputStreamPtr loadAll(const std::string & attributes_string, const Block & sample_block);

    BlockInputStreamPtr loadIds(const std::string & ids_string, const std::string & attributes_string, const Block & sample_block);

    bool isModified();

    bool supportsSelectiveLoad();

private:
    void libDelete();

    Block dataToBlock(const Block & sample_block, const void * data);

    std::string library_path;

    SharedLibraryPtr library;

    std::shared_ptr<CStringsHolder> settings_holder;

    void * lib_data;
};

using SharedLibraryHandlerPtr = std::shared_ptr<SharedLibraryHandler>;

}
