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
    SharedLibraryHandler(const std::string & dictionary_id_);

    ~SharedLibraryHandler();

    const std::string & getDictID() { return dictionary_id; }

    //void libDelete();

    void libNew(const std::string & path, const std::string & settings);

    BlockInputStreamPtr loadAll(const std::string & attributes_string, const Block & sample_block);

    BlockInputStreamPtr loadIds(const std::string & ids_string, const std::string & attributes_string, const Block & sample_block);

private:
    Block dataToBlock(const Block & sample_block, const void * data);

    Poco::Logger * log;

    std::string dictionary_id;

    std::string library_path;

    SharedLibraryPtr library;

    std::shared_ptr<CStringsHolder> settings_holder;

    void * lib_data;
};

using SharedLibraryHandlerPtr = std::shared_ptr<SharedLibraryHandler>;

}
