#pragma once

#include <Storages/MergeTree/IDataPartStorage.h>
#include <base/types.h>

namespace DB
{

/// A segment of the text index.
/// Created during materialization of text index.
struct TextIndexSegment
{
    TextIndexSegment(DataPartStoragePtr part_storage_, String index_file_name_, size_t part_index_)
        : part_storage(std::move(part_storage_))
        , index_file_name(std::move(index_file_name_))
        , part_index(part_index_)
    {
    }

    DataPartStoragePtr part_storage;
    String index_file_name;
    size_t part_index;
};

}
