#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <memory>

#include <roaring/roaring64map.hh>

namespace DB
{
class IObjectStorage;
using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;
}

namespace DB::Iceberg
{

std::unique_ptr<roaring::Roaring64Map> readIcebergDeletionVector(
    const String & file_path,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    UInt64 max_content_size_in_bytes,
    const ObjectStoragePtr & object_storage,
    ContextPtr context,
    LoggerPtr log);

}
