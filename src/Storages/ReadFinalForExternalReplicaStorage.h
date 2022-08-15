#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL || USE_LIBPQXX

#include <Storages/StorageProxy.h>
#include <Processors/Pipe.h>


namespace DB
{

bool needRewriteQueryWithFinalForStorage(const Names & column_names, const StoragePtr & storage);

Pipe readFinalFromNestedStorage(
    StoragePtr nested_storage,
    const Names & column_names,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned int num_streams);

}

#endif
