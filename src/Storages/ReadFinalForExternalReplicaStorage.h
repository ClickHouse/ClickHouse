#pragma once

#include "config_core.h"

#if USE_MYSQL || USE_LIBPQXX

#include <Storages/StorageProxy.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

bool needRewriteQueryWithFinalForStorage(const Names & column_names, const StoragePtr & storage);

Pipe readFinalFromNestedStorage(
    StoragePtr nested_storage,
    const Names & column_names,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned int num_streams);

}

#endif
