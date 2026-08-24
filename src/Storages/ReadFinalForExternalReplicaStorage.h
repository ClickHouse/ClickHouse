#pragma once

#include "config.h"

#if USE_MYSQL || USE_LIBPQXX

#include <Storages/StorageProxy.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

bool needRewriteQueryWithFinalForStorage(const Names & column_names, const StoragePtr & storage);

void readFinalFromNestedStorage(
    QueryPlan & query_plan,
    StoragePtr nested_storage,
    const Names & column_names,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams);

}

#endif
