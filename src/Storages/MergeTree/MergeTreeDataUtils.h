#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

bool getQueryProcessingStageWithAggregateProjection(
    ContextPtr query_context, const StorageMetadataPtr & metadata_snapshot, SelectQueryInfo & query_info);

}
