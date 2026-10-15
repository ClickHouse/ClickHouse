#pragma once

#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MergeTree/MergeTask.h>

#include <vector>


namespace DB
{

class ReadBuffer;
class WriteBuffer;
class TemporaryDataBuffer;

size_t rewriteRowsSources(
    ReadBuffer & in,
    WriteBuffer & out,
    std::vector<QueryPipeline> row_exists_per_part,
    TemporaryDataBuffer * ttl_kept_rows,
    const MergeTask::GlobalRuntimeContext & global_ctx);

}
