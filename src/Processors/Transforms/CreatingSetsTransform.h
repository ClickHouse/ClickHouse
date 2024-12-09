#pragma once

#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/IAccumulatingTransform.h>
#include <QueryPipeline/Chain.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Interpreters/PreparedSets.h>
#include <Common/Stopwatch.h>

#include <Poco/Logger.h>

namespace DB
{

class QueryStatus;
struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;

class PushingPipelineExecutor;

/// This processor creates set during execution.
/// Don't return any data. Sets are created when Finish status is returned.
/// In general, several work() methods need to be called to finish.
/// Independent processors is created for each subquery.
class CreatingSetsTransform : public IAccumulatingTransform
{
public:
    CreatingSetsTransform(
        Block in_header_,
        Block out_header_,
        SetAndKeyPtr set_and_key_,
        StoragePtr external_table_,
        SizeLimits network_transfer_limits_,
        PreparedSetsCachePtr prepared_sets_cache_);

    ~CreatingSetsTransform() override;

    String getName() const override { return "CreatingSetsTransform"; }

    void work() override;
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    SetAndKeyPtr set_and_key;
    StoragePtr external_table;
    std::optional<std::promise<SetPtr>> promise_to_build;

    QueryPipeline table_out;
    std::unique_ptr<PushingPipelineExecutor> executor;
    UInt64 read_rows = 0;
    bool set_from_cache = false;
    Stopwatch watch;

    bool done_with_set = true;
    bool done_with_table = true;

    SizeLimits network_transfer_limits;
    PreparedSetsCachePtr prepared_sets_cache;

    size_t rows_to_transfer = 0;
    size_t bytes_to_transfer = 0;

    using Logger = Poco::Logger;
    LoggerPtr log = getLogger("CreatingSetsTransform");

    bool is_initialized = false;

    void init();
    void startSubquery();
    void finishSubquery();
};

}
