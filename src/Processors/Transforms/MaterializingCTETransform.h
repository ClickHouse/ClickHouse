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

/// This processor materializes CTE during execution.
/// Don't return any data. Tables are created when Finish status is returned.
/// In general, several work() methods need to be called to finish.
/// Independent processors is created for each CTE expressions.
class MaterializingCTETransform : public IAccumulatingTransform, WithContext
{
public:
    MaterializingCTETransform(
        ContextPtr context_,
        Block in_header_,
        Block out_header_,
        StoragePtr external_table_,
        const String & cte_table_name,
        SizeLimits network_transfer_limits_);

    ~MaterializingCTETransform() override;

    String getName() const override { return "MaterializingCTETransform"; }

    void work() override;
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    String cte_table_name;
    StoragePtr external_table;

    QueryPipeline table_out;
    std::unique_ptr<PushingPipelineExecutor> executor;
    UInt64 read_rows = 0;
    Stopwatch watch;

    bool done_with_table = true;

    SizeLimits network_transfer_limits;

    size_t rows_to_transfer = 0;
    size_t bytes_to_transfer = 0;

    using Logger = Poco::Logger;
    LoggerPtr log = getLogger("MaterializingCTETransform");

    bool is_initialized = false;

    void init();
    void startSubquery();
    void finishSubquery();
};

}
