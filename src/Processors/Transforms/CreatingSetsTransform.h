#pragma once
#include <Poco/Logger.h>
#include <Processors/IAccumulatingTransform.h>
#include <Interpreters/SubqueryForSet.h>
#include <Common/Stopwatch.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

class QueryStatus;
struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;

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
        SubqueryForSet subquery_for_set_,
        SizeLimits network_transfer_limits_,
        const Context & context_);

    String getName() const override { return "CreatingSetsTransform"; }

    void work() override;
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    SubqueryForSet subquery;

    BlockOutputStreamPtr table_out;
    UInt64 read_rows = 0;
    Stopwatch watch;

    bool done_with_set = true;
    bool done_with_join = true;
    bool done_with_table = true;

    SizeLimits network_transfer_limits;
    const Context & context;

    size_t rows_to_transfer = 0;
    size_t bytes_to_transfer = 0;

    using Logger = Poco::Logger;
    Poco::Logger * log = &Poco::Logger::get("CreatingSetsTransform");

    bool is_initialized = false;

    void init();
    void startSubquery();
    void finishSubquery();
};

}
