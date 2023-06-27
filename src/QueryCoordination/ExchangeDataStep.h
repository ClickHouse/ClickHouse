#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/StreamLocalLimits.h>

namespace DB
{

class ExchangeDataStep final : public ISourceStep
{

public:
    struct SortInfo
    {
        size_t max_block_size;
        bool always_read_till_end = false;
        UInt64 limit;
        SortDescription result_description;
    };


    ExchangeDataStep(const DataStream & data_stream, StorageLimitsList & storage_limits_)
        : ISourceStep(data_stream), storage_limits(std::make_shared<StorageLimitsList>(storage_limits_))
    {
    }

    String getName() const override { return "ExchangeData"; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override;

    void setPlanID(Int32 plan_id_)
    {
        plan_id = plan_id_;
    }

    void setFragmentID(Int32 fragment_id_)
    {
        fragment_id = fragment_id_;
    }

    void setSources(const std::vector<String> & sources_)
    {
        sources = sources_;
    }

    void setSortInfo(const SortInfo & sort_info_)
    {
        sort_info = sort_info_;
        has_sort_info = true;
    }

private:
    void mergingSorted(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, UInt64 limit_);

    std::shared_ptr<const StorageLimitsList> storage_limits;

    std::vector<String> sources;

    Int32 plan_id;

    Int32 fragment_id;

    bool has_sort_info = false;

    SortInfo sort_info;
};

}
