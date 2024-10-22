#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/TemporaryDataOnDisk.h>

namespace DB
{

/// Sort data stream
class SortingStep : public ITransformingStep
{
public:
    enum class Type : uint8_t
    {
        Full,
        FinishSorting,
        MergingSorted,
    };

    struct Settings
    {
        size_t max_block_size;
        SizeLimits size_limits;
        size_t max_bytes_before_remerge = 0;
        double remerge_lowered_memory_bytes_ratio = 0;
        size_t max_bytes_before_external_sort = 0;
        TemporaryDataOnDiskScopePtr tmp_data = nullptr;
        size_t min_free_disk_space = 0;
        size_t max_block_bytes = 0;
        size_t read_in_order_use_buffering = 0;

        explicit Settings(const Context & context);
        explicit Settings(size_t max_block_size_);
    };

    /// Full
    SortingStep(
        const Header & input_header,
        SortDescription description_,
        UInt64 limit_,
        const Settings & settings_);

    /// Full with partitioning
    SortingStep(
        const Header & input_header,
        const SortDescription & description_,
        const SortDescription & partition_by_description_,
        UInt64 limit_,
        const Settings & settings_);

    /// FinishSorting
    SortingStep(
        const Header & input_header,
        SortDescription prefix_description_,
        SortDescription result_description_,
        size_t max_block_size_,
        UInt64 limit_);

    /// MergingSorted
    SortingStep(
        const Header & input_header,
        SortDescription sort_description_,
        size_t max_block_size_,
        UInt64 limit_ = 0,
        bool always_read_till_end_ = false
    );

    String getName() const override { return "Sorting"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    UInt64 getLimit() const { return limit; }
    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    const SortDescription & getSortDescription() const override { return result_description; }

    bool hasPartitions() const { return !partition_by_description.empty(); }

    void convertToFinishSorting(SortDescription prefix_description, bool use_buffering_);

    Type getType() const { return type; }
    const Settings & getSettings() const { return sort_settings; }

    static void fullSortStreams(
        QueryPipelineBuilder & pipeline,
        const Settings & sort_settings,
        const SortDescription & result_sort_desc,
        UInt64 limit_,
        bool skip_partial_sort = false);

private:
    void scatterByPartitionIfNeeded(QueryPipelineBuilder& pipeline);
    void updateOutputHeader() override;

    static void mergeSorting(
        QueryPipelineBuilder & pipeline,
        const Settings & sort_settings,
        const SortDescription & result_sort_desc,
        UInt64 limit_);

    void mergingSorted(
        QueryPipelineBuilder & pipeline,
        const SortDescription & result_sort_desc,
        UInt64 limit_);
    void finishSorting(
        QueryPipelineBuilder & pipeline,
        const SortDescription & input_sort_desc,
        const SortDescription & result_sort_desc,
        UInt64 limit_);
    void fullSort(
        QueryPipelineBuilder & pipeline,
        const SortDescription & result_sort_desc,
        UInt64 limit_,
        bool skip_partial_sort = false);

    Type type;

    SortDescription prefix_description;
    const SortDescription result_description;

    SortDescription partition_by_description;

    UInt64 limit;
    bool always_read_till_end = false;
    bool use_buffering = false;

    Settings sort_settings;
};

}
