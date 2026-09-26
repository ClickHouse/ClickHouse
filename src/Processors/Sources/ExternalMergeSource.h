#pragma once

#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/ISource.h>
#include <Common/Logger.h>

#include <functional>
#include <vector>

namespace DB
{

class BufferingToFileSink;

/// Reduces sorted temporary files to a bounded number by merging selected files into replacement
/// files, then streams the final merge. Each group supplies its own intermediate merger so callers
/// can retain different column layouts and duplicate-handling rules. The limit applies to file readers
/// owned by this coordinator; it does not bound total query memory or readers in other coordinators.
/// An optional in-memory tail joins only the final merge and does not count toward the file limit.
/// A zero fan-in limit sends every file directly to the final merge.
class ExternalMergeSource final : public IProcessor
{
public:
    struct Run
    {
        explicit Run(TemporaryBlockStreamHolder file_);

        TemporaryBlockStreamHolder file;
        size_t compressed_size;
    };
    using Runs = std::vector<Run>;
    using MergeFactory = std::function<ProcessorPtr(const SharedHeaders &)>;

    /// Runs within a group share a column layout and intermediate merge policy. Each merge selects
    /// the group with the fewest compressed input bytes per file removed, using its smallest files.
    /// Callers must encode required tie-breaking in row comparisons rather than relying on file order.
    /// The final merge accepts all groups.
    class Group
    {
    public:
        Group(Runs runs_, MergeFactory merge_) : runs(std::move(runs_)), merge(std::move(merge_))
        {
        }

    private:
        friend class ExternalMergeSource;
        Runs runs;
        MergeFactory merge;
    };

    ExternalMergeSource(
        SharedHeader header, std::vector<Group> groups_, SourcePtr tail_, MergeFactory final_merge_, size_t max_fan_in_,
        TemporaryDataOnDiskScopePtr tmp_data_, size_t min_free_disk_space_, LoggerPtr log_);

    static size_t validateFanIn(size_t fan_in);

    String getName() const override { return "ExternalMergeSource"; }
    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

private:
    enum class Stage
    {
        Preparing,
        Connecting,
        Writing,
        Removing,
        Reading,
    };

    static size_t getSmallestRunsCompressedSize(const Runs & runs, size_t num_runs);
    void selectMergeGroup();
    void prepareMerge();
    void addRun(Run run, SharedHeaders & headers);

    std::vector<Group> groups;
    SourcePtr tail;
    MergeFactory final_merge;
    const size_t max_fan_in;
    TemporaryDataOnDiskScopePtr tmp_data;
    const size_t min_free_disk_space;
    LoggerPtr log;
    size_t num_files = 0;
    size_t group_index = 0;
    size_t merged_inputs = 0;
    Stage stage = Stage::Preparing;
    Processors processors;
    std::shared_ptr<BufferingToFileSink> sink;
    ProcessorPtr merger;
};

}
