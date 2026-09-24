#include <Processors/Sources/ExternalMergeSource.h>

#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Common/ProfileEvents.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>

#include <algorithm>
#include <limits>
#include <queue>

namespace ProfileEvents
{
    extern const Event ExternalProcessingIntermediateMerge;
    extern const Event ExternalProcessingIntermediateMergeInputs;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ExternalMergeSource::Run::Run(TemporaryBlockStreamHolder file_)
    : file(std::move(file_))
    , compressed_size(file.getHolder()->getStat().compressed_size)
{
}

size_t ExternalMergeSource::validateFanIn(size_t fan_in)
{
    if (fan_in == 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External merge fan-in must be 0 (unlimited) or at least 2, got {}", fan_in);
    return fan_in;
}

ExternalMergeSource::ExternalMergeSource(
    SharedHeader header, std::vector<Group> groups_, SourcePtr tail_, MergeFactory final_merge_, size_t max_fan_in_,
    TemporaryDataOnDiskScopePtr tmp_data_, size_t min_free_disk_space_, LoggerPtr log_)
    : IProcessor({}, {std::move(header)})
    , groups(std::move(groups_))
    , tail(std::move(tail_))
    , final_merge(std::move(final_merge_))
    , max_fan_in(max_fan_in_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , log(std::move(log_))
{
    chassert(max_fan_in == 0 || max_fan_in >= 2);

    /// Intermediate merges cannot combine different groups. Each nonempty group therefore retains
    /// at least one file, and the final merge must have room for all of them.
    chassert(max_fan_in == 0 || groups.size() <= max_fan_in);
    for (const auto & group : groups)
        num_files += group.runs.size();
    chassert(num_files);
    LOG_TRACE(log, "Preparing external merge with {} files in {} groups (fan-in limit: {})",
        num_files, groups.size(), max_fan_in);
    if (max_fan_in && num_files > max_fan_in)
        for (auto & group : groups)
            std::ranges::make_heap(group.runs, std::greater{}, &Run::compressed_size);
}

IProcessor::Status ExternalMergeSource::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (stage == Stage::Preparing)
        return Status::Ready;
    if (stage == Stage::Connecting || stage == Stage::Removing)
        return Status::UpdatePipeline;

    if (stage == Stage::Writing)
    {

        /// A merge with a row limit can finish its writer before its readers reach the end of their
        /// files. Wait for the writer to finalize its file and for all readers to release their
        /// buffers and prefetched blocks before starting another intermediate merge.
        for (auto & completion : inputs)
        {
            chassert(!completion.hasData());
            if (!completion.isFinished())
            {
                completion.setNeeded();
                return Status::NeedData;
            }
        }
        return Status::Ready;
    }

    chassert(stage == Stage::Reading);
    auto & input = inputs.back();
    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    output.pushData(input.pullData());
    return Status::PortFull;
}

void ExternalMergeSource::addRun(Run run, SharedHeaders & headers)
{
    auto source = std::make_shared<BufferingFromFileSource>(std::move(run.file));
    headers.push_back(source->getPort().getSharedHeader());
    processors.push_back(std::move(source));
}

size_t ExternalMergeSource::getSmallestRunsCompressedSize(const Runs & runs, size_t num_runs)
{
    chassert(num_runs > 0 && num_runs <= runs.size());

    /// Each group is a min-heap by compressed size. Visit its smallest entries through a queue of
    /// heap positions, adding each entry's children when it is visited. Only candidate inputs and
    /// their immediate children are examined, and file ownership and heap ordering stay unchanged.
    auto compare = [&runs](size_t lhs, size_t rhs)
    {
        return runs[lhs].compressed_size > runs[rhs].compressed_size;
    };
    std::priority_queue<size_t, std::vector<size_t>, decltype(compare)> candidates(compare);
    candidates.push(0);
    size_t compressed_size = 0;
    for (size_t i = 0; i < num_runs; ++i)
    {
        const auto position = candidates.top();
        candidates.pop();
        compressed_size += runs[position].compressed_size;
        for (const auto child : {2 * position + 1, 2 * position + 2})
            if (child < runs.size())
                candidates.push(child);
    }
    return compressed_size;
}

void ExternalMergeSource::selectMergeGroup()
{
    double lowest_bytes_per_removed_file = std::numeric_limits<double>::infinity();
    merged_inputs = 0;
    for (size_t index = 0; index < groups.size(); ++index)
    {
        const auto & runs = groups[index].runs;
        if (runs.size() < 2)
            continue;

        /// Merging several files into one reduces the file count by one less than the input count.
        /// Select only enough files to reach the cap, subject to the reader limit and group size.
        /// For example, 65 files at a cap of 64 need only a two-input merge.
        const size_t num_inputs = std::min({max_fan_in, num_files - max_fan_in + 1, runs.size()});

        /// Groups can remove different numbers of files in one merge. Compare compressed input bytes
        /// per file removed to favor inexpensive reductions without predicting output compression or
        /// deduplication. Recompute after each merge because its replacement file changes the choices.
        const size_t compressed_size = getSmallestRunsCompressedSize(runs, num_inputs);
        const double bytes_per_removed_file = static_cast<double>(compressed_size) / static_cast<double>(num_inputs - 1);
        if (bytes_per_removed_file < lowest_bytes_per_removed_file)
        {
            lowest_bytes_per_removed_file = bytes_per_removed_file;
            group_index = index;
            merged_inputs = num_inputs;
        }
    }

    /// More files than the cap, with at most that many groups, guarantees a group with two files.
    chassert(merged_inputs >= 2);
}

void ExternalMergeSource::prepareMerge()
{
    chassert(processors.empty());
    SharedHeaders headers;
    const bool is_final_merge = !max_fan_in || num_files <= max_fan_in;
    if (is_final_merge)
    {
        for (auto & group : groups)
        {
            for (auto & run : group.runs)
                addRun(std::move(run), headers);
            group.runs.clear();
        }
        if (tail)
            headers.push_back(tail->getPort().getSharedHeader());
    }
    else
    {
        selectMergeGroup();
        auto & group = groups[group_index];
        for (size_t i = 0; i < merged_inputs; ++i)
        {
            std::ranges::pop_heap(group.runs, std::greater{}, &Run::compressed_size);
            addRun(std::move(group.runs.back()), headers);
            group.runs.pop_back();
        }
    }

    const auto & merge = is_final_merge ? final_merge : groups[group_index].merge;
    merger = merge(headers);
    chassert(merger->getInputs().size() == headers.size());
    chassert(merger->getOutputs().size() == 1);

    if (is_final_merge)
    {
        LOG_TRACE(log, "Starting final external merge with {} files and {} in-memory inputs "
            "(fan-in limit: {}, query memory: {})",
            num_files, tail ? 1 : 0, max_fan_in, formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()));
    }
    else
    {
        auto header = merger->getOutputs().front().getSharedHeader();
        sink = std::make_shared<BufferingToFileSink>(
            header, TemporaryBlockStreamHolder(header, tmp_data, min_free_disk_space), log);
        LOG_TRACE(log, "Starting intermediate external merge with {} inputs "
            "(group: {} of {}, remaining files: {}, fan-in limit: {}, query memory: {})",
            merged_inputs, group_index + 1, groups.size(), num_files, max_fan_in,
            formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()));
    }
    stage = Stage::Connecting;
}

void ExternalMergeSource::work()
{
    if (stage == Stage::Preparing)
        prepareMerge();
    else
    {
        chassert(stage == Stage::Writing);
        auto & runs = groups[group_index].runs;
        runs.emplace_back(sink->releaseFile());
        std::ranges::push_heap(runs, std::greater{}, &Run::compressed_size);
        num_files -= merged_inputs - 1;
        ProfileEvents::increment(ProfileEvents::ExternalProcessingIntermediateMerge);
        ProfileEvents::increment(ProfileEvents::ExternalProcessingIntermediateMergeInputs, merged_inputs);
        LOG_TRACE(log, "Finished intermediate external merge of {} inputs; {} files remain", merged_inputs, num_files);
        stage = Stage::Removing;
    }
}

IProcessor::PipelineUpdate ExternalMergeSource::updatePipeline()
{
    if (stage == Stage::Removing)
    {
        for (auto & input : inputs)
            disconnect(input.getOutputPort(), input);
        inputs.clear();
        sink.reset();
        merger.reset();
        stage = Stage::Preparing;
        return {.to_add = {}, .to_remove = std::move(processors)};
    }

    chassert(stage == Stage::Connecting);
    auto input = merger->getInputs().begin();
    for (const auto & source : processors)
    {
        connect(source->getOutputs().front(), *input++);
        auto & file_source = assert_cast<BufferingFromFileSource &>(*source);
        inputs.emplace_back(Block(), this);
        connect(file_source.getCompletionPort(), inputs.back());
    }
    if (!sink && tail)
    {
        connect(tail->getPort(), *input++);
        processors.push_back(std::move(tail));
    }
    processors.push_back(merger);

    if (sink)
    {
        connect(merger->getOutputs().front(), sink->getPort());
        inputs.emplace_back(Block(), this);
        connect(sink->getCompletionPort(), inputs.back());
        processors.push_back(sink);
        stage = Stage::Writing;
    }
    else
    {
        inputs.emplace_back(outputs.front().getHeader(), this);
        connect(merger->getOutputs().front(), inputs.back());
        stage = Stage::Reading;
    }
    return {.to_add = processors, .to_remove = {}};
}

}
