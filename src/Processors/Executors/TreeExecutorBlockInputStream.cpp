#include <Processors/Executors/TreeExecutorBlockInputStream.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Interpreters/ProcessList.h>
#include <stack>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/LimitTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int LOGICAL_ERROR;
}

static void checkProcessorHasSingleOutput(IProcessor * processor)
{
    /// SourceFromInputStream may have totals port. Skip this check.
    if (typeid_cast<const SourceFromInputStream *>(processor))
        return;

    size_t num_outputs = processor->getOutputs().size();
    if (num_outputs != 1)
        throw Exception("All processors in TreeExecutorBlockInputStream must have single output, "
                        "but processor with name " + processor->getName() + " has " + std::to_string(num_outputs),
                        ErrorCodes::LOGICAL_ERROR);
}

/// Check tree invariants (described in TreeExecutor.h).
/// Collect sources with progress.
static void validateTree(
    const Processors & processors,
    IProcessor * root, IProcessor * totals_root, IProcessor * extremes_root,
    std::vector<ISourceWithProgress *> & sources)
{
    std::unordered_map<IProcessor *, size_t> index;

    for (const auto & processor : processors)
    {
        bool is_inserted = index.try_emplace(processor.get(), index.size()).second;

        if (!is_inserted)
            throw Exception("Duplicate processor in TreeExecutorBlockInputStream with name " + processor->getName(),
                            ErrorCodes::LOGICAL_ERROR);
    }

    std::vector<bool> is_visited(processors.size(), false);
    std::stack<IProcessor *> stack;

    stack.push(root);
    if (totals_root)
        stack.push(totals_root);
    if (extremes_root)
        stack.push(extremes_root);

    while (!stack.empty())
    {
        IProcessor * node = stack.top();
        stack.pop();

        auto it = index.find(node);

        if (it == index.end())
            throw Exception("Processor with name " + node->getName() + " "
                            "was not mentioned in list passed to TreeExecutorBlockInputStream, "
                            "but was traversed to from other processors.", ErrorCodes::LOGICAL_ERROR);

        size_t position = it->second;

        if (is_visited[position])
        {
            /// SourceFromInputStream may have totals port. Skip this check.
            if (typeid_cast<const SourceFromInputStream *>(node))
                continue;

            throw Exception("Processor with name " + node->getName() +
                            " was visited twice while traverse in TreeExecutorBlockInputStream. "
                            "Passed processors are not tree.", ErrorCodes::LOGICAL_ERROR);
        }

        is_visited[position] = true;

        checkProcessorHasSingleOutput(node);

        auto & children = node->getInputs();
        for (auto & child : children)
            stack.push(&child.getOutputPort().getProcessor());

        /// Fill sources array.
        if (children.empty())
        {
            if (auto * source = dynamic_cast<ISourceWithProgress *>(node))
                sources.push_back(source);
        }
    }

    for (size_t i = 0; i < is_visited.size(); ++i)
        if (!is_visited[i])
            throw Exception("Processor with name " + processors[i]->getName() +
                            " was not visited by traverse in TreeExecutorBlockInputStream.", ErrorCodes::LOGICAL_ERROR);
}

void TreeExecutorBlockInputStream::init()
{
    if (processors.empty())
        throw Exception("No processors were passed to TreeExecutorBlockInputStream.", ErrorCodes::LOGICAL_ERROR);

    root = &output_port.getProcessor();
    IProcessor * totals_root = nullptr;
    IProcessor * extremes_root = nullptr;

    if (totals_port)
        totals_root = &totals_port->getProcessor();

    if (extremes_port)
        extremes_root = &extremes_port->getProcessor();

    validateTree(processors, root, totals_root, extremes_root, sources_with_progress);

    input_port = std::make_unique<InputPort>(getHeader(), root);
    connect(output_port, *input_port);
    input_port->setNeeded();

    if (totals_port)
    {
        input_totals_port = std::make_unique<InputPort>(totals_port->getHeader(), root);
        connect(*totals_port, *input_totals_port);
        input_totals_port->setNeeded();
    }

    if (extremes_port)
    {
        input_extremes_port = std::make_unique<InputPort>(extremes_port->getHeader(), root);
        connect(*extremes_port, *input_extremes_port);
        input_extremes_port->setNeeded();
    }

    initRowsBeforeLimit();
}

void TreeExecutorBlockInputStream::execute(bool on_totals, bool on_extremes)
{
    std::stack<IProcessor *> stack;

    if (on_totals)
        stack.push(&totals_port->getProcessor());
    else if (on_extremes)
        stack.push(&extremes_port->getProcessor());
    else
        stack.push(root);

    auto prepare_processor = [](IProcessor * processor)
    {
        try
        {
            return processor->prepare();
        }
        catch (Exception & exception)
        {
            exception.addMessage(" While executing processor " + processor->getName());
            throw;
        }
    };

    while (!stack.empty() && !is_cancelled)
    {
        IProcessor * node = stack.top();

        auto status = prepare_processor(node);

        switch (status)
        {
            case IProcessor::Status::NeedData:
            {
                auto & inputs = node->getInputs();

                if (inputs.empty())
                    throw Exception("Processors " + node->getName() + " with empty input "
                                    "has returned NeedData in TreeExecutorBlockInputStream", ErrorCodes::LOGICAL_ERROR);

                bool all_finished = true;

                for (auto & input : inputs)
                {
                    if (input.isFinished())
                        continue;

                    all_finished = false;

                    stack.push(&input.getOutputPort().getProcessor());
                }

                if (all_finished)
                    throw Exception("Processors " + node->getName() + " has returned NeedData in TreeExecutorBlockInputStream, "
                                    "but all it's inputs are finished.", ErrorCodes::LOGICAL_ERROR);
                break;
            }
            case IProcessor::Status::PortFull:
            case IProcessor::Status::Finished:
            {
                stack.pop();
                break;
            }
            case IProcessor::Status::Ready:
            {
                node->work();

                /// This is handled inside PipelineExecutor now,
                ///   and doesn't checked by processors as in IInputStream before.
                if (process_list_element && process_list_element->isKilled())
                    throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);

                break;
            }
            case IProcessor::Status::Async:
            case IProcessor::Status::Wait:
            case IProcessor::Status::ExpandPipeline:
            {
                throw Exception("Processor with name " + node->getName() + " "
                                "returned status " + IProcessor::statusToName(status) + " "
                                "which is not supported in TreeExecutorBlockInputStream.", ErrorCodes::LOGICAL_ERROR);
            }
        }
    }
}

void TreeExecutorBlockInputStream::initRowsBeforeLimit()
{
    std::vector<LimitTransform *> limit_transforms;
    std::vector<SourceFromInputStream *> sources;

    struct StackEntry
    {
        IProcessor * processor;
        bool visited_limit;
    };

    std::stack<StackEntry> stack;
    stack.push({root, false});

    while (!stack.empty())
    {
        auto * processor = stack.top().processor;
        bool visited_limit = stack.top().visited_limit;
        stack.pop();

        if (!visited_limit)
        {

            if (auto * limit = typeid_cast<LimitTransform *>(processor))
            {
                visited_limit = true;
                limit_transforms.emplace_back(limit);
            }

            if (auto * source = typeid_cast<SourceFromInputStream *>(processor))
                sources.emplace_back(source);
        }
        else if (auto * sorting = typeid_cast<PartialSortingTransform *>(processor))
        {
            if (!rows_before_limit_at_least)
                rows_before_limit_at_least = std::make_shared<RowsBeforeLimitCounter>();

            sorting->setRowsBeforeLimitCounter(rows_before_limit_at_least);

            /// Don't go to children. Take rows_before_limit from last PartialSortingTransform.
            continue;
        }

        for (auto & child_port : processor->getInputs())
        {
            auto * child_processor = &child_port.getOutputPort().getProcessor();
            stack.push({child_processor, visited_limit});
        }
    }

    if (!rows_before_limit_at_least && (!limit_transforms.empty() || !sources.empty()))
    {
        rows_before_limit_at_least = std::make_shared<RowsBeforeLimitCounter>();

        for (auto & limit : limit_transforms)
            limit->setRowsBeforeLimitCounter(rows_before_limit_at_least);

        for (auto & source : sources)
            source->setRowsBeforeLimitCounter(rows_before_limit_at_least);
    }

    /// If there is a limit, then enable rows_before_limit_at_least
    /// It is needed when zero rows is read, but we still want rows_before_limit_at_least in result.
    if (!limit_transforms.empty())
        rows_before_limit_at_least->add(0);
}

Block TreeExecutorBlockInputStream::readImpl()
{
    while (!is_cancelled)
    {
        if (input_port->isFinished())
        {
            if (totals_port && !input_totals_port->isFinished())
            {
                execute(true, false);
                if (input_totals_port->hasData())
                    totals = getHeader().cloneWithColumns(input_totals_port->pull().detachColumns());
            }

            if (extremes_port && !input_extremes_port->isFinished())
            {
                execute(false, true);
                if (input_extremes_port->hasData())
                    extremes = getHeader().cloneWithColumns(input_extremes_port->pull().detachColumns());
            }

            if (rows_before_limit_at_least && rows_before_limit_at_least->hasAppliedLimit())
                info.setRowsBeforeLimit(rows_before_limit_at_least->get());

            return {};
        }

        if (input_port->hasData())
        {
            auto chunk = input_port->pull();
            Block block = getHeader().cloneWithColumns(chunk.detachColumns());

            if (const auto & chunk_info = chunk.getChunkInfo())
            {
                if (const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(chunk_info.get()))
                {
                    block.info.bucket_num = agg_info->bucket_num;
                    block.info.is_overflows = agg_info->is_overflows;
                }
            }

            return block;
        }

        execute(false, false);
    }

    return {};
}

void TreeExecutorBlockInputStream::setProgressCallback(const ProgressCallback & callback)
{
    for (auto & source : sources_with_progress)
        source->setProgressCallback(callback);
}

void TreeExecutorBlockInputStream::setProcessListElement(QueryStatus * elem)
{
    process_list_element = elem;

    for (auto & source : sources_with_progress)
        source->setProcessListElement(elem);
}

void TreeExecutorBlockInputStream::setLimits(const IBlockInputStream::LocalLimits & limits_)
{
    for (auto & source : sources_with_progress)
        source->setLimits(limits_);
}

void TreeExecutorBlockInputStream::setQuota(const std::shared_ptr<const EnabledQuota> & quota_)
{
    for (auto & source : sources_with_progress)
        source->setQuota(quota_);
}

void TreeExecutorBlockInputStream::addTotalRowsApprox(size_t value)
{
    /// Add only for one source.
    if (!sources_with_progress.empty())
        sources_with_progress.front()->addTotalRowsApprox(value);
}

void TreeExecutorBlockInputStream::cancel(bool kill)
{
    IBlockInputStream::cancel(kill);

    for (auto & processor : processors)
        processor->cancel();
}

}
