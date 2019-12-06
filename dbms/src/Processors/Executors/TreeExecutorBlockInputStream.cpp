#include <Processors/Executors/TreeExecutorBlockInputStream.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <stack>

namespace DB
{

static void checkProcessorHasSingleOutput(IProcessor * processor)
{
    size_t num_outputs = processor->getOutputs().size();
    if (num_outputs != 1)
        throw Exception("All processors in TreeExecutorBlockInputStream must have single output, "
                        "but processor with name " + processor->getName() + " has " + std::to_string(num_outputs),
                        ErrorCodes::LOGICAL_ERROR);
}

/// Check tree invariants (described in TreeExecutor.h).
/// Collect sources with progress.
static void validateTree(const Processors & processors, IProcessor * root, std::vector<ISourceWithProgress *> & sources)
{
    std::unordered_map<IProcessor *, size_t> index;

    for (auto & processor : processors)
    {
        bool is_inserted = index.try_emplace(processor.get(), index.size()).second;

        if (!is_inserted)
            throw Exception("Duplicate processor in TreeExecutorBlockInputStream with name " + processor->getName(),
                            ErrorCodes::LOGICAL_ERROR);
    }

    std::vector<bool> is_visited(processors.size(), false);
    std::stack<IProcessor *> stack;

    stack.push(root);

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
            throw Exception("Processor with name " + node->getName() + " was visited twice while traverse in TreeExecutorBlockInputStream. "
                            "Passed processors are not tree.", ErrorCodes::LOGICAL_ERROR);

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

    validateTree(processors, root, sources_with_progress);

    input_port = std::make_unique<InputPort>(getHeader(), root);
    connect(output_port, *input_port);
    input_port->setNeeded();
}

void TreeExecutorBlockInputStream::execute()
{
    std::stack<IProcessor *> stack;
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

    while (!stack.empty())
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
            {
                stack.pop();
                break;
            }
            case IProcessor::Status::Finished:
            {
                stack.pop();
                break;
            }
            case IProcessor::Status::Ready:
            {
                node->work();
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

Block TreeExecutorBlockInputStream::readImpl()
{
    while (true)
    {
        if (input_port->isFinished())
            return {};

        if (input_port->hasData())
            return getHeader().cloneWithColumns(input_port->pull().detachColumns());

        execute();
    }
}

void TreeExecutorBlockInputStream::setProgressCallback(const ProgressCallback & callback)
{
    for (auto & source : sources_with_progress)
        source->setProgressCallback(callback);
}

void TreeExecutorBlockInputStream::setProcessListElement(QueryStatus * elem)
{
    for (auto & source : sources_with_progress)
        source->setProcessListElement(elem);
}

void TreeExecutorBlockInputStream::setLimits(const IBlockInputStream::LocalLimits & limits_)
{
    for (auto & source : sources_with_progress)
        source->setLimits(limits_);
}

void TreeExecutorBlockInputStream::setQuota(const std::shared_ptr<QuotaContext> & quota_)
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

}
