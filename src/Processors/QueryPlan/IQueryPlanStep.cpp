#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/IProcessor.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

IQueryPlanStep::IQueryPlanStep()
{
    step_index = CurrentThread::isInitialized() ? CurrentThread::get().getNextPlanStepIndex() : 0;
}

void IQueryPlanStep::updateInputHeaders(Headers input_headers_)
{
    input_headers = std::move(input_headers_);
    updateOutputHeader();
}

void IQueryPlanStep::updateInputHeader(Header input_header, size_t idx)
{
    if (idx >= input_headers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot update input header {} for step {} because it has only {} headers",
            idx, getName(), input_headers.size());

    input_headers[idx] = input_header;
    updateOutputHeader();
}

const Header & IQueryPlanStep::getOutputHeader() const
{
    if (!hasOutputHeader())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPlanStep {} does not have output stream.", getName());

    return *output_header;
}

const SortDescription & IQueryPlanStep::getSortDescription() const
{
    static SortDescription empty;
    return empty;
}

static void doDescribeHeader(const Block & header, size_t count, IQueryPlanStep::FormatSettings & settings)
{
    String prefix(settings.offset, settings.indent_char);
    prefix += "Header";

    if (count > 1)
        prefix += " × " + std::to_string(count) + " ";

    prefix += ": ";

    settings.out << prefix;

    if (!header)
    {
        settings.out << " empty\n";
        return;
    }

    prefix.assign(prefix.size(), settings.indent_char);
    bool first = true;

    for (const auto & elem : header)
    {
        if (!first)
            settings.out << prefix;

        first = false;
        elem.dumpNameAndType(settings.out);
        settings.out << ": ";
        elem.dumpStructure(settings.out);
        settings.out << '\n';
    }
}

static void doDescribeProcessor(const IProcessor & processor, size_t count, IQueryPlanStep::FormatSettings & settings)
{
    settings.out << String(settings.offset, settings.indent_char) << processor.getName();
    if (count > 1)
        settings.out << " × " << std::to_string(count);

    size_t num_inputs = processor.getInputs().size();
    size_t num_outputs = processor.getOutputs().size();
    if (num_inputs != 1 || num_outputs != 1)
        settings.out << " " << std::to_string(num_inputs) << " → " << std::to_string(num_outputs);

    settings.out << '\n';

    if (settings.write_header)
    {
        const Block * last_header = nullptr;
        size_t num_equal_headers = 0;

        for (const auto & port : processor.getOutputs())
        {
            if (last_header && !blocksHaveEqualStructure(*last_header, port.getHeader()))
            {
                doDescribeHeader(*last_header, num_equal_headers, settings);
                num_equal_headers = 0;
            }

            ++num_equal_headers;
            last_header = &port.getHeader();
        }

        if (last_header)
            doDescribeHeader(*last_header, num_equal_headers, settings);
    }

    if (!processor.getDescription().empty())
        settings.out << String(settings.offset, settings.indent_char) << "Description: " << processor.getDescription() << '\n';

    settings.offset += settings.indent;
}

void IQueryPlanStep::describePipeline(const Processors & processors, FormatSettings & settings)
{
    const IProcessor * prev = nullptr;
    size_t count = 0;

    for (auto it = processors.rbegin(); it != processors.rend(); ++it)
    {
        if (prev && prev->getName() != (*it)->getName())
        {
            doDescribeProcessor(*prev, count, settings);
            count = 0;
        }

        ++count;
        prev = it->get();
    }

    if (prev)
        doDescribeProcessor(*prev, count, settings);
}

void IQueryPlanStep::appendExtraProcessors(const Processors & extra_processors)
{
    processors.insert(processors.end(), extra_processors.begin(), extra_processors.end());
}

void IQueryPlanStep::serialize(Serialization & /*ctx*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serialize is not implemented for {}", getName());
}

void IQueryPlanStep::updateOutputHeader() { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented"); }

}
