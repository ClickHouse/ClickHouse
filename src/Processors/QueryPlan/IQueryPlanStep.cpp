#include <Common/CurrentThread.h>
#include <Common/UTF8Helpers.h>
#include <Common/ThreadStatus.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <fmt/format.h>
#include <algorithm>
#include <unordered_map>

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

void IQueryPlanStep::updateInputHeaders(SharedHeaders input_headers_)
{
    input_headers = std::move(input_headers_);
    updateOutputHeader();
}

void IQueryPlanStep::updateInputHeader(SharedHeader input_header, size_t idx)
{
    if (idx >= input_headers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot update input header {} for step {} because it has only {} headers",
            idx, getName(), input_headers.size());

    input_headers[idx] = input_header;
    updateOutputHeader();
}

void IQueryPlanStep::setRuntimeDataflowStatisticsCacheUpdater(RuntimeDataflowStatisticsCacheUpdaterPtr updater)
{
    if (!supportsDataflowStatisticsCollection())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Step {} doesn't support dataflow statistics collection", getName());
    dataflow_cache_updater = std::move(updater);
}

IQueryPlanStep::RemoveUnusedColumnsResult IQueryPlanStep::removeUnusedColumns(const std::vector<size_t> & /*required_output_positions*/, bool /*remove_inputs*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "removeUnusedColumns is not implemented for step {}", getName());
}

bool IQueryPlanStep::canRemoveColumnsFromOutput() const
{
    return false;
}

bool IQueryPlanStep::hasCorrelatedExpressions() const
{
    return false;
}

const SharedHeader & IQueryPlanStep::getOutputHeader() const
{
    if (!hasOutputHeader())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPlanStep {} does not have output stream.", getName());

    return output_header;
}

std::string_view IQueryPlanStep::getStepDescription() const
{
    if (std::holds_alternative<std::string_view>(step_description))
        return std::get<std::string_view>(step_description);
    if (std::holds_alternative<std::string>(step_description))
        return std::get<std::string>(step_description);

    return {};
}

void IQueryPlanStep::setStepDescription(std::string description, size_t limit)
{
    if (description.size() > limit)
    {
        description.resize(limit);
        description.shrink_to_fit();
    }

    step_description = std::move(description);
}

void IQueryPlanStep::setStepDescription(const IQueryPlanStep & step)
{
    step_description = step.step_description;
}

QueryPlanStepPtr IQueryPlanStep::clone() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot clone {} plan step", getName());
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

    if (header.empty())
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

static String getProcessorDescriptionLine(const IProcessor & processor, size_t count, size_t offset, char indent_char)
{
    String line(offset, indent_char);
    line += processor.getName();

    if (count > 1)
    {
        line += " × ";
        line += std::to_string(count);
    }

    size_t num_inputs = processor.getInputs().size();
    size_t num_outputs = processor.getOutputs().size();
    if (num_inputs != 1 || num_outputs != 1)
    {
        line += " ";
        line += std::to_string(num_inputs);
        line += " → ";
        line += std::to_string(num_outputs);
    }

    return line;
}

static void doDescribeProcessorDetails(const IProcessor & processor, IQueryPlanStep::FormatSettings & settings)
{
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
}

static void doDescribeProcessor(const IProcessor & processor, size_t count, IQueryPlanStep::FormatSettings & settings)
{
    settings.out << getProcessorDescriptionLine(processor, count, settings.offset, settings.indent_char) << '\n';

    doDescribeProcessorDetails(processor, settings);

    settings.offset += settings.base_indent;
}

static size_t getLineWidth(const String & line)
{
    return UTF8::computeWidth(reinterpret_cast<const UInt8 *>(line.data()), line.size());
}

static bool haveProcessorsEqualOutputHeadersForExplain(const IProcessor & lhs, const IProcessor & rhs)
{
    const auto & lhs_outputs = lhs.getOutputs();
    const auto & rhs_outputs = rhs.getOutputs();

    if (lhs_outputs.size() != rhs_outputs.size())
        return false;

    auto rhs_output = rhs_outputs.begin();
    for (const auto & lhs_output : lhs_outputs)
    {
        if (!blocksHaveEqualStructure(lhs_output.getHeader(), rhs_output->getHeader()))
            return false;

        ++rhs_output;
    }

    return true;
}

static bool areProcessorsSimilarForExplain(const IProcessor & lhs, const IProcessor & rhs, bool compare_headers)
{
    return lhs.getName() == rhs.getName()
        && lhs.getInputs().size() == rhs.getInputs().size()
        && lhs.getOutputs().size() == rhs.getOutputs().size()
        && lhs.getDescription() == rhs.getDescription()
        && (!compare_headers || haveProcessorsEqualOutputHeadersForExplain(lhs, rhs));
}

static bool areProcessorChainsSimilar(
    const std::vector<const IProcessor *> & processors,
    size_t first_chain_begin,
    size_t second_chain_begin,
    size_t chain_size,
    bool compare_headers)
{
    for (size_t processor_offset = 0; processor_offset < chain_size; ++processor_offset)
        if (!areProcessorsSimilarForExplain(
                *processors[first_chain_begin + processor_offset],
                *processors[second_chain_begin + processor_offset],
                compare_headers))
            return false;

    return true;
}

static bool hasConnectionTo(const IProcessor & from, const IProcessor & to)
{
    for (const auto & output : from.getOutputs())
        if (output.isConnected() && &output.getInputPort().getProcessor() == &to)
            return true;

    return false;
}

static bool isProcessorChain(const std::vector<const IProcessor *> & processors, size_t chain_begin, size_t chain_size)
{
    for (size_t processor_offset = 0; processor_offset + 1 < chain_size; ++processor_offset)
    {
        const auto & consumer = *processors[chain_begin + processor_offset];
        const auto & producer = *processors[chain_begin + processor_offset + 1];
        if (!hasConnectionTo(producer, consumer))
            return false;
    }

    return true;
}

using ProcessorChainIndexes = std::unordered_map<const IProcessor *, size_t>;

static ProcessorChainIndexes collectProcessorChainIndexes(
    const std::vector<const IProcessor *> & processors,
    size_t repeated_chains_begin,
    size_t chain_size,
    size_t chain_count)
{
    ProcessorChainIndexes processor_chain_indexes;
    processor_chain_indexes.reserve(chain_size * chain_count);

    for (size_t chain_index = 0; chain_index < chain_count; ++chain_index)
    {
        const size_t chain_begin = repeated_chains_begin + chain_index * chain_size;
        for (size_t processor_offset = 0; processor_offset < chain_size; ++processor_offset)
            processor_chain_indexes.emplace(processors[chain_begin + processor_offset], chain_index);
    }

    return processor_chain_indexes;
}

static bool hasConnectionToDifferentProcessorChain(
    const IProcessor & processor,
    size_t source_chain_index,
    const ProcessorChainIndexes & processor_chain_indexes)
{
    for (const auto & output : processor.getOutputs())
    {
        if (!output.isConnected())
            continue;

        const auto * const target_processor = &output.getInputPort().getProcessor();
        auto target_chain = processor_chain_indexes.find(target_processor);
        if (target_chain != processor_chain_indexes.end() && target_chain->second != source_chain_index)
            return true;
    }

    return false;
}

static bool hasConnectionBetweenRepeatedProcessorChains(
    const std::vector<const IProcessor *> & processors,
    size_t repeated_chains_begin,
    size_t chain_size,
    size_t chain_count)
{
    const ProcessorChainIndexes processor_chain_indexes = collectProcessorChainIndexes(processors, repeated_chains_begin, chain_size, chain_count);

    for (size_t chain_index = 0; chain_index < chain_count; ++chain_index)
    {
        const size_t chain_begin = repeated_chains_begin + chain_index * chain_size;
        for (size_t processor_offset = 0; processor_offset < chain_size; ++processor_offset)
        {
            if (hasConnectionToDifferentProcessorChain(*processors[chain_begin + processor_offset], chain_index, processor_chain_indexes))
                return true;
        }
    }

    return false;
}

static bool isRepeatedProcessorChainIndependent(
    const std::vector<const IProcessor *> & processors,
    size_t repeated_chains_begin,
    size_t chain_size,
    size_t chain_count)
{
    for (size_t chain_index = 0; chain_index < chain_count; ++chain_index)
    {
        const size_t chain_begin = repeated_chains_begin + chain_index * chain_size;
        if (!isProcessorChain(processors, chain_begin, chain_size))
            return false;
    }

    return !hasConnectionBetweenRepeatedProcessorChains(processors, repeated_chains_begin, chain_size, chain_count);
}

static std::pair<size_t, size_t> findRepeatedProcessorChainRange(
    const std::vector<const IProcessor *> & processors,
    size_t begin,
    bool compare_headers)
{
    static constexpr size_t min_period = 2;
    static constexpr size_t max_period = 8;

    const size_t remaining = processors.size() - begin;
    const size_t max_candidate_period = std::min(max_period, remaining / 2);

    for (size_t period = min_period; period <= max_candidate_period; ++period)
    {
        size_t count = 1;
        while (begin + (count + 1) * period <= processors.size()
            && areProcessorChainsSimilar(processors, begin, begin + count * period, period, compare_headers))
        {
            ++count;
        }

        if (count > 1 && isRepeatedProcessorChainIndependent(processors, begin, period, count))
            return {period, count};
    }

    return {0, 0};
}

static void doDescribeRepeatedProcessorChains(
    const std::vector<const IProcessor *> & processors,
    size_t begin,
    size_t length,
    size_t count,
    IQueryPlanStep::FormatSettings & settings)
{
    static constexpr size_t marker_padding = 4;

    std::vector<String> processor_lines;
    processor_lines.reserve(length);
    size_t max_line_width = 0;

    for (size_t i = 0; i < length; ++i)
    {
        processor_lines.emplace_back(
            getProcessorDescriptionLine(*processors[begin + i], 1, settings.offset + i * settings.base_indent, settings.indent_char));
        max_line_width = std::max(max_line_width, getLineWidth(processor_lines.back()));
    }

    for (size_t i = 0; i < length; ++i)
    {
        settings.offset += settings.base_indent * i;

        settings.out << processor_lines[i];
        settings.out << String(max_line_width - getLineWidth(processor_lines[i]) + marker_padding, ' ') << "│";
        if (i + 1 == length)
            settings.out << " × " << count;
        settings.out << '\n';

        doDescribeProcessorDetails(*processors[begin + i], settings);

        settings.offset -= settings.base_indent * i;
    }

    settings.offset += settings.base_indent * length;
}

static void doDescribePipeline(const Processors & processors, IQueryPlanStep::FormatSettings & settings)
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

static void doDescribePipelineWithRepeatedProcessorChainCompaction(
    const Processors & processors,
    IQueryPlanStep::FormatSettings & settings)
{
    /// Scan processors in display order.
    std::vector<const IProcessor *> ordered_processors;
    ordered_processors.reserve(processors.size());
    for (auto it = processors.rbegin(); it != processors.rend(); ++it)
        ordered_processors.emplace_back(it->get());

    size_t position = 0;
    while (position < ordered_processors.size())
    {
        /// Compact adjacent similar processors first.
        size_t equal_processors_count = 1;
        while (position + equal_processors_count < ordered_processors.size()
            && areProcessorsSimilarForExplain(*ordered_processors[position], *ordered_processors[position + equal_processors_count], settings.write_header))
        {
            ++equal_processors_count;
        }

        if (equal_processors_count > 1)
        {
            doDescribeProcessor(*ordered_processors[position], equal_processors_count, settings);
            position += equal_processors_count;
            continue;
        }

        /// Compact repeated processor chains when adjacent processors are not similar.
        const auto [repeated_period, repeated_count] = findRepeatedProcessorChainRange(ordered_processors, position, settings.write_header);
        if (repeated_count > 1)
        {
            doDescribeRepeatedProcessorChains(ordered_processors, position, repeated_period, repeated_count, settings);
            position += repeated_period * repeated_count;
            continue;
        }

        /// Describe a single processor when no compaction applies.
        doDescribeProcessor(*ordered_processors[position], 1, settings);
        ++position;
    }
}

void IQueryPlanStep::describePipeline(const Processors & processors, FormatSettings & settings)
{
    if (settings.compact_repeated_processor_chains)
        doDescribePipelineWithRepeatedProcessorChainCompaction(processors, settings);
    else
        doDescribePipeline(processors, settings);
}

void IQueryPlanStep::appendExtraProcessors(const Processors & extra_processors)
{
    processors.insert(processors.end(), extra_processors.begin(), extra_processors.end());
}

String IQueryPlanStep::getUniqID() const
{
    return fmt::format("{}_{}", getName(), step_index);
}

void IQueryPlanStep::serialize(Serialization & /*ctx*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serialize is not implemented for {}", getName());
}

void IQueryPlanStep::updateOutputHeader() { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented"); }

}
