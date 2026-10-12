#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>

#include <unordered_map>


namespace DB
{

struct Settings;

/** Implements `aggregate_function_input_format = 'value'` and `'array'` uniformly for every input format.
  *
  * The underlying format is asked to parse a different header: every `AggregateFunction(f, T...)` in it is replaced
  * with the type of the values that `f` aggregates - `T` (or `Tuple(T...)` when there are several arguments), or
  * `Array` of it in the `array` mode. The format reads these values exactly as it reads a column of that type,
  * so the representation is whatever is native for the format: a JSON number or array, a CSV field, a RowBinary
  * or a Parquet value, and so on. This processor then builds the aggregate function states from the values
  * and outputs chunks of the original header. The replacement is applied recursively inside `Array`, `Tuple` and `Map`.
  *
  * The states are built from fresh values, so they must be aggregated under the current query settings, even though
  * the function stored in a declared `AggregateFunction(...)` type is resolved independently of them. For a function
  * over a `Variant` the function is resolved again with `settings` (still as a declared state type, so the
  * admissibility checks such as `allow_lossy_numeric_supertype` stay unaffected), which makes the NULL rows of the
  * `Variant` follow `aggregate_functions_skip_variant_nulls`. The resolved function has the same state layout.
  *
  * Everything that callers may do with an input format is forwarded to the underlying one.
  */
class AggregateFunctionStatesFromValuesInputFormat final : public IInputFormat
{
public:
    using Mode = FormatSettings::AggregateFunctionInputFormat;

    /// The header the underlying format has to parse instead of `header`,
    /// or nullopt if `header` has no `AggregateFunction` columns and the underlying format can be used as is.
    static std::optional<Block> getHeaderToParse(const Block & header, Mode mode);

    AggregateFunctionStatesFromValuesInputFormat(
        SharedHeader header_, ReadBuffer * in_, InputFormatPtr underlying_, Mode mode_, const Settings & settings);

    String getName() const override { return "AggregateFunctionStatesFromValuesInputFormat"; }

    Chunk read() override;

    void resetParser() override;
    void setReadBuffer(ReadBuffer & in_) override;
    void resetReadBuffer() override;
    void setBucketsToRead(const FileBucketInfoPtr & buckets_to_read) override { underlying->setBucketsToRead(buckets_to_read); }
    const BlockMissingValues * getMissingValues() const override { return underlying->getMissingValues(); }
    void setRowsReadBefore(size_t rows) override { underlying->setRowsReadBefore(rows); }
    void setSerializationHints(const SerializationInfoByName & hints) override { underlying->setSerializationHints(hints); }
    size_t getApproxBytesReadForChunk() const override { return underlying->getApproxBytesReadForChunk(); }
    void needOnlyCount() override { underlying->needOnlyCount(); }
    void setQueryParameters(const NameToNameMap & parameters) override { underlying->setQueryParameters(parameters); }
    std::optional<std::pair<std::vector<size_t>, size_t>> getMatchedBuckets() const override { return underlying->getMatchedBuckets(); }
    std::vector<std::pair<size_t, Field>> getTopKBestValuesOfBuckets() const override { return underlying->getTopKBestValuesOfBuckets(); }
    bool isTopKFilterApplied() const override { return underlying->isTopKFilterApplied(); }
    bool isMinMaxFromStatisticsApplied() const override { return underlying->isMinMaxFromStatisticsApplied(); }

protected:
    /// The underlying format already annotates the exceptions with the file name, no need to do it twice.
    Chunk generate() override { return read(); }
    void onCancel() noexcept override { underlying->cancel(); }

private:
    InputFormatPtr underlying;
    /// Connected to the output port of the underlying format, which is driven by this processor and is not a part of the pipeline.
    InputPort port;
    Mode mode;
    /// The functions to aggregate the values with, for the `AggregateFunction` types of the header (including
    /// the nested ones) whose function had to be resolved again with the query settings. Keyed by the type object,
    /// which the header keeps alive. A type that is absent here uses its own function.
    std::unordered_map<const IDataType *, AggregateFunctionPtr> functions_for_values;
};

}
