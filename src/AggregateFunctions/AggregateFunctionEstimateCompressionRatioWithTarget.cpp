#include <AggregateFunctions/AggregateFunctionEstimateCompressionRatioWithTarget.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CompressionEstimatePartWriter.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/TypeTree.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <memory>
#include <mutex>
#include <optional>


namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Not a `MergeTree` setting: the most bytes of rows a state keeps before it writes them, which bounds the memory per group. A
/// granule wider than this is written in pieces. What a serialization writes once per call is then written once per piece,
/// where the writer writes it once per granule: the header of a `LowCardinality` column (16 bytes), the end of a granule of a
/// sparse column, the mode of compact `Variant` discriminators.
constexpr size_t PENDING_BYTES = DBMS_DEFAULT_BUFFER_SIZE;

/// Converts rows as `ALTER TABLE ... MODIFY COLUMN` does, with `_CAST` and the settings of the query that creates it (of the
/// server without a query). A state keeps the conversion of its pending rows, for a later query that reads it.
class Conversion
{
public:
    Conversion(const DataTypePtr & source_type, DataTypePtr target_type_, std::optional<DataTypeValidationSettings> validation_settings_)
        : target_type(std::move(target_type_))
        , validation_settings(std::move(validation_settings_))
    {
        /// By name: `Bool` equals `UInt8`, but `CAST` turns every value that is not 0 into 1.
        if (source_type->getName() == target_type->getName())
            return;
        ContextPtr context = CurrentThread::tryGetQueryContext();
        if (!context)
            context = Context::getGlobalContextInstance();
        ColumnsWithTypeAndName arguments{
            {source_type->createColumn(), source_type, ""},
            {DataTypeString().createColumnConst(1, target_type->getName()), std::make_shared<DataTypeString>(), ""}};
        cast = FunctionFactory::instance().get("_CAST", context)->build(arguments);
        prepared_cast = cast->prepare({});
    }

    /// Validates the target type at every call, since the rows of several queries can go into one part.
    ColumnPtr convert(const ColumnPtr & rows) const
    {
        if (validation_settings)
            validateDataType(target_type, *validation_settings);
        if (!cast)
            return rows;
        /// With the cast's own types: a `DateTime` without a time zone takes the session's, which a later query may change.
        ColumnsWithTypeAndName arguments{
            {rows, cast->getArgumentTypes()[0], ""},
            {DataTypeString().createColumnConst(rows->size(), target_type->getName()), std::make_shared<DataTypeString>(), ""}};
        return prepared_cast->execute(arguments, cast->getResultType(), rows->size(), /*dry_run=*/false);
    }

private:
    DataTypePtr target_type;
    std::optional<DataTypeValidationSettings> validation_settings;
    FunctionBasePtr cast;
    /// Can run in several threads at once.
    ExecutableFunctionPtr prepared_cast;
};
using ConversionPtr = std::shared_ptr<const Conversion>;

struct EstimateCompressionRatioWithTargetData
{
    /// The bytes of the rows in their own type, of the rows written and of the merged states.
    UInt64 source_bytes = 0;
    /// The compressed bytes in the target type, of the finished parts and of the merged states.
    UInt64 compressed_bytes = 0;
    /// The rows not written yet, in their own type, and how to convert them. All pending rows share one conversion.
    MutableColumnPtr pending;
    /// The rows that `pending` takes before it completes the open granule, read at every row. Set with `pending`: the part
    /// does not change while it lives.
    size_t pending_granule_rows = 0;
    ConversionPtr conversion;
    /// The part the rows are written to, until the state is read.
    std::unique_ptr<CompressionEstimatePartWriter> part;
    /// A state kept in a `Memory` table outlives its query and is read by later queries: maybe several at once (`mutex`: a
    /// read changes the state, as it finishes the part), maybe after a read that failed half-way (`broken`: set while the
    /// part changes, it stays set if that fails, and the state cannot be read then).
    bool broken = false;
    std::mutex mutex;
};

/// `estimateCompressionRatio` with a `target_type`: the bytes of the rows in their own type, divided by the bytes they would
/// take compressed as a column of `target_type` in a wide part of a new `MergeTree` table. The numerator depends only on the
/// rows, so the ratios of several targets for the same rows rank the targets.
///
/// A state keeps the rows it gets as pending rows, in their own type. When they fill a granule (`index_granularity` rows) or
/// `PENDING_BYTES`, they are converted to the target type and written to the part of the state: a `CompressedSizeCalculator`
/// per stream and one serialization state, as the writer has them. A granule wider than `PENDING_BYTES` is written in
/// pieces. Reading the state (its result, its serialization, a merge of a state that has opened a part) finishes the part,
/// and the next rows start a new one.
class AggregateFunctionEstimateCompressionRatioWithTarget final
    : public IAggregateFunctionDataHelper<EstimateCompressionRatioWithTargetData, AggregateFunctionEstimateCompressionRatioWithTarget>
{
public:
    AggregateFunctionEstimateCompressionRatioWithTarget(
        const DataTypes & arguments,
        const Array & params,
        DataTypePtr target_type,
        ASTPtr codec_description,
        std::optional<UInt64> block_size_bytes,
        std::optional<DataTypeValidationSettings> validation_settings)
        : IAggregateFunctionDataHelper(arguments, params, std::make_shared<DataTypeFloat64>())
        , source_type(arguments[0])
        , source_serialization(source_type->getDefaultSerialization())
        , conversion(std::make_shared<const Conversion>(source_type, target_type, std::move(validation_settings)))
        , layout(std::move(target_type), std::move(codec_description), block_size_bytes)
    {
    }

    String getName() const override { return "estimateCompressionRatio"; }

    bool allocatesMemoryInArena() const override { return false; }

    /// The function is registered as a window function, so the factory never wraps it in the `Null` combinator, and its rows
    /// keep their NULLs (measured as a null map, or an error for a type that cannot hold NULL). Returning itself as its own
    /// `Null` adapter tells `RewriteAggregateFunctionWithIfPass` not to turn `f(if(c, NULL, x))` into `fIf(x, not c)`, which
    /// would drop them. The `chassert` checks the registration, not the properties argument: the rewrite pass passes default
    /// ones.
    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes &, const Array &, const AggregateFunctionProperties &) const override
    {
        chassert(AggregateFunctionFactory::instance().tryGetProperties(getName(), NullsAction::EMPTY).value().is_window_function);
        return nested_function;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & data_ref = data(place);
        pendingOf(data_ref, conversion).insertFrom(*columns[0], row_num);
        if (isPendingFull(data_ref))
            writePending(data_ref);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos < 0)
        {
            addRows(data(place), *columns[0], row_begin, row_end, conversion);
            return;
        }

        const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
        IColumn::Filter filter;
        filter.assign(flags.begin() + row_begin, flags.begin() + row_end);
        auto rows = columns[0]->cut(row_begin, row_end - row_begin)->filter(filter, -1);
        addRows(data(place), *rows, 0, rows->size(), conversion);
    }

    /// The default adds the values first and the default rows after them, and the order of the rows matters to compression.
    void addBatchSparseSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *) const override
    {
        auto rows = columns[0]->cut(row_begin, row_end - row_begin)->convertToFullColumnIfSparse();
        addRows(data(place), *rows, 0, rows->size(), conversion);
    }

    /// The other state is often another thread's share of the same group. If it has not opened a part, its rows are added to
    /// this state, so that they are written to the same part: the block headers and dictionaries of a part of its own would
    /// outweigh a few hundred rows. A state that has opened a part has compressed its rows already, so it is finished as a
    /// separate part, and the result of a larger group still depends on the threads. The rows passed on keep their
    /// conversion. The other state is not changed otherwise, so a state kept in a `Memory` table can be read again.
    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & rhs_ref = data(const_cast<AggregateDataPtr>(rhs));
        std::lock_guard lock(rhs_ref.mutex);
        throwIfBroken(rhs_ref);
        if (rhs_ref.part)
            finish(rhs_ref);

        auto & data_ref = data(place);
        data_ref.source_bytes += rhs_ref.source_bytes;
        data_ref.compressed_bytes += rhs_ref.compressed_bytes;
        if (rhs_ref.pending)
            addRows(data_ref, *rhs_ref.pending, 0, rhs_ref.pending->size(), rhs_ref.conversion);
    }

    /// A state that has not opened a part keeps its rows, so that writing it out (a `-State` column, `hex`, an output format)
    /// does not change how it merges afterwards.
    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        auto [source_bytes, compressed_bytes] = read(place, /*keep_pending_rows=*/true);
        writeVarUInt(source_bytes, buf);
        writeVarUInt(compressed_bytes, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        readVarUInt(data(place).source_bytes, buf);
        readVarUInt(data(place).compressed_bytes, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto [source_bytes, compressed_bytes] = read(place);
        Float64 ratio = 0;
        if (compressed_bytes > 0)
            ratio = static_cast<Float64>(source_bytes) / static_cast<Float64>(compressed_bytes);
        assert_cast<ColumnFloat64 &>(to).getData().push_back(ratio);
    }

private:
    /// The pending rows, to add rows that convert with `rows_conversion`. Pending rows of another conversion are written first,
    /// so that all pending rows share one (a state merged with a state added by another query).
    IColumn & pendingOf(Data & data_ref, const ConversionPtr & rows_conversion) const
    {
        if (data_ref.conversion != rows_conversion)
        {
            if (data_ref.pending && !data_ref.pending->empty())
                writePending(data_ref);
            data_ref.conversion = rows_conversion;
        }
        if (!data_ref.pending)
        {
            data_ref.pending = source_type->createColumn();
            /// Otherwise the first row allocates 4 KiB, which a group of a few rows does not need.
            data_ref.pending->reserve(1);
            data_ref.pending_granule_rows = data_ref.part ? data_ref.part->rowsLeftInGranule() : layout.granuleRows();
        }
        return *data_ref.pending;
    }

    /// Whether the pending rows complete the open granule by rows, or reach `PENDING_BYTES`. Where a granule ends by bytes
    /// depends on the converted rows, and the part finds it.
    bool isPendingFull(const Data & data_ref) const
    {
        return data_ref.pending->size() >= data_ref.pending_granule_rows || data_ref.pending->byteSize() >= PENDING_BYTES;
    }

    /// Adds the rows as `add` adds them one by one: the pending rows are written right after the row that fills them, so the
    /// result does not depend on the blocks the rows come in. The rows go in as ranges that fit the room left, guessed from
    /// their average size; a range that overfills the pending rows by bytes is taken back and added row by row.
    void addRows(Data & data_ref, const IColumn & column, size_t row_begin, size_t row_end, const ConversionPtr & rows_conversion) const
    {
        if (row_begin == row_end)
            return;

        const size_t bytes_per_row = std::max<size_t>(1, column.byteSize() / column.size());
        while (row_begin < row_end)
        {
            auto & pending = pendingOf(data_ref, rows_conversion);
            const size_t pending_bytes = pending.byteSize();
            const size_t room_rows = data_ref.pending_granule_rows - std::min(data_ref.pending_granule_rows, pending.size());
            const size_t room_bytes = PENDING_BYTES - std::min(PENDING_BYTES, pending_bytes);
            const size_t rows = std::max<size_t>(1, std::min({row_end - row_begin, room_rows, room_bytes / bytes_per_row}));
            pending.insertRangeFrom(column, row_begin, rows);
            if (rows > 1 && pending.byteSize() >= PENDING_BYTES)
            {
                /// At least one row goes in, so the loop progresses even if `popBack` does not give all bytes back.
                pending.popBack(rows);
                do
                    pending.insertFrom(column, row_begin++);
                while (row_begin < row_end && !isPendingFull(data_ref));
            }
            else
            {
                row_begin += rows;
            }
            if (isPendingFull(data_ref))
                writePending(data_ref);
        }
    }

    /// Converts the pending rows and writes them to the part.
    void writePending(Data & data_ref) const
    {
        throwIfBroken(data_ref);
        chassert(data_ref.conversion);
        ColumnPtr rows = data_ref.pending->getPtr();
        ColumnPtr converted = data_ref.conversion->convert(rows);
        const UInt64 rows_bytes = sourceBytes(*rows, /*first_in_part=*/!data_ref.part);

        data_ref.broken = true;
        if (!data_ref.part)
            data_ref.part = std::make_unique<CompressionEstimatePartWriter>(layout, *converted);
        data_ref.part->write(*converted);
        data_ref.source_bytes += rows_bytes;
        data_ref.pending.reset();
        data_ref.broken = false;
    }

    /// The numerator of the rows: their bytes serialized in their own type, as the function without a target type counts them.
    /// The prefix and suffix of the serialization (for example, the mode of `Variant` discriminators) are counted once per part,
    /// with its first rows, so that the numerator does not depend on where the granules of the target end. The rows themselves
    /// write nothing per call: the source types that would (`LowCardinality`, sparse, `Dynamic`) are removed by the factory,
    /// made dense in `addBatchSparseSinglePlace`, or rejected in the constructor.
    UInt64 sourceBytes(const IColumn & rows, bool first_in_part) const
    {
        NullWriteBufferWithMemory out(16384);
        ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [&out](const ISerialization::SubstreamPath &) -> WriteBuffer * { return &out; };
        ISerialization::SerializeBinaryBulkStatePtr state;
        source_serialization->serializeBinaryBulkStatePrefix(rows, settings, state);
        const size_t prefix_end = out.count();
        source_serialization->serializeBinaryBulkWithMultipleStreams(rows, 0, rows.size(), settings, state);
        const size_t rows_end = out.count();
        source_serialization->serializeBinaryBulkStateSuffix(settings, state);
        out.finalize();
        return first_in_part ? out.count() : rows_end - prefix_end;
    }

    /// Writes the pending rows and finishes the part. Reading the state again then gives the same totals.
    void finish(Data & data_ref) const
    {
        throwIfBroken(data_ref);
        if (data_ref.pending && !data_ref.pending->empty())
            writePending(data_ref);
        if (!data_ref.part)
            return;

        data_ref.broken = true;
        data_ref.compressed_bytes += data_ref.part->finish();
        data_ref.part.reset();
        data_ref.broken = false;
    }

    /// The totals of the state, after its part is finished. With `keep_pending_rows`, a state that has not opened a part is
    /// left as it is, and its pending rows are finished in a copy.
    std::pair<UInt64, UInt64> read(ConstAggregateDataPtr __restrict place, bool keep_pending_rows = false) const
    {
        auto & data_ref = data(const_cast<AggregateDataPtr>(place));
        std::lock_guard lock(data_ref.mutex);
        if (keep_pending_rows && !data_ref.part && data_ref.pending && !data_ref.pending->empty())
        {
            throwIfBroken(data_ref);
            Data copy;
            copy.source_bytes = data_ref.source_bytes;
            copy.compressed_bytes = data_ref.compressed_bytes;
            copy.pending = data_ref.pending->cloneResized(data_ref.pending->size());
            copy.pending_granule_rows = data_ref.pending_granule_rows;
            copy.conversion = data_ref.conversion;
            finish(copy);
            return {copy.source_bytes, copy.compressed_bytes};
        }
        finish(data_ref);
        return {data_ref.source_bytes, data_ref.compressed_bytes};
    }

    void throwIfBroken(const Data & data_ref) const
    {
        if (data_ref.broken)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "The state of aggregate function {} is incomplete after an earlier error", getName());
    }

    const DataTypePtr source_type;
    const SerializationPtr source_serialization;
    /// Given to the states with the rows this function adds.
    const ConversionPtr conversion;
    const CompressionEstimatePartWriter::Layout layout;
};

}

AggregateFunctionPtr createAggregateFunctionEstimateCompressionRatioWithTarget(
    const String & name,
    const DataTypes & arguments,
    const Array & parameters,
    DataTypePtr target_type,
    const std::optional<String> & codec,
    std::optional<UInt64> block_size_bytes,
    const Settings * settings)
{
    /// As a target, `Dynamic` and `JSON` have streams that depend on the data. As a source, pending rows keep the dynamic
    /// structure of their blocks, and the numerator would depend on the blocks.
    const auto has_dynamic_structure = [](const IDataType & type) { return isDynamic(type) || isObject(type); };
    if (anyInTypeTree(*target_type, has_dynamic_structure))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} does not support target type {}", name, target_type->getName());
    /// `LowCardinality` reaches here only inside `Variant`. It writes a dictionary per call, so the numerator would depend on
    /// where granules end.
    const auto & source_type = arguments[0];
    if (anyInTypeTree(*source_type, [&](const IDataType & type) { return has_dynamic_structure(type) || type.lowCardinality(); }))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Aggregate function {} with a target type does not support argument type {}; convert the argument in the query first",
            name, source_type->getName());

    ASTPtr codec_description = codec ? parseEstimateCompressionRatioCodec(*codec)
                                     : CompressionCodecFactory::instance().getDefaultCodec()->getFullCodecDescription();
    std::optional<DataTypeValidationSettings> validation_settings;
    if (settings)
        validation_settings.emplace(*settings);
    return std::make_shared<AggregateFunctionEstimateCompressionRatioWithTarget>(
        arguments, parameters, std::move(target_type), std::move(codec_description), block_size_bytes, std::move(validation_settings));
}

}
