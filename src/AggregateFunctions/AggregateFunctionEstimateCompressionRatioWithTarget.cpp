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

/// The most bytes of pending rows; bounds the memory of a state. A wider granule is written in pieces, and what a serialization
/// writes per call (a `LowCardinality` header, a sparse granule end, the compact `Variant` mode) is written once per piece.
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
    [[nodiscard]] ColumnPtr convert(const ColumnPtr & rows) const
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
    /// In the source type, of the rows written and of the merged states.
    UInt64 source_bytes = 0;
    /// Of the finished parts and of the merged states.
    UInt64 compressed_bytes = 0;
    /// In the source type. All of them convert with `conversion`.
    MutableColumnPtr pending;
    /// The rows that `pending` takes before it completes the open granule, read at every row. Set with `pending`: the part
    /// does not change while it lives.
    size_t pending_granule_rows = 0;
    ConversionPtr conversion;
    std::unique_ptr<CompressionEstimatePartWriter> part;
    /// A state kept in a `Memory` table is read by later queries, maybe at once, maybe after a read that failed half-way.
    bool broken = false;
    std::mutex mutex;
};

/// `estimateCompressionRatio` with a `target_type`: the bytes of the rows in their own type, divided by their compressed bytes
/// as `target_type` in a part of a new `MergeTree` table. Rows wait in their own type until they fill a granule or
/// `PENDING_BYTES`. Reading the state finishes its part.
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

    /// Registered as a window function, so the factory adds no `Null` combinator and NULLs reach the function. Returning itself
    /// stops `RewriteAggregateFunctionWithIfPass` from turning `f(if(c, NULL, x))` into `fIf(x, not c)`, which drops them.
    /// That pass gives default properties, so the `chassert` checks the registration.
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

    /// The default adds the default rows after the values, and row order matters to compression.
    void addBatchSparseSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *) const override
    {
        auto rows = columns[0]->cut(row_begin, row_end - row_begin)->convertToFullColumnIfSparse();
        addRows(data(place), *rows, 0, rows->size(), conversion);
    }

    /// The rows of a state without a part (often another thread's share of a group) join this state's part; a state with a
    /// part is finished as a separate part. `rhs` is not changed otherwise, so a `Memory` table state can be read again.
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

    /// Leaves a state without a part as it is, so that writing it out does not change how it merges later.
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
    /// For rows that convert with `rows_conversion`. Pending rows of another conversion are written first.
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
            /// The first row would allocate 4 KiB otherwise.
            data_ref.pending->reserve(1);
            data_ref.pending_granule_rows = data_ref.part ? data_ref.part->rowsLeftInGranule() : layout.granuleRows();
        }
        return *data_ref.pending;
    }

    /// The part finds where a granule ends by bytes.
    bool isPendingFull(const Data & data_ref) const
    {
        return data_ref.pending->size() >= data_ref.pending_granule_rows || data_ref.pending->byteSize() >= PENDING_BYTES;
    }

    /// As `add` row by row, so that the result does not depend on the blocks. Ranges are sized from the average row; a range
    /// that overfills `PENDING_BYTES` is taken back and added row by row.
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

    /// The serialization prefix and suffix count once per part, so the numerator does not depend on where granules end. Source
    /// types that write per call are removed by the factory, made dense, or rejected at creation.
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

    /// Reading the state again then gives the same totals.
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

    /// With `keep_pending_rows`, a state without a part is finished in a copy.
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
