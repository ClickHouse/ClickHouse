#include <algorithm>
#include <memory>
#include <optional>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CompressedColumnSizeEstimator.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Poco/String.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int BAD_QUERY_PARAMETER;
extern const int UNKNOWN_QUERY_PARAMETER;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Whether the bytes of a column of `type` do not depend on how its rows are split into writes, because every stream holds one
/// value after another. Others write something with every write or every granule: `LowCardinality` a header, the shared data of
/// `JSON` its paths, and `Dynamic` and `JSON` hold any type.
bool isSplitIndependent(const IDataType & type)
{
    if (type.isValueRepresentedByNumber() || isStringOrFixedString(type))
        return true;
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
        return isSplitIndependent(*nullable->getNestedType());
    if (const auto * array = typeid_cast<const DataTypeArray *>(&type))
        return isSplitIndependent(*array->getNestedType());
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
        return std::ranges::all_of(tuple->getElements(), [](const DataTypePtr & element) { return isSplitIndependent(*element); });
    if (const auto * map = typeid_cast<const DataTypeMap *>(&type))
        return isSplitIndependent(*map->getKeyType()) && isSplitIndependent(*map->getValueType());
    return false;
}

/// One run of a state, as MergeTree writes a column of one part. A sparse column after a dense one, or vice versa, starts a new
/// run; with a target type, of either of the two columns.
struct Run
{
    using Writer = CompressedColumnSizeEstimator::Writer;

    ISerialization::KindStack kind_stack;
    ISerialization::KindStack source_kind_stack;
    std::unique_ptr<Writer> writer;
    /// With a target type: the rows in the type of the column, only counted, the numerator.
    std::unique_ptr<Writer> source_writer;

    bool finished = false;
    /// The sizes of this run and of the finished runs before it since the state was last read.
    UInt64 uncompressed_bytes = 0;
    UInt64 compressed_bytes = 0;
};

struct AggregateFunctionEstimateCompressionRatioData
{
    UInt64 merged_compressed_size = 0;
    UInt64 merged_uncompressed_size = 0;

    /// Pending work: rows that do not fill a granule yet and the current run. Completed whenever the state is read, also through
    /// a const pointer (`serialize`, `merge`), which does not change what the state stands for.
    mutable MutableColumnPtr buffer;
    mutable std::unique_ptr<Run> run;
    /// The type of the column the rows of `buffer` come from. A state of a `LowCardinality(String)` column has the same type as
    /// a state of a `String` column, so the function that completes it may not be the one that produced it.
    DataTypePtr represented_type;
};

/// A conversion of the rows, prepared once. The prepared function refers to the function it is prepared from without owning
/// it, so both are kept.
struct Conversion
{
    FunctionBasePtr function;
    ExecutableFunctionPtr prepared;
};

/// The parameters of the function, and the settings of the server and of the query the writer would use.
struct EstimateCompressionRatioConfig
{
    std::optional<String> codec;
    std::optional<UInt64> block_size_bytes;
    DataTypePtr target_type;
    CompressedColumnSizeEstimator::WriterSettings writer_settings;
};

class AggregateFunctionEstimateCompressionRatio final
    : public IAggregateFunctionDataHelper<AggregateFunctionEstimateCompressionRatioData, AggregateFunctionEstimateCompressionRatio>
{
private:
    const EstimateCompressionRatioConfig config;
    /// The type of the column, `LowCardinality` included, which the function is created from (see
    /// `AggregateFunctionProperties::created_with_low_cardinality_types`), and the type its rows arrive as.
    const DataTypePtr represented_type;
    const DataTypePtr plain_type;
    /// The `target_type` parameter when it differs from the type of the column.
    const DataTypePtr target_type;
    /// The ratio is the serialized size of the rows in `represented_type` over their compressed size in `estimated_type`.
    const DataTypePtr estimated_type;
    /// The conversions of the rows from `plain_type`, where it differs.
    const std::optional<Conversion> to_represented;
    const std::optional<Conversion> to_target;
    /// A granule is written at once even when the blocks of the query split it, unless the bytes do not depend on that (see
    /// `isSplitIndependent`).
    const bool write_whole_granules;
    const CompressionCodecPtr default_codec;
    /// The `codec` parameter, or the default codec, as the `CODEC` clause of a column.
    const ASTPtr codec_description;
    /// Writes the rows as the MergeTree writer would; building it validates the codec against the estimated type.
    const CompressedColumnSizeEstimator estimator;
    /// Whether a sparse column is written as such with the server's serialization versions (`Nullable` only with
    /// `nullable_serialization_version = 'allow_sparse'`). Otherwise a table stores it dense, and so is it measured.
    const bool sparse_allowed;

    static ASTPtr parseCodecDescription(const String & codec)
    {
        ParserCodec codec_parser;
        auto ast = parseQuery(codec_parser, "(" + codec + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        /// The serialized values are compressed directly, so a declarative codec would measure nothing.
        CompressionCodecFactory::instance().checkCodecChainIsNotDeclarative(ast);
        return ast;
    }

    /// The cast to `to`; `prepare` builds the converters and so rejects a conversion that does not exist.
    static Conversion createCast(const DataTypePtr & from, const DataTypePtr & to)
    {
        ColumnsWithTypeAndName arguments{{from->createColumn(), from, ""}, {nullptr, std::make_shared<DataTypeString>(), ""}};
        Conversion conversion;
        conversion.function = createInternalCast(arguments[0], to, CastType::accurate, {}, nullptr);
        conversion.prepared = conversion.function->prepare(arguments);
        return conversion;
    }

    /// The settings of the writer, with `block_size_bytes` as both the smallest and the largest size of a compressed block.
    static CompressedColumnSizeEstimator::WriterSettings writerSettings(const EstimateCompressionRatioConfig & config)
    {
        auto settings = config.writer_settings;
        if (config.block_size_bytes)
        {
            settings.min_compress_block_size = *config.block_size_bytes;
            settings.max_compress_block_size = *config.block_size_bytes;
            settings.bulk.min_compress_block_size = *config.block_size_bytes;
        }
        return settings;
    }

    static bool allowsSparse(const CompressedColumnSizeEstimator::WriterSettings & settings, const IDataType & type)
    {
        return settings.serialization_info.canUseSparseSerialization(type);
    }

    /// Starts a run for `column` (and for `source`, the same rows in the type of the column, with a target type).
    std::unique_ptr<Run> startRun(
        const IColumn & column, const IColumn & source, ISerialization::KindStack kind_stack, ISerialization::KindStack source_kind_stack) const
    {
        auto run = std::make_unique<Run>();
        run->kind_stack = std::move(kind_stack);
        run->source_kind_stack = std::move(source_kind_stack);
        run->writer = estimator.startWriter(column);
        if (target_type)
            run->source_writer = CompressedColumnSizeEstimator::startCounting(represented_type, estimator.getSettings(), source);
        return run;
    }

    /// Ends the run as the writer ends a part. Idempotent.
    void finishRun(Run & run) const
    {
        if (run.finished)
            return;

        const auto sizes = run.writer->finish();
        run.compressed_bytes += sizes.compressed;
        if (run.source_writer)
            run.uncompressed_bytes += run.source_writer->finish().uncompressed;
        else
            run.uncompressed_bytes += sizes.uncompressed;

        run.writer.reset();
        run.source_writer.reset();
        run.finished = true;
    }

    /// The current run, or a new one when there is none, when the state was read, or when the representation changed.
    Run & runFor(const Data & data_ref, const IColumn & rows, const IColumn & source) const
    {
        auto kind_stack = ISerialization::getKindStack(rows);
        auto source_kind_stack = target_type ? ISerialization::getKindStack(source) : ISerialization::KindStack{};
        if (data_ref.run && !data_ref.run->finished && data_ref.run->kind_stack == kind_stack
            && data_ref.run->source_kind_stack == source_kind_stack)
            return *data_ref.run;

        auto run = startRun(rows, source, std::move(kind_stack), std::move(source_kind_stack));
        if (data_ref.run)
        {
            finishRun(*data_ref.run);
            run->uncompressed_bytes = data_ref.run->uncompressed_bytes;
            run->compressed_bytes = data_ref.run->compressed_bytes;
        }
        data_ref.run = std::move(run);
        return *data_ref.run;
    }

    /// `rows` of `plain_type` converted to `type`.
    ColumnPtr convertRows(const ColumnPtr & rows, const std::optional<Conversion> & conversion, const DataTypePtr & type) const
    {
        if (!conversion)
            return rows;

        ColumnsWithTypeAndName arguments{
            {rows, plain_type, ""},
            {DataTypeString().createColumnConst(rows->size(), type->getName()), std::make_shared<DataTypeString>(), ""}};
        return conversion->prepared->execute(arguments, type, rows->size(), /*dry_run=*/ false);
    }

    /// Serializes the rows of [offset, offset + limit) one granule at a time, as the writer serializes a block.
    void serializeRows(const Data & data_ref, const IColumn & rows, size_t offset, size_t limit) const
    {
        const size_t granule_rows = estimator.granuleRows(rows);
        if (!to_represented && !target_type)
        {
            for (size_t end = offset + limit; offset < end; offset += granule_rows)
                runFor(data_ref, rows, rows).writer->write(rows, offset, std::min(granule_rows, end - offset));
            return;
        }

        /// A sparse column stays sparse through a conversion that keeps its default value the default, as a table stores the
        /// converted column (see `IExecutableFunction::executeWithoutReplicatedColumns`).
        const ColumnPtr input = rows.getPtr();
        for (size_t end = offset + limit; offset < end; offset += granule_rows)
        {
            const size_t rows_in_granule = std::min(granule_rows, end - offset);
            const ColumnPtr granule = (offset == 0 && rows_in_granule == input->size()) ? input : input->cut(offset, rows_in_granule);
            const ColumnPtr represented = convertRows(granule, to_represented, represented_type);
            if (!target_type)
            {
                runFor(data_ref, *represented, *represented).writer->write(*represented, 0, rows_in_granule);
                continue;
            }

            const ColumnPtr target = convertRows(granule, to_target, target_type);
            Run & run = runFor(data_ref, *target, *represented);
            run.source_writer->write(*represented, 0, rows_in_granule);
            run.writer->write(*target, 0, rows_in_granule);
        }
    }

    IColumn & bufferFor(Data & data_ref) const
    {
        if (!data_ref.buffer)
        {
            data_ref.buffer = plain_type->createColumn();
            data_ref.represented_type = represented_type;
        }
        return *data_ref.buffer;
    }

    /// Serializes rows after the rows added one at a time. With `write_whole_granules`, the rows that do not fill a granule wait
    /// in the buffer for the next rows, so that every granule is written at once, as in a part whose blocks hold whole granules,
    /// whatever the blocks of the query. The writer writes the rows that complete a granule from the next block separately. A
    /// sparse column is serialized as it is, since the buffer is not sparse.
    void addRows(Data & data_ref, const IColumn & rows, size_t offset, size_t limit) const
    {
        if (!write_whole_granules || rows.isSparse())
        {
            flushBuffer(data_ref);
            serializeRows(data_ref, rows, offset, limit);
            return;
        }

        const size_t granule_rows = estimator.granuleRows(rows);
        if (data_ref.buffer && !data_ref.buffer->empty())
        {
            const size_t taken = std::min(limit, granule_rows - std::min(granule_rows, data_ref.buffer->size()));
            data_ref.buffer->insertRangeFrom(rows, offset, taken);
            offset += taken;
            limit -= taken;
            if (data_ref.buffer->size() < granule_rows)
                return;
            flushBuffer(data_ref);
        }

        const size_t whole_granules_rows = limit - limit % granule_rows;
        serializeRows(data_ref, rows, offset, whole_granules_rows);
        if (whole_granules_rows < limit)
            bufferFor(data_ref).insertRangeFrom(rows, offset + whole_granules_rows, limit - whole_granules_rows);
    }

    void flushBuffer(const Data & data_ref) const
    {
        if (!data_ref.buffer || data_ref.buffer->empty())
            return;

        /// The rows come from a column of another type: they are measured as the function for that type measures them.
        if (data_ref.represented_type != represented_type && !data_ref.represented_type->equals(*represented_type))
        {
            const AggregateFunctionEstimateCompressionRatio producer({data_ref.represented_type}, parameters, config);
            producer.flushBuffer(data_ref);
            return;
        }

        serializeRows(data_ref, *data_ref.buffer, 0, data_ref.buffer->size());
        data_ref.buffer = data_ref.buffer->cloneEmpty();
    }

    /// Completes the pending work and returns the sizes of everything the state has seen.
    std::pair<UInt64, UInt64> totalSizes(const Data & data_ref) const
    {
        flushBuffer(data_ref);

        UInt64 uncompressed_size = data_ref.merged_uncompressed_size;
        UInt64 compressed_size = data_ref.merged_compressed_size;
        if (data_ref.run)
        {
            finishRun(*data_ref.run);
            uncompressed_size += data_ref.run->uncompressed_bytes;
            compressed_size += data_ref.run->compressed_bytes;
        }

        return {uncompressed_size, compressed_size};
    }

    /// Serializes the rows of [row_begin, row_end) that pass the condition of `-If` (`flags`) and are not NULL (`null_map`).
    void addRange(
        AggregateDataPtr __restrict place, const IColumn & column, size_t row_begin, size_t row_end, const UInt8 * flags, const UInt8 * null_map) const
    {
        if (row_begin >= row_end)
            return;

        const IColumn * rows = &column;
        size_t offset = row_begin;
        size_t limit = row_end - row_begin;
        ColumnPtr filtered;

        if (flags || null_map)
        {
            IColumn::Filter filter(limit);
            for (size_t i = row_begin; i < row_end; ++i)
                filter[i - row_begin] = (!flags || flags[i]) && (!null_map || !null_map[i]);

            const size_t passed = countBytesInFilter(filter);
            if (passed == 0)
                return;

            if (passed != limit)
            {
                filtered = (offset == 0 && limit == column.size())
                    ? column.filter(filter, passed)
                    : column.cut(offset, limit)->filter(filter, passed);
                rows = filtered.get();
                offset = 0;
                limit = passed;
            }
        }

        if (rows->isSparse() && !sparse_allowed)
        {
            ColumnPtr dense = recursiveRemoveSparse(rows->getPtr());
            filtered = std::move(dense);
            rows = filtered.get();
        }

        addRows(data(place), *rows, offset, limit);
    }

public:
    AggregateFunctionEstimateCompressionRatio(const DataTypes & arguments, const Array & params, EstimateCompressionRatioConfig config_)
        : IAggregateFunctionDataHelper({recursiveRemoveLowCardinality(arguments[0])}, params, std::make_shared<DataTypeFloat64>())
        , config(std::move(config_))
        , represented_type(arguments[0])
        , plain_type(argument_types[0])
        , target_type(config.target_type && !config.target_type->equals(*represented_type) ? config.target_type : nullptr)
        , estimated_type(target_type ? target_type : represented_type)
        , to_represented(represented_type != plain_type ? std::optional(createCast(plain_type, represented_type)) : std::nullopt)
        , to_target(target_type && !target_type->equals(*plain_type) ? std::optional(createCast(plain_type, target_type)) : std::nullopt)
        , write_whole_granules(!isSplitIndependent(*represented_type) || !isSplitIndependent(*estimated_type))
        , default_codec(CompressionCodecFactory::instance().getDefaultCodec())
        , codec_description(config.codec ? parseCodecDescription(*config.codec) : default_codec->getFullCodecDescription())
        , estimator(estimated_type, codec_description, default_codec, writerSettings(config))
        , sparse_allowed(
              allowsSparse(estimator.getSettings(), *estimated_type) && (!target_type || allowsSparse(estimator.getSettings(), *represented_type)))
    {
    }

    String getName() const override { return "estimateCompressionRatio"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Data & data_ref = data(place);
        IColumn & buffer = bufferFor(data_ref);
        buffer.insertFrom(*columns[0], row_num);

        if (buffer.size() >= estimator.granuleRows(buffer))
            flushBuffer(data_ref);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos) const override
    {
        const UInt8 * flags = if_argument_pos >= 0 ? assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data() : nullptr;
        addRange(place, *columns[0], row_begin, row_end, flags, nullptr);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const UInt8 * flags = if_argument_pos >= 0 ? assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data() : nullptr;
        addRange(place, *columns[0], row_begin, row_end, flags, null_map);
    }

    /// A sparse column is serialized as sparse, as a part stores it.
    void addBatchSparseSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *) const override
    {
        addRange(place, *columns[0], row_begin, row_end, nullptr, nullptr);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto [uncompressed_size, compressed_size] = totalSizes(data(rhs));

        data(place).merged_uncompressed_size += uncompressed_size;
        data(place).merged_compressed_size += compressed_size;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        auto [uncompressed_size, compressed_size] = totalSizes(data(place));

        writeVarUInt(uncompressed_size, buf);
        writeVarUInt(compressed_size, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        readVarUInt(data(place).merged_uncompressed_size, buf);
        readVarUInt(data(place).merged_compressed_size, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        Data & data_ref = data(place);
        auto [uncompressed_size, compressed_size] = totalSizes(data_ref);

        /// The sizes are kept and the run is dropped: the next row of a growing window frame adds to them, while a repeated
        /// result for an unchanged frame does not count the run again.
        data_ref.merged_uncompressed_size = uncompressed_size;
        data_ref.merged_compressed_size = compressed_size;
        data_ref.run.reset();
        data_ref.buffer.reset();

        Float64 ratio = 0;
        if (compressed_size > 0)
            ratio = static_cast<Float64>(uncompressed_size) / static_cast<Float64>(compressed_size);

        assert_cast<ColumnFloat64 &>(to).getData().push_back(ratio);
    }
};

}

static AggregateFunctionPtr createAggregateFunctionEstimateCompressionRatio(
    const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings * settings)
{
    if (arguments.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one argument", name);
    if (parameters.size() > 3)
        throw Exception(ErrorCodes::UNKNOWN_QUERY_PARAMETER, "Aggregate function {} accepts at most three parameters", name);

    const auto & type_factory = DataTypeFactory::instance();
    EstimateCompressionRatioConfig config;
    auto & codec = config.codec;
    auto & block_size_bytes = config.block_size_bytes;
    auto & target_type = config.target_type;
    for (const auto & param : parameters)
    {
        if (param.getType() == Field::Types::String)
        {
            const String & value = param.safeGet<String>();

            /// No codec is named like a data type: a string that names a type is the target type, any other string the codec.
            if (DataTypePtr type = type_factory.tryGet(value))
            {
                if (target_type)
                    throw Exception(
                        ErrorCodes::BAD_QUERY_PARAMETER,
                        "Multiple data type parameters specified for {}. Expected at most one data type (target_type) parameter",
                        name);
                target_type = std::move(type);
            }
            else
            {
                /// A string starting with the name of a type family is a mistyped type: report its error, not an unknown codec.
                const String family(value.begin(), std::find_if_not(value.begin(), value.end(), isWordCharASCII));
                if (type_factory.hasNameOrAlias(family) || type_factory.hasNameOrAlias(Poco::toLower(family)))
                    type_factory.get(value);

                if (codec.has_value())
                    throw Exception(
                        ErrorCodes::BAD_QUERY_PARAMETER,
                        "Multiple codec parameters specified for {}. Expected at most one string that is not a data type (codec)",
                        name);
                codec = value;
            }
        }
        else if (param.getType() == Field::Types::UInt64)
        {
            if (block_size_bytes.has_value())
                throw Exception(
                    ErrorCodes::BAD_QUERY_PARAMETER,
                    "Multiple numeric type parameters specified for {}. Expected at most one numeric type (block_size_bytes) parameter",
                    name);

            UInt64 new_block_size_bytes = param.safeGet<UInt64>();
            if (new_block_size_bytes == 0)
                throw Exception(ErrorCodes::BAD_QUERY_PARAMETER, "block_size_bytes should be greater than 0");

            /// The limit of the writer too; it prevents absurd memory allocations from fuzzed queries.
            static constexpr UInt64 max_block_size_bytes = CompressedColumnSizeEstimator::WriterSettings::MAX_COMPRESS_BLOCK_SIZE;
            if (new_block_size_bytes > max_block_size_bytes)
                throw Exception(
                    ErrorCodes::BAD_QUERY_PARAMETER,
                    "block_size_bytes ({}) is too large, maximum is {}",
                    new_block_size_bytes, max_block_size_bytes);

            block_size_bytes = new_block_size_bytes;
        }
        else
        {
            throw Exception(
                ErrorCodes::UNKNOWN_QUERY_PARAMETER,
                "Invalid parameter type for {}. Expected String (codec or target_type) and/or UInt64 (block_size_bytes)",
                name);
        }
    }

    /// Without a query (a type being parsed) the defaults of the query settings apply.
    config.writer_settings = CompressedColumnSizeEstimator::WriterSettings::fromServer(settings);

    return std::make_shared<AggregateFunctionEstimateCompressionRatio>(arguments, parameters, std::move(config));
}

void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory);
void registerAggregateFunctionEstimateCompressionRatio(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Estimates the compression ratio of a given column without compressing it.

The estimate follows the layout of a MergeTree wide part: every substream of the column (the dictionary and the indexes of a `LowCardinality` column, the null map of a `Nullable` column, the sizes of an `Array` column, ...) is compressed on its own, the codec applies to the data substreams while the structural substreams keep only its generic codecs, as `CODEC(...)` on a table column does, and the rows are serialized one granule at a time, whatever the size of the blocks the query reads. A `LowCardinality` column is estimated with its dictionary encoding. The rows of each `GROUP BY` group and of each window frame are serialized as if they were a part of their own, so the result also depends on how the rows are split between groups, threads and frames.

To estimate another representation of the data, either convert the argument, for example `toLowCardinality(s)` or `CAST(ip AS IPv6)`, which gives the ratio of that representation, or pass the type as the `target_type` parameter, which keeps the serialized size of the column as it is as the baseline, so that the ratios for different target types are directly comparable. The on-disk size of a representation is its serialized size divided by its ratio (for an existing column, `column_data_uncompressed_bytes` in `system.parts_columns`).

The intermediate state records only sizes, not the type they were measured from: merging states computed from columns of different types, such as `LowCardinality(String)` and `String`, gives the ratio of both columns together.

<Note>
For the examples below, the result will differ based on the default compression codec of the server.
See [Column Compression Codecs](/reference/statements/create/table#column_compression_codec).
</Note>
    )";
    FunctionDocumentation::Syntax syntax = "estimateCompressionRatio([codec][, block_size_bytes][, target_type])(column)";
    FunctionDocumentation::Arguments arguments = {
        {"column", "Column of any type.", {"Any"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"codec", "String containing a compression codec or multiple comma-separated codecs in a single string.", {"String"}},
        {"block_size_bytes", "Block size of compressed data. This is similar to setting both [`max_compress_block_size`](/reference/settings/merge-tree-settings/max#max_compress_block_size) and [`min_compress_block_size`](/reference/settings/merge-tree-settings/min#min_compress_block_size). The default value is 1 MiB (1048576 bytes). Maximum allowed value is 256 MiB (268435456 bytes).", {"UInt64"}},
        {"target_type", "Name of a data type. The column is estimated as if it were stored as this type: its rows are converted to it, as `accurateCast` does, and compressed with the codec, while the ratio keeps the serialized size of the column in its own type as the baseline. The ratios of one column for different target types are therefore directly comparable: the higher the ratio, the smaller the column would be on disk. The parameters can be given in any order: a string that names a data type is the target type, any other string is the codec.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an estimate compression ratio for the given column.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage with default codec",
        R"(
CREATE TABLE compression_estimate_example
(
    `number` UInt64
)
ENGINE = MergeTree()
ORDER BY number
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO compression_estimate_example
SELECT number FROM system.numbers LIMIT 100_000;

SELECT estimateCompressionRatio(number) AS estimate FROM compression_estimate_example
        )",
        R"(
┌──────────estimate─┐
│ 5.758875867430677 │
└───────────────────┘
        )"
    },
    {
        "Using a specific codec",
        R"(
SELECT estimateCompressionRatio('T64')(number) AS estimate FROM compression_estimate_example
        )",
        R"(
┌──────────estimate─┐
│ 3.762758101688538 │
└───────────────────┘
        )"
    },
    {
        "Using multiple codecs",
        R"(
SELECT estimateCompressionRatio('T64, ZSTD')(number) AS estimate FROM compression_estimate_example
        )",
        R"(
┌───────────estimate─┐
│ 143.60078980434392 │
└────────────────────┘
        )"
    },
    {
        "Comparing a column with another representation of its data",
        R"(
CREATE TABLE compression_estimate_strings
(
    `s` String
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO compression_estimate_strings
SELECT concat('category_', toString(number % 50)) FROM system.numbers LIMIT 100_000;

-- `as_low_cardinality` is the ratio of the `LowCardinality(String)` representation, whose serialized size is already
-- small, so it is lower than `as_string` although the column takes fewer bytes on disk. `as_low_cardinality_vs_string`
-- keeps the size of the `String` column as the baseline: `LowCardinality(String)` stores it in 1361 / 231 ≈ 6 times
-- fewer bytes than `String`.
SELECT
    estimateCompressionRatio('LZ4')(s) AS as_string,
    estimateCompressionRatio('LZ4')(toLowCardinality(s)) AS as_low_cardinality,
    estimateCompressionRatio('LZ4', 'LowCardinality(String)')(s) AS as_low_cardinality_vs_string
FROM compression_estimate_strings
        )",
        R"(
┌──────────as_string─┬─as_low_cardinality─┬─as_low_cardinality_vs_string─┐
│ 230.69403714565004 │   116.280276816609 │           1361.0149942329874 │
└────────────────────┴────────────────────┴──────────────────────────────┘
        )"
    },
    {
        "Comparing a wider type for the same values under different codecs",
        R"(
-- `IPv6` stores the same addresses in 16 bytes instead of 4. The `IPv6` ratios keep the size of the `IPv4` column as the
-- baseline, so each pair compares the on-disk sizes of the two representations: with `LZ4` the 12 constant bytes of every
-- address cost space, with `ZSTD` they compress away.
SELECT
    estimateCompressionRatio('LZ4')(ip) AS lz4_as_ipv4,
    estimateCompressionRatio('LZ4', 'IPv6')(ip) AS lz4_as_ipv6,
    estimateCompressionRatio('ZSTD')(ip) AS zstd_as_ipv4,
    estimateCompressionRatio('ZSTD', 'IPv6')(ip) AS zstd_as_ipv6
FROM (SELECT toIPv4(number * 7919) AS ip FROM numbers(100000))
        )",
        R"(
┌────────lz4_as_ipv4─┬───────lz4_as_ipv6─┬───────zstd_as_ipv4─┬───────zstd_as_ipv6─┐
│ 0.9960283370061879 │ 0.753694516307123 │ 1.0557822554676324 │ 1.2303679415329154 │
└────────────────────┴───────────────────┴────────────────────┴────────────────────┘
        )"
    },
    {
        "A specialized codec on a Nullable column",
        R"(
-- As for a table column, `T64` applies to the values and the null map keeps only the generic codecs of the chain (none here).
SELECT estimateCompressionRatio('T64')(n) AS estimate
FROM (SELECT if(number % 10 = 0, NULL, toUInt32(number)) AS n FROM numbers(100000))
        )",
        R"(
┌───────────estimate─┐
│ 1.5993090984694611 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn introduced_in = {25, 4};
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};
    factory.registerFunction(
        "estimateCompressionRatio",
        {createAggregateFunctionEstimateCompressionRatio,
         documentation,
         {.is_order_dependent = true, .is_window_function = true, .created_with_low_cardinality_types = true}});
}
}
