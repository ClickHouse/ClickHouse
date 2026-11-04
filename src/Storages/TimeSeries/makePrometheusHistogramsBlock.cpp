#include <Storages/TimeSeries/makePrometheusHistogramsBlock.h>

#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Parsers/Prometheus/TimeSeriesVersion.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>
#include <DataTypes/TimeSeries/TimeSeriesColumnNames.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>

#include <limits>
#include <type_traits>


namespace ProfileEvents
{
    extern const Event PrometheusRemoteWriteHistograms;
    extern const Event PrometheusRemoteWriteDroppedHistograms;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// The numbers of elements of the histogram samples of a request, so that every column is reserved once.
    struct Sizes
    {
        size_t num_histograms = 0;
        size_t num_positive_int_buckets = 0;
        size_t num_negative_int_buckets = 0;
        size_t num_positive_float_buckets = 0;
        size_t num_negative_float_buckets = 0;
        size_t num_custom_values = 0;
    };

    Sizes countSizes(std::span<const prometheus::TimeSeries * const> time_series)
    {
        Sizes sizes;
        for (const auto * element : time_series)
        {
            sizes.num_histograms += element->histograms_size();
            for (const auto & histogram : element->histograms())
            {
                sizes.num_positive_int_buckets += histogram.positive_deltas_size();
                sizes.num_negative_int_buckets += histogram.negative_deltas_size();
                sizes.num_positive_float_buckets += histogram.positive_counts_size();
                sizes.num_negative_float_buckets += histogram.negative_counts_size();
                sizes.num_custom_values += histogram.custom_values_size();
            }
        }
        return sizes;
    }

    using MutableView = TimeSeriesHistogramsColumnsMutableView;
    using Registry = TimeSeriesHistogramsColumns;

    /// The containers of an `Array(Float64)` element being built: the elements and the offsets.
    struct FloatArrayData
    {
        ColumnFloat64::Container & elements;
        ColumnArray::Offsets & offsets;

        explicit FloatArrayData(ColumnArray & array)
            : elements(assert_cast<ColumnFloat64 &>(array.getData()).getData())
            , offsets(array.getOffsets())
        {
        }
    };

    /// The containers of an `Array(Tuple(index Int32, count Count))` element being built.
    template <typename Count>
    struct PairsData
    {
        ColumnInt32::Container & indexes;
        typename ColumnVector<Count>::Container & counts;
        ColumnArray::Offsets & offsets;

        explicit PairsData(ColumnArray & array)
            : indexes(assert_cast<ColumnInt32 &>(assert_cast<ColumnTuple &>(array.getData()).getColumn(Registry::getPairIndexPosition())).getData())
            , counts(assert_cast<ColumnVector<Count> &>(assert_cast<ColumnTuple &>(array.getData()).getColumn(Registry::getPairCountPosition())).getData())
            , offsets(array.getOffsets())
        {
        }

        void reserve(size_t num_pairs)
        {
            indexes.reserve(num_pairs);
            counts.reserve(num_pairs);
        }

        /// Ends the array of the current sample.
        void finishArray() { offsets.push_back(indexes.size()); }
    };

    /// The columns of the samples being built: the timestamps, the payload columns of the registry under a typed view, and
    /// the containers of the array elements the appends push into.
    struct SampleColumns
    {
        MutableColumnPtr timestamps;
        MutableView::Columns payload;
        MutableView view;
        PairsData<Int64> positive_int;
        PairsData<Int64> negative_int;
        PairsData<Float64> positive_float;
        PairsData<Float64> negative_float;
        FloatArrayData custom_values;

        SampleColumns(const DataTypePtr & timestamp_type, const Sizes & sizes)
            : timestamps(timestamp_type->createColumn())
            , payload(MutableView::createPayloadColumns())
            , view(*timestamps, payload)
            , positive_int(view.positive_int)
            , negative_int(view.negative_int)
            , positive_float(view.positive_float)
            , negative_float(view.negative_float)
            , custom_values(view.custom_values)
        {
            timestamps->reserve(sizes.num_histograms);
            for (const auto & column : payload)
                column->reserve(sizes.num_histograms);

            positive_int.reserve(sizes.num_positive_int_buckets);
            negative_int.reserve(sizes.num_negative_int_buckets);
            positive_float.reserve(sizes.num_positive_float_buckets);
            negative_float.reserve(sizes.num_negative_float_buckets);
            custom_values.elements.reserve(sizes.num_custom_values);
        }
    };

    template <typename... Args>
    [[noreturn]] void fail(const prometheus::Histogram & histogram, fmt::format_string<Args...> format, Args &&... args)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid histogram sample with timestamp {} ms: {}",
            histogram.timestamp(), fmt::format(format, std::forward<Args>(args)...));
    }

    /// The protobuf fields are wider than the columns (`sint32 schema`, an open enum for `reset_hint`, so any int32 arrives, and
    /// `uint64` counts). A plain cast would wrap an out-of-range value into a valid one, e.g. -309 into -53, or a huge count into a negative one.
    template <typename To, typename From>
    To narrow(From value, const prometheus::Histogram & histogram, std::string_view field)
    {
        if (accurate::lessOp(value, std::numeric_limits<To>::min()) || accurate::greaterOp(value, std::numeric_limits<To>::max()))
            fail(histogram, "{} {} is out of the range {}..{}",
                field,
                value,
                static_cast<Int64>(std::numeric_limits<To>::min()),
                static_cast<Int64>(std::numeric_limits<To>::max()));
        return static_cast<To>(value);
    }

    void appendValues(const google::protobuf::RepeatedField<double> & values, FloatArrayData & dest)
    {
        dest.elements.insert(values.data(), values.data() + values.size());
        dest.offsets.push_back(dest.elements.size());
    }

    /// Reads the values of the integer buckets of one side. Prometheus sends them as deltas, each bucket's count relative to the
    /// previous one (the first one to zero): the reader decodes them to absolute counts; a negative count is Prometheus's
    /// `ErrHistogramNegativeBucketCount`.
    class IntBucketsReader
    {
    public:
        IntBucketsReader(const google::protobuf::RepeatedField<Int64> & deltas_, const prometheus::Histogram & histogram_, std::string_view side_)
            : deltas(deltas_), histogram(histogram_), side(side_)
        {
        }

        Int64 get(size_t bucket)
        {
            if (__builtin_add_overflow(count, deltas[static_cast<int>(bucket)], &count))
                fail(histogram, "{} side: the bucket counts overflow Int64 while decoding the deltas", side);
            if (count < 0)
                fail(histogram, "{} side: bucket #{} has a negative count {} after decoding the deltas", side, bucket, count);
            return count;
        }

    private:
        const google::protobuf::RepeatedField<Int64> & deltas;
        const prometheus::Histogram & histogram;
        const std::string_view side;
        Int64 count = 0;
    };

    /// Reads the values of the float buckets of one side: the counts as they are.
    class FloatBucketsReader
    {
    public:
        FloatBucketsReader(const google::protobuf::RepeatedField<double> & counts_, const prometheus::Histogram &, std::string_view)
            : counts(counts_)
        {
        }

        Float64 get(size_t bucket) const { return counts[static_cast<int>(bucket)]; }

    private:
        const google::protobuf::RepeatedField<double> & counts;
    };

    /// Expands the spans of one side into the bucket indexes, as Prometheus iterates them: the offset of a span is relative to the
    /// index after the previous span (the first one to 0), and the buckets of a span have consecutive indexes. Pairs bucket `i` with
    /// its value from `values`, read by the reader of the type of the values (deltas for `Int64`, counts for `Float64`); a bucket with
    /// a count of 0 is kept, so the spans come back as sent (`client_golang` pads gaps of one or two buckets with empty ones).
    /// Rejects what the pairs can't take (spans needing another number of buckets than given, indexes outside Int32) and a negative
    /// offset of a span after the first one, as Prometheus's `checkHistogramSpans` does: with buckets in the span the indexes would go
    /// backwards, which the sink rejects anyway; without them the pairs can't tell. The other span rules hold on the pairs.
    template <typename Count>
    void appendPairs(
        const google::protobuf::RepeatedPtrField<prometheus::BucketSpan> & spans,
        const google::protobuf::RepeatedField<Count> & values,
        PairsData<Count> & dest,
        const prometheus::Histogram & histogram,
        std::string_view side)
    {
        using Reader = std::conditional_t<std::is_same_v<Count, Int64>, IntBucketsReader, FloatBucketsReader>;
        Reader reader{values, histogram, side};

        size_t total_length = 0;
        for (const auto & span : spans)
            total_length += span.length();
        if (total_length != static_cast<size_t>(values.size()))
            fail(histogram, "{} side: the spans need {} buckets, but {} buckets are given", side, total_length, values.size());

        Int64 index = 0;
        size_t bucket = 0;
        for (int span_number = 0; span_number != spans.size(); ++span_number)
        {
            const auto & span = spans[span_number];
            if ((span_number > 0) && (span.offset() < 0))
                fail(histogram, "{} side: span #{} has a negative offset {}", side, span_number, span.offset());
            index += span.offset();
            if (span.length() && ((index < std::numeric_limits<Int32>::min()) || (index + span.length() - 1 > std::numeric_limits<Int32>::max())))
                fail(histogram, "{} side: the bucket indexes overflow Int32", side);
            for (UInt32 j = 0; j != span.length(); ++j, ++index, ++bucket)
            {
                dest.indexes.push_back(static_cast<Int32>(index));
                dest.counts.push_back(reader.get(bucket));
            }
        }
        dest.finishArray();
    }

    /// Appends one histogram sample to the columns: the flavour's counts as sent, the other flavour's scalars 0 and arrays empty.
    /// A histogram is an integer or a float one by its `count` arm (Prometheus's `IsFloatHistogram`; an unset arm is an integer
    /// histogram with count 0), and only that flavour's fields are read, like Prometheus's receiver does.
    void appendHistogram(const prometheus::Histogram & histogram, UInt32 timestamp_scale, SampleColumns & columns)
    {
        const bool is_float = histogram.count_case() == prometheus::Histogram::kCountFloat;

        const MutableView & view = columns.view;
        insertPrometheusTimestamp(histogram.timestamp(), timestamp_scale, view.timestamps);
        view.flags.insertValue(is_float ? TimeSeriesHistogramFlags::IsFloat : 0);
        view.counter_reset_hint.insertValue(narrow<UInt8>(static_cast<Int32>(histogram.reset_hint()), histogram, "reset_hint"));
        view.schema.insertValue(narrow<Int8>(histogram.schema(), histogram, "schema"));
        view.zero_threshold.insertValue(histogram.zero_threshold());
        view.sum.insertValue(histogram.sum());
        appendValues(histogram.custom_values(), columns.custom_values);

        if (is_float)
        {
            view.count_int.insertValue(0);
            view.zero_count_int.insertValue(0);
            view.count_float.insertValue(histogram.count_float());
            view.zero_count_float.insertValue(histogram.zero_count_float());
            columns.positive_int.finishArray();
            columns.negative_int.finishArray();
            appendPairs(histogram.positive_spans(), histogram.positive_counts(), columns.positive_float, histogram, "positive");
            appendPairs(histogram.negative_spans(), histogram.negative_counts(), columns.negative_float, histogram, "negative");
        }
        else
        {
            view.count_int.insertValue(narrow<Int64>(histogram.count_int(), histogram, "count_int"));
            view.zero_count_int.insertValue(narrow<Int64>(histogram.zero_count_int(), histogram, "zero_count_int"));
            view.count_float.insertValue(0);
            view.zero_count_float.insertValue(0);
            appendPairs(histogram.positive_spans(), histogram.positive_deltas(), columns.positive_int, histogram, "positive");
            appendPairs(histogram.negative_spans(), histogram.negative_deltas(), columns.negative_int, histogram, "negative");
            columns.positive_float.finishArray();
            columns.negative_float.finishArray();
        }
    }

    /// Returns the type of the elements of the `histograms.timestamp` outer column. A table storing histogram samples has it
    /// (see normalizeTimeSeriesDefinition), so a table without the column must not get here.
    DataTypePtr getHistogramsTimestampType(const StorageInMemoryMetadata & metadata)
    {
        const auto column_name = Registry::getOuterColumnName(TimeSeriesColumnNames::Timestamp);
        if (!metadata.columns.has(column_name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column `{}` is missing in a TimeSeries table storing histogram samples", column_name);
        const auto array_type = typeid_cast<std::shared_ptr<const DataTypeArray>>(metadata.columns.get(column_name).type);
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have an Array type", column_name);
        return array_type->getNestedType();
    }
}


Block makePrometheusHistogramsBlock(
    std::span<const prometheus::TimeSeries * const> time_series,
    size_t num_metadata_rows,
    const StorageInMemoryMetadata & metadata)
{
    const auto sizes = countSizes(time_series);
    if (!sizes.num_histograms)
        return {};
    ProfileEvents::increment(ProfileEvents::PrometheusRemoteWriteHistograms, sizes.num_histograms);

    const auto timestamp_type = getHistogramsTimestampType(metadata);
    const size_t num_rows = time_series.size() + num_metadata_rows;
    const UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_type).value_or(0);
    SampleColumns columns{timestamp_type, sizes};

    /// The arrays of the group share one offsets column: element k of every array is histogram sample k of the row.
    auto offsets = ColumnArray::ColumnOffsets::create();
    auto & offsets_data = offsets->getData();
    offsets_data.reserve(num_rows);

    for (const auto * element : time_series)
    {
        for (const auto & histogram : element->histograms())
            appendHistogram(histogram, timestamp_scale, columns);
        offsets_data.push_back(columns.view.size());
    }
    offsets_data.resize_fill(num_rows, columns.view.size()); /// The metadata rows have no histograms.

    Block block;
    const ColumnPtr shared_offsets = std::move(offsets);
    const auto timestamp_outer_column = Registry::getOuterTimestampColumn(timestamp_type);
    block.insert(ColumnWithTypeAndName{
        ColumnArray::create(ColumnPtr{std::move(columns.timestamps)}, shared_offsets), timestamp_outer_column.type, timestamp_outer_column.name});

    auto outer_column = Registry::getOuterPayloadColumns().begin();
    for (const auto column : Registry::getAll())
    {
        block.insert(ColumnWithTypeAndName{
            ColumnArray::create(ColumnPtr{std::move(columns.payload[static_cast<size_t>(column)])}, shared_offsets), outer_column->type, outer_column->name});
        ++outer_column;
    }
    return block;
}


void dropPrometheusHistograms(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    const StorageTimeSeries & time_series_storage)
{
    size_t num_histograms = 0;
    for (const auto & element : time_series)
        num_histograms += element.histograms_size();
    if (!num_histograms)
        return;
    ProfileEvents::increment(ProfileEvents::PrometheusRemoteWriteHistograms, num_histograms);
    ProfileEvents::increment(ProfileEvents::PrometheusRemoteWriteDroppedHistograms, num_histograms);

    static const LoggerPtr log = getLogger("PrometheusRemoteWriteProtocol");
    LOG_WARNING(
        LogFrequencyLimiter(log, 60),
        "{}: Dropped {} native histogram samples: the table stores no histograms because its version {} is older than {}. "
        "To store native histograms, create a table of version {} or later and move the data into it",
        time_series_storage.getStorageID().getNameForLogs(),
        num_histograms,
        time_series_storage.getVersion(),
        TimeSeriesVersion::MIN_WITH_NATIVE_HISTOGRAMS,
        TimeSeriesVersion::MIN_WITH_NATIVE_HISTOGRAMS);
}

}

#endif
