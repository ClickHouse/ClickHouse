#include <Storages/TimeSeries/makePrometheusHistogramsBlock.h>

#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesHistogramsColumns.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>

#include <array>
#include <limits>


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
}

namespace
{
    /// The numbers of elements of the histogram samples of a request, so that every column is reserved once.
    struct Sizes
    {
        size_t num_histograms = 0;
        size_t num_spans = 0;
        size_t num_int_buckets = 0;
        size_t num_float_buckets = 0;
        size_t num_custom_values = 0;
    };

    Sizes countSizes(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series)
    {
        Sizes sizes;
        for (const auto & element : time_series)
        {
            sizes.num_histograms += element.histograms_size();
            for (const auto & histogram : element.histograms())
            {
                sizes.num_spans += histogram.positive_spans_size() + histogram.negative_spans_size();
                sizes.num_int_buckets += histogram.positive_deltas_size() + histogram.negative_deltas_size();
                sizes.num_float_buckets += histogram.positive_counts_size() + histogram.negative_counts_size();
                sizes.num_custom_values += histogram.custom_values_size();
            }
        }
        return sizes;
    }

    /// Typed views of the columns being built.

    template <typename T>
    typename ColumnVector<T>::Container & vectorData(IColumn & column)
    {
        return assert_cast<ColumnVector<T> &>(column).getData();
    }

    /// The data of an `Array(T)` column: the elements and the offsets.
    template <typename T>
    struct ArrayData
    {
        typename ColumnVector<T>::Container & elements;
        ColumnArray::Offsets & offsets;

        explicit ArrayData(IColumn & column)
            : elements(vectorData<T>(assert_cast<ColumnArray &>(column).getData()))
            , offsets(assert_cast<ColumnArray &>(column).getOffsets())
        {
        }
    };

    /// The data of an `Array(Tuple(offset Int32, length UInt32))` column.
    struct SpansData
    {
        ColumnInt32::Container & span_offsets;
        ColumnUInt32::Container & span_lengths;
        ColumnArray::Offsets & offsets;

        explicit SpansData(IColumn & column)
            : span_offsets(vectorData<Int32>(assert_cast<ColumnTuple &>(assert_cast<ColumnArray &>(column).getData()).getColumn(0)))
            , span_lengths(vectorData<UInt32>(assert_cast<ColumnTuple &>(assert_cast<ColumnArray &>(column).getData()).getColumn(1)))
            , offsets(assert_cast<ColumnArray &>(column).getOffsets())
        {
        }
    };

    /// The payload columns of the group (every column of TimeSeriesHistogramsColumns) and their typed views.
    struct PayloadColumns
    {
        using ColumnsArray = std::array<MutableColumnPtr, TimeSeriesHistogramsColumns::getAll().size()>;
        ColumnsArray columns = createColumns();

        /// The views, one per column of the registry: a column added there must be added here too.
        ColumnUInt8::Container & is_float = vectorData<UInt8>(get(TimeSeriesHistogramsColumn::IsFloat));
        ColumnUInt8::Container & counter_reset_hint = vectorData<UInt8>(get(TimeSeriesHistogramsColumn::CounterResetHint));
        ColumnInt8::Container & schema = vectorData<Int8>(get(TimeSeriesHistogramsColumn::Schema));
        ColumnFloat64::Container & zero_threshold = vectorData<Float64>(get(TimeSeriesHistogramsColumn::ZeroThreshold));
        ColumnFloat64::Container & sum = vectorData<Float64>(get(TimeSeriesHistogramsColumn::Sum));
        SpansData positive_spans{get(TimeSeriesHistogramsColumn::PositiveSpans)};
        SpansData negative_spans{get(TimeSeriesHistogramsColumn::NegativeSpans)};
        ArrayData<Float64> custom_values{get(TimeSeriesHistogramsColumn::CustomValues)};
        ColumnUInt64::Container & count_int = vectorData<UInt64>(get(TimeSeriesHistogramsColumn::CountInt));
        ColumnUInt64::Container & zero_count_int = vectorData<UInt64>(get(TimeSeriesHistogramsColumn::ZeroCountInt));
        ArrayData<UInt64> positive_values_int{get(TimeSeriesHistogramsColumn::PositiveValuesInt)};
        ArrayData<UInt64> negative_values_int{get(TimeSeriesHistogramsColumn::NegativeValuesInt)};
        ColumnFloat64::Container & count_float = vectorData<Float64>(get(TimeSeriesHistogramsColumn::CountFloat));
        ColumnFloat64::Container & zero_count_float = vectorData<Float64>(get(TimeSeriesHistogramsColumn::ZeroCountFloat));
        ArrayData<Float64> positive_values_float{get(TimeSeriesHistogramsColumn::PositiveValuesFloat)};
        ArrayData<Float64> negative_values_float{get(TimeSeriesHistogramsColumn::NegativeValuesFloat)};

        explicit PayloadColumns(const Sizes & sizes)
        {
            for (auto & column : columns)
                column->reserve(sizes.num_histograms);

            /// The sizes are counted over both sides of the histograms. The positive side gets the whole reservation and
            /// the negative side grows on demand: reserving the total for each side would double the allocation.
            positive_spans.span_offsets.reserve(sizes.num_spans);
            positive_spans.span_lengths.reserve(sizes.num_spans);
            custom_values.elements.reserve(sizes.num_custom_values);
            positive_values_int.elements.reserve(sizes.num_int_buckets);
            positive_values_float.elements.reserve(sizes.num_float_buckets);
        }

        IColumn & get(TimeSeriesHistogramsColumn column) { return *columns[static_cast<size_t>(column)]; }

    private:
        static ColumnsArray createColumns()
        {
            ColumnsArray result;
            for (auto column : TimeSeriesHistogramsColumns::getAll())
                result[static_cast<size_t>(column)] = TimeSeriesHistogramsColumns::getDataType(column)->createColumn();
            return result;
        }
    };

    template <typename... Args>
    [[noreturn]] void fail(const prometheus::Histogram & histogram, fmt::format_string<Args...> format, Args &&... args)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid histogram sample with timestamp {} ms: {}",
            histogram.timestamp(), fmt::format(format, std::forward<Args>(args)...));
    }

    /// A histogram is an integer or a float one by its `count` arm (Prometheus's `IsFloatHistogram`; an unset arm is an integer
    /// histogram with count 0). Prometheus's receiver ignores the fields of the other flavour; the columns store one flavour
    /// and must round-trip exactly, so a message carrying both is rejected - no well-formed sender produces it.
    void checkFlavourIsUnambiguous(const prometheus::Histogram & histogram, bool is_float)
    {
        if (is_float)
        {
            if (histogram.zero_count_case() == prometheus::Histogram::kZeroCountInt)
                fail(histogram, "a float histogram (count_float is set) has zero_count_int instead of zero_count_float");
            if (histogram.positive_deltas_size() || histogram.negative_deltas_size())
                fail(histogram, "a float histogram (count_float is set) has integer bucket deltas instead of float bucket counts");
        }
        else
        {
            if (histogram.zero_count_case() == prometheus::Histogram::kZeroCountFloat)
                fail(histogram, "an integer histogram (count_float is not set) has zero_count_float instead of zero_count_int");
            if (histogram.positive_counts_size() || histogram.negative_counts_size())
                fail(histogram, "an integer histogram (count_float is not set) has float bucket counts instead of integer bucket deltas");
        }
    }

    /// The protobuf fields are wider than the columns (`sint32 schema`, and an open enum for `reset_hint`, so any int32 arrives).
    /// A plain cast would wrap an out-of-range value into a valid one, e.g. -309 into -53.
    template <typename To>
    To narrow(Int32 value, const prometheus::Histogram & histogram, std::string_view field)
    {
        if ((value < std::numeric_limits<To>::min()) || (value > std::numeric_limits<To>::max()))
            fail(histogram, "{} {} is out of the range {}..{}",
                field,
                value,
                static_cast<Int32>(std::numeric_limits<To>::min()),
                static_cast<Int32>(std::numeric_limits<To>::max()));
        return static_cast<To>(value);
    }

    void appendSpans(const google::protobuf::RepeatedPtrField<prometheus::BucketSpan> & spans, SpansData & dest)
    {
        for (const auto & span : spans)
        {
            dest.span_offsets.push_back(span.offset());
            dest.span_lengths.push_back(span.length());
        }
        dest.offsets.push_back(dest.span_offsets.size());
    }

    void appendValues(const google::protobuf::RepeatedField<double> & values, ArrayData<Float64> & dest)
    {
        dest.elements.insert(values.data(), values.data() + values.size());
        dest.offsets.push_back(dest.elements.size());
    }

    template <typename T>
    void appendEmpty(ArrayData<T> & dest)
    {
        dest.offsets.push_back(dest.elements.size());
    }

    /// Prometheus sends the integer buckets as deltas: each bucket's count relative to the previous one (the first one to zero).
    /// Decodes them to absolute counts; a negative count is Prometheus's `ErrHistogramNegativeBucketCount`.
    void decodeDeltas(
        const google::protobuf::RepeatedField<Int64> & deltas, ArrayData<UInt64> & dest, const prometheus::Histogram & histogram, std::string_view side)
    {
        Int64 count = 0;
        for (int i = 0; i != deltas.size(); ++i)
        {
            if (__builtin_add_overflow(count, deltas[i], &count))
                fail(histogram, "{} side: the bucket counts overflow Int64 while decoding the deltas", side);
            if (count < 0)
                fail(histogram, "{} side: bucket #{} has a negative count {} after decoding the deltas", side, i, count);
            dest.elements.push_back(static_cast<UInt64>(count));
        }
        dest.offsets.push_back(dest.elements.size());
    }

    /// Appends one histogram sample to the columns.
    void appendHistogram(const prometheus::Histogram & histogram, IColumn & timestamps, UInt32 timestamp_scale, PayloadColumns & payload)
    {
        const bool is_float = histogram.count_case() == prometheus::Histogram::kCountFloat;
        checkFlavourIsUnambiguous(histogram, is_float);

        insertPrometheusTimestamp(histogram.timestamp(), timestamp_scale, timestamps);
        payload.is_float.push_back(is_float);
        payload.counter_reset_hint.push_back(narrow<UInt8>(static_cast<Int32>(histogram.reset_hint()), histogram, "reset_hint"));
        payload.schema.push_back(narrow<Int8>(histogram.schema(), histogram, "schema"));
        payload.zero_threshold.push_back(histogram.zero_threshold());
        payload.sum.push_back(histogram.sum());
        appendSpans(histogram.positive_spans(), payload.positive_spans);
        appendSpans(histogram.negative_spans(), payload.negative_spans);
        appendValues(histogram.custom_values(), payload.custom_values);

        /// The columns of the other flavour get their defaults.
        if (is_float)
        {
            payload.count_int.push_back(0);
            payload.zero_count_int.push_back(0);
            appendEmpty(payload.positive_values_int);
            appendEmpty(payload.negative_values_int);
            payload.count_float.push_back(histogram.count_float());
            payload.zero_count_float.push_back(histogram.zero_count_float());
            appendValues(histogram.positive_counts(), payload.positive_values_float);
            appendValues(histogram.negative_counts(), payload.negative_values_float);
        }
        else
        {
            payload.count_int.push_back(histogram.count_int());
            payload.zero_count_int.push_back(histogram.zero_count_int());
            decodeDeltas(histogram.positive_deltas(), payload.positive_values_int, histogram, "positive");
            decodeDeltas(histogram.negative_deltas(), payload.negative_values_int, histogram, "negative");
            payload.count_float.push_back(0);
            payload.zero_count_float.push_back(0);
            appendEmpty(payload.positive_values_float);
            appendEmpty(payload.negative_values_float);
        }
    }

    /// Returns the type of the elements of the `histograms.timestamp` outer column, or nullptr if the table has no
    /// `histograms.*` columns (its version is older than TimeSeriesVersion::MIN_WITH_HISTOGRAMS_TARGET).
    DataTypePtr tryGetHistogramsTimestampType(const StorageInMemoryMetadata & metadata)
    {
        const auto column_name = TimeSeriesHistogramsColumns::getOuterColumnName(TimeSeriesColumnNames::Timestamp);
        if (!metadata.columns.has(column_name))
            return nullptr;
        const auto array_type = typeid_cast<std::shared_ptr<const DataTypeArray>>(metadata.columns.get(column_name).type);
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have an Array type", column_name);
        return array_type->getNestedType();
    }

    /// A table of an older version has no histograms table. Rejecting the request would make the sender drop the float
    /// samples of the same request too, so the histograms are dropped and the rest of the request is written.
    void logDroppedHistograms(const StorageTimeSeries & time_series_storage, size_t num_histograms)
    {
        static const LoggerPtr log = getLogger("PrometheusRemoteWriteProtocol");
        ProfileEvents::increment(ProfileEvents::PrometheusRemoteWriteDroppedHistograms, num_histograms);
        LOG_WARNING(
            LogFrequencyLimiter(log, 60),
            "{}: Dropped {} native histogram samples: the table has no histograms table because its version {} is older than {}. "
            "To store native histograms, create a table of the current version and move the data into it "
            "(see the documentation of the TimeSeries table engine)",
            time_series_storage.getStorageID().getNameForLogs(),
            num_histograms,
            time_series_storage.getVersion(),
            TimeSeriesVersion::MIN_WITH_HISTOGRAMS_TARGET);
    }
}


Block makePrometheusHistogramsBlock(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    size_t num_metadata_rows,
    const StorageTimeSeries & time_series_storage,
    const StorageInMemoryMetadata & metadata)
{
    const auto sizes = countSizes(time_series);
    if (!sizes.num_histograms)
        return {};
    ProfileEvents::increment(ProfileEvents::PrometheusRemoteWriteHistograms, sizes.num_histograms);

    const auto timestamp_type = tryGetHistogramsTimestampType(metadata);
    if (!timestamp_type)
    {
        logDroppedHistograms(time_series_storage, sizes.num_histograms);
        return {};
    }

    const size_t num_rows = time_series.size() + num_metadata_rows;
    const UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_type).value_or(0);
    auto timestamps = timestamp_type->createColumn();
    timestamps->reserve(sizes.num_histograms);
    PayloadColumns payload{sizes};

    /// The arrays of the group share one offsets column: element k of every array is histogram sample k of the row.
    auto offsets = ColumnArray::ColumnOffsets::create();
    auto & offsets_data = offsets->getData();
    offsets_data.reserve(num_rows);

    for (const auto & element : time_series)
    {
        for (const auto & histogram : element.histograms())
            appendHistogram(histogram, *timestamps, timestamp_scale, payload);
        offsets_data.push_back(timestamps->size());
    }
    offsets_data.resize_fill(num_rows, timestamps->size()); /// The metadata rows have no histograms.

    Block block;
    const ColumnPtr shared_offsets = std::move(offsets);
    const auto timestamp_outer_column = TimeSeriesHistogramsColumns::getOuterTimestampColumn(timestamp_type);
    block.insert(ColumnWithTypeAndName{
        ColumnArray::create(ColumnPtr{std::move(timestamps)}, shared_offsets), timestamp_outer_column.type, timestamp_outer_column.name});

    auto outer_column = TimeSeriesHistogramsColumns::getOuterPayloadColumns().begin();
    for (auto & column : payload.columns)
    {
        block.insert(ColumnWithTypeAndName{ColumnArray::create(ColumnPtr{std::move(column)}, shared_offsets), outer_column->type, outer_column->name});
        ++outer_column;
    }
    return block;
}

}

#endif
