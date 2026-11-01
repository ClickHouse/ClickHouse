#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>


namespace DB
{

class ColumnTuple;

/// Bit layout of the `flags` column of the "histograms" target table.
namespace TimeSeriesHistogramFlags
{
    constexpr UInt8 IsFloat = 0x01;
    constexpr UInt8 CounterResetHintShift = 1;
    constexpr UInt8 CounterResetHintMask = 0x06;  /// prometheus::Histogram::ResetHint (UNKNOWN/YES/NO/GAUGE) << 1
    /// 0x08 is reserved (was a gauge bit, dropped as redundant with reset hint == GAUGE).
    constexpr UInt8 StaleMarker = 0x10;
}

/// Indexes of the elements of the tuple in the outer `histograms` column,
/// see getTimeSeriesHistogramsOuterColumnType().
/// The tuple is a persisted format: extend it only by APPENDING new elements at the end
/// (before `Size`), so tables written by older versions keep a compatible layout.
namespace TimeSeriesHistogramsTupleIndex
{
    constexpr size_t Timestamp = 0;
    constexpr size_t Flags = 1;
    constexpr size_t Schema = 2;
    constexpr size_t ZeroThreshold = 3;
    constexpr size_t Count = 4;
    constexpr size_t Sum = 5;
    constexpr size_t ZeroCount = 6;
    constexpr size_t PositiveSpans = 7;
    constexpr size_t PositiveValues = 8;
    constexpr size_t NegativeSpans = 9;
    constexpr size_t NegativeValues = 10;
    constexpr size_t CustomValues = 11;

    /// Exact carriers of the counts of an integer-flavor histogram (see TimeSeriesColumnNames):
    /// `Float64` represents integers only up to 2^53 exactly, so when the `flags` bit 0 is clear
    /// (an integer histogram) its count, zero count and decoded bucket counts are also stored here,
    /// verbatim, which makes such a histogram round-trip losslessly. The corresponding Float64
    /// columns stay populated with rounded copies, so readers unaware of these elements keep working.
    /// Always zero/empty for float-flavor histograms.
    constexpr size_t CountInt = 12;
    constexpr size_t ZeroCountInt = 13;
    constexpr size_t PositiveValuesInt = 14;
    constexpr size_t NegativeValuesInt = 15;
    constexpr size_t Size = 16;
}

/// Type of the `positive_spans` and `negative_spans` columns: Array(Tuple(offset Int32, length UInt32)).
DataTypePtr getTimeSeriesHistogramSpansType();

/// The payload columns of the "histograms" target table: everything except `id` and `timestamp`,
/// in the order they appear both in the table and in the outer column's tuple.
NamesAndTypes getTimeSeriesHistogramPayloadColumns();

/// Type of the outer `histograms` column of a TimeSeries table with a "histograms" target:
/// Array(Tuple(timestamp, <payload columns>)), one tuple per histogram sample.
DataTypePtr getTimeSeriesHistogramsOuterColumnType(const DataTypePtr & timestamp_type);

/// Schema numbers of Prometheus native histograms: exponential schemas are in [-4, 8],
/// and -53 selects custom buckets (Prometheus model/histogram/generic.go).
constexpr Int32 HISTOGRAM_EXPONENTIAL_SCHEMA_MIN = -4;
constexpr Int32 HISTOGRAM_EXPONENTIAL_SCHEMA_MAX = 8;
constexpr Int32 HISTOGRAM_CUSTOM_BUCKETS_SCHEMA = -53;

/// Checks the invariants later readers rely on for one sample of the outer `histograms` column: known flags and schema,
/// non-negative counts which are NaN only in a stale marker, spans that cover exactly the bucket values, the custom-bucket
/// rules (no negative buckets, an unused zero bucket, finite strictly increasing bounds covering the bucket indexes) or
/// no custom bounds for an exponential schema, and exact integer carriers that match the counts of an integer histogram
/// (and are zero/empty for a float one). Throws INCORRECT_DATA otherwise. `tuple` is indexed by TimeSeriesHistogramsTupleIndex.
/// Both the Prometheus remote-write protocol and an `INSERT` into the outer `histograms` column run it.
void validateTimeSeriesHistogramSample(const ColumnTuple & tuple, size_t row);

}
