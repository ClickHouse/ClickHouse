#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>


namespace DB
{

/// Bit layout of the `flags` column of the "histograms" target table.
namespace TimeSeriesHistogramFlags
{
    constexpr UInt8 IsFloat = 0x01;
    constexpr UInt8 CounterResetHintShift = 1;
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
    constexpr size_t Size = 12;
}

/// Type of the `positive_spans` and `negative_spans` columns: Array(Tuple(offset Int32, length UInt32)).
DataTypePtr getTimeSeriesHistogramSpansType();

/// The payload columns of the "histograms" target table: everything except `id` and `timestamp`,
/// in the order they appear both in the table and in the outer column's tuple.
NamesAndTypes getTimeSeriesHistogramPayloadColumns();

/// Type of the outer `histograms` column of a TimeSeries table with a "histograms" target:
/// Array(Tuple(timestamp, <payload columns>)), one tuple per histogram sample.
DataTypePtr getTimeSeriesHistogramsOuterColumnType(const DataTypePtr & timestamp_type);

}
