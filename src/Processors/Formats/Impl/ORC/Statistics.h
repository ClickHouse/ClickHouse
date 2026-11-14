#pragma once

#include <Processors/Formats/Impl/ORC/Type.h>

#include <string_view>

namespace DB::ORC
{

/// Accumulates the statistics of a column for a row group, a stripe or the whole file,
/// in the same way as the reference implementation.
class StatisticsBuilder
{
public:
    explicit StatisticsBuilder(TypeKind kind_) : kind(kind_) {}

    void increase(UInt64 count) { number_of_values += count; }
    void setHasNull() { has_null = true; }

    void updateInteger(Int64 value);
    void updateDouble(double value);
    void updateString(std::string_view value);
    void updateBinary(size_t length);
    void updateBoolean(bool value);
    void updateDate(Int32 value);
    void updateDecimal(Int128 value, Int32 scale);
    /// `millis` since the epoch in UTC, `nanos` is the sub-millisecond part.
    void updateTimestamp(Int64 millis, Int32 nanos);
    void updateCollection(UInt64 children);

    void merge(const StatisticsBuilder & other);
    void reset();

    Proto::ColumnStatistics toProto() const;

private:
    TypeKind kind;
    UInt64 number_of_values = 0;
    bool has_null = false;
    bool has_minimum = false;

    Int64 int_minimum = 0;
    Int64 int_maximum = 0;
    Int64 int_sum = 0;
    bool has_int_sum = true;

    double double_minimum = 0;
    double double_maximum = 0;
    double double_sum = 0;

    std::string string_minimum;
    std::string string_maximum;
    UInt64 total_length = 0;

    UInt64 true_count = 0;

    Int128 decimal_minimum = 0;
    Int32 decimal_minimum_scale = 0;
    Int128 decimal_maximum = 0;
    Int32 decimal_maximum_scale = 0;
    Int128 decimal_sum = 0;
    Int32 decimal_sum_scale = 0;
    bool has_decimal_sum = true;

    Int32 minimum_nanos = 0;
    Int32 maximum_nanos = 999999;

    UInt64 total_children = 0;
};

}
