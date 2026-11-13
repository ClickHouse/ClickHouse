#pragma once

#include <Processors/Formats/Impl/ORC/Type.h>

#include <optional>
#include <string>
#include <string_view>

namespace DB::ORC
{

/// String statistics longer than this are replaced by bounds, as in the reference implementation.
static constexpr size_t MAX_STRING_STATISTICS_SIZE = 1024;

/// A prefix of at most `MAX_STRING_STATISTICS_SIZE` bytes that does not split a UTF-8 character. Not greater than `value`.
std::string_view truncateStringLowerBound(std::string_view value);

/// A string of about `MAX_STRING_STATISTICS_SIZE` bytes greater than `value` and every string with the same prefix:
/// the last character of the truncated prefix is incremented. `std::nullopt` if there is no such string.
std::optional<std::string> truncateStringUpperBound(std::string_view value);

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

    [[nodiscard]] Proto::ColumnStatistics toProto() const;

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

    void setStringMinimum(std::string_view value);
    void setStringMaximum(std::string_view value);

    /// Long values are kept as a truncated lower bound and upper bound (or the exact maximum if it has no upper bound).
    std::string string_minimum;
    std::string string_maximum;
    bool string_minimum_is_bound = false;
    bool string_maximum_is_bound = false;
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
