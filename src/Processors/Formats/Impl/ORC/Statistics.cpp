#include <Processors/Formats/Impl/ORC/Statistics.h>
#include <Processors/Formats/Impl/ORC/SearchArgument.h>

#include <Common/UTF8Helpers.h>

#include <base/extended_types.h>

#include <algorithm>


namespace DB::ORC
{

namespace
{

int compareDecimals(Int128 a, Int32 a_scale, Int128 b, Int32 b_scale)
{
    Int256 left = a;
    Int256 right = b;
    for (Int32 s = a_scale; s < b_scale; ++s)
        left *= 10;
    for (Int32 s = b_scale; s < a_scale; ++s)
        right *= 10;
    return left < right ? -1 : (left == right ? 0 : 1);
}

std::string decimalToString(Int128 value, Int32 scale)
{
    return Literal::Decimal{value, scale}.toString();
}

struct UTF8Character
{
    size_t start;
    UInt32 code_point;
};

/// The last character of the string, if it is a valid UTF-8 sequence.
std::optional<UTF8Character> lastUTF8Character(const std::string & s)
{
    const auto * begin = reinterpret_cast<const UInt8 *>(s.data());
    const UInt8 * last = begin + s.size() - 1;
    UTF8::syncBackward(last, begin);
    const size_t start = last - begin;
    const size_t length = s.size() - start;
    if (UTF8::seqLength(*last) != length)
        return std::nullopt;
    /// Rejects the overlong encodings, which do not keep the order of the code points.
    auto code_point = UTF8::convertUTF8ToCodePoint(s.data() + start, length);
    if (!code_point)
        return std::nullopt;
    return UTF8Character{start, *code_point};
}

}

std::string_view truncateStringLowerBound(std::string_view value)
{
    if (value.size() <= MAX_STRING_STATISTICS_SIZE)
        return value;
    size_t length = MAX_STRING_STATISTICS_SIZE;
    /// The first byte after the prefix must not continue a character, which takes at most 4 bytes.
    for (size_t i = 0; i < 3 && length > 0 && UTF8::isContinuationOctet(static_cast<UInt8>(value[length])); ++i)
        --length;
    return value.substr(0, length);
}

std::optional<std::string> truncateStringUpperBound(std::string_view value)
{
    std::string result(truncateStringLowerBound(value));
    /// Any string greater than the prefix and not starting with it is greater than all strings that start with it.
    while (!result.empty())
    {
        if (auto last = lastUTF8Character(result))
        {
            /// Increment the last character, which keeps valid UTF-8 valid for the readers that decode the bounds.
            result.resize(last->start);
            /// The largest code point cannot be incremented. U+FFFF is incremented to U+10000, which the readers
            /// comparing UTF-16 (Java) order before it. Increment the previous character instead.
            if (last->code_point >= 0x10FFFF || last->code_point == 0xFFFF)
                continue;
            UInt32 next = last->code_point + 1;
            if (UTF8::isSurrogateCodePoint(next))
                next = 0xE000;
            char encoded[4];
            const size_t encoded_size = UTF8::convertCodePointToUTF8(static_cast<int>(next), encoded, sizeof(encoded));
            result.append(encoded, encoded_size);
            return result;
        }
        /// Not valid UTF-8: increment the last byte.
        if (static_cast<UInt8>(result.back()) != 0xFF)
        {
            result.back() = static_cast<char>(static_cast<UInt8>(result.back()) + 1);
            return result;
        }
        result.pop_back();
    }
    return std::nullopt;
}

void StatisticsBuilder::updateInteger(Int64 value)
{
    if (!has_minimum)
    {
        has_minimum = true;
        int_minimum = int_maximum = value;
    }
    else
    {
        int_minimum = std::min(int_minimum, value);
        int_maximum = std::max(int_maximum, value);
    }
    if (has_int_sum && __builtin_add_overflow(int_sum, value, &int_sum))
        has_int_sum = false;
}

void StatisticsBuilder::updateDouble(double value)
{
    if (!has_minimum)
    {
        has_minimum = true;
        double_minimum = double_maximum = value;
    }
    else
    {
        double_minimum = std::min(double_minimum, value);
        double_maximum = std::max(double_maximum, value);
    }
    double_sum += value;
}

void StatisticsBuilder::setStringMinimum(std::string_view value)
{
    string_minimum_is_bound = value.size() > MAX_STRING_STATISTICS_SIZE;
    string_minimum = truncateStringLowerBound(value);
}

void StatisticsBuilder::setStringMaximum(std::string_view value)
{
    string_maximum_is_bound = value.size() > MAX_STRING_STATISTICS_SIZE;
    if (!string_maximum_is_bound)
    {
        string_maximum = value;
        return;
    }
    auto upper_bound = truncateStringUpperBound(value);
    if (upper_bound)
        string_maximum = std::move(*upper_bound);
    else
    {
        /// The readers need a maximum (Java treats a column without one as having no values), so keep the exact one.
        string_maximum_is_bound = false;
        string_maximum = value;
    }
}

void StatisticsBuilder::updateString(std::string_view value)
{
    if (!has_minimum)
    {
        has_minimum = true;
        setStringMinimum(value);
        setStringMaximum(value);
    }
    else
    {
        /// The comparisons with the bounds keep them correct. A value equal to a bound makes it exact.
        if (value < std::string_view(string_minimum))
            setStringMinimum(value);
        else if (string_minimum_is_bound && value == std::string_view(string_minimum))
            string_minimum_is_bound = false;

        if (value > std::string_view(string_maximum))
            setStringMaximum(value);
        else if (string_maximum_is_bound && value == std::string_view(string_maximum))
            string_maximum_is_bound = false;
    }
    total_length += value.size();
}

void StatisticsBuilder::updateBinary(size_t length)
{
    total_length += length;
}

void StatisticsBuilder::updateBoolean(bool value)
{
    if (value)
        ++true_count;
}

void StatisticsBuilder::updateDate(Int32 value)
{
    updateInteger(value);
}

void StatisticsBuilder::updateDecimal(Int128 value, Int32 scale)
{
    if (!has_minimum)
    {
        has_minimum = true;
        decimal_minimum = decimal_maximum = value;
        decimal_minimum_scale = decimal_maximum_scale = scale;
    }
    else
    {
        if (compareDecimals(value, scale, decimal_minimum, decimal_minimum_scale) < 0)
        {
            decimal_minimum = value;
            decimal_minimum_scale = scale;
        }
        if (compareDecimals(value, scale, decimal_maximum, decimal_maximum_scale) > 0)
        {
            decimal_maximum = value;
            decimal_maximum_scale = scale;
        }
    }

    if (has_decimal_sum)
    {
        Int256 sum = decimal_sum;
        Int256 addend = value;
        Int32 result_scale = std::max(decimal_sum_scale, scale);
        for (Int32 s = decimal_sum_scale; s < result_scale; ++s)
            sum *= 10;
        for (Int32 s = scale; s < result_scale; ++s)
            addend *= 10;
        sum += addend;
        if (sum > Int256(std::numeric_limits<Int128>::max()) || sum < Int256(std::numeric_limits<Int128>::min()))
            has_decimal_sum = false;
        else
        {
            decimal_sum = static_cast<Int128>(sum);
            decimal_sum_scale = result_scale;
        }
    }
}

void StatisticsBuilder::updateTimestamp(Int64 millis, Int32 nanos)
{
    if (!has_minimum)
    {
        has_minimum = true;
        int_minimum = int_maximum = millis;
        minimum_nanos = maximum_nanos = nanos;
        return;
    }
    if (millis <= int_minimum)
    {
        if (millis < int_minimum || nanos < minimum_nanos)
            minimum_nanos = nanos;
        int_minimum = millis;
    }
    if (millis >= int_maximum)
    {
        if (millis > int_maximum || nanos > maximum_nanos)
            maximum_nanos = nanos;
        int_maximum = millis;
    }
}

void StatisticsBuilder::updateCollection(UInt64 children)
{
    Int64 value = static_cast<Int64>(children);
    if (!has_minimum)
    {
        has_minimum = true;
        int_minimum = int_maximum = value;
    }
    else
    {
        int_minimum = std::min(int_minimum, value);
        int_maximum = std::max(int_maximum, value);
    }
    total_children += children;
}

void StatisticsBuilder::merge(const StatisticsBuilder & other)
{
    number_of_values += other.number_of_values;
    has_null |= other.has_null;

    switch (kind)
    {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DATE:
            if (other.has_minimum)
            {
                if (!has_minimum)
                {
                    int_minimum = other.int_minimum;
                    int_maximum = other.int_maximum;
                }
                else
                {
                    int_minimum = std::min(int_minimum, other.int_minimum);
                    int_maximum = std::max(int_maximum, other.int_maximum);
                }
                has_minimum = true;
            }
            has_int_sum = has_int_sum && other.has_int_sum;
            if (has_int_sum && __builtin_add_overflow(int_sum, other.int_sum, &int_sum))
                has_int_sum = false;
            break;
        case FLOAT:
        case DOUBLE:
            if (other.has_minimum)
            {
                if (!has_minimum)
                {
                    double_minimum = other.double_minimum;
                    double_maximum = other.double_maximum;
                }
                else
                {
                    double_minimum = std::min(double_minimum, other.double_minimum);
                    double_maximum = std::max(double_maximum, other.double_maximum);
                }
                has_minimum = true;
            }
            double_sum += other.double_sum;
            break;
        case STRING:
        case CHAR:
        case VARCHAR:
            if (other.has_minimum)
            {
                if (!has_minimum)
                {
                    string_minimum = other.string_minimum;
                    string_minimum_is_bound = other.string_minimum_is_bound;
                    string_maximum = other.string_maximum;
                    string_maximum_is_bound = other.string_maximum_is_bound;
                }
                else
                {
                    if (other.string_minimum < string_minimum)
                    {
                        string_minimum = other.string_minimum;
                        string_minimum_is_bound = other.string_minimum_is_bound;
                    }
                    else if (other.string_minimum == string_minimum)
                        string_minimum_is_bound = string_minimum_is_bound && other.string_minimum_is_bound;

                    if (other.string_maximum > string_maximum)
                    {
                        string_maximum = other.string_maximum;
                        string_maximum_is_bound = other.string_maximum_is_bound;
                    }
                    else if (other.string_maximum == string_maximum)
                        string_maximum_is_bound = string_maximum_is_bound && other.string_maximum_is_bound;
                }
                has_minimum = true;
            }
            total_length += other.total_length;
            break;
        case BINARY:
            total_length += other.total_length;
            break;
        case BOOLEAN:
            true_count += other.true_count;
            break;
        case DECIMAL:
            if (other.has_minimum)
            {
                if (!has_minimum)
                {
                    decimal_minimum = other.decimal_minimum;
                    decimal_minimum_scale = other.decimal_minimum_scale;
                    decimal_maximum = other.decimal_maximum;
                    decimal_maximum_scale = other.decimal_maximum_scale;
                }
                else
                {
                    if (compareDecimals(other.decimal_minimum, other.decimal_minimum_scale, decimal_minimum, decimal_minimum_scale) < 0)
                    {
                        decimal_minimum = other.decimal_minimum;
                        decimal_minimum_scale = other.decimal_minimum_scale;
                    }
                    if (compareDecimals(other.decimal_maximum, other.decimal_maximum_scale, decimal_maximum, decimal_maximum_scale) > 0)
                    {
                        decimal_maximum = other.decimal_maximum;
                        decimal_maximum_scale = other.decimal_maximum_scale;
                    }
                }
                has_minimum = true;
            }
            has_decimal_sum = has_decimal_sum && other.has_decimal_sum;
            if (has_decimal_sum)
            {
                /// Reuse the update of the sum without touching the minimum and the maximum.
                bool saved_has_minimum = has_minimum;
                Int128 saved_min = decimal_minimum;
                Int32 saved_min_scale = decimal_minimum_scale;
                Int128 saved_max = decimal_maximum;
                Int32 saved_max_scale = decimal_maximum_scale;
                updateDecimal(other.decimal_sum, other.decimal_sum_scale);
                has_minimum = saved_has_minimum;
                decimal_minimum = saved_min;
                decimal_minimum_scale = saved_min_scale;
                decimal_maximum = saved_max;
                decimal_maximum_scale = saved_max_scale;
            }
            break;
        case TIMESTAMP:
        case TIMESTAMP_INSTANT:
            if (other.has_minimum)
            {
                if (!has_minimum)
                {
                    int_minimum = other.int_minimum;
                    int_maximum = other.int_maximum;
                    minimum_nanos = other.minimum_nanos;
                    maximum_nanos = other.maximum_nanos;
                    has_minimum = true;
                }
                else
                {
                    if (other.int_minimum <= int_minimum)
                    {
                        if (other.int_minimum < int_minimum || other.minimum_nanos < minimum_nanos)
                            minimum_nanos = other.minimum_nanos;
                        int_minimum = other.int_minimum;
                    }
                    if (other.int_maximum >= int_maximum)
                    {
                        if (other.int_maximum > int_maximum || other.maximum_nanos > maximum_nanos)
                            maximum_nanos = other.maximum_nanos;
                        int_maximum = other.int_maximum;
                    }
                }
            }
            break;
        case LIST:
        case MAP:
            if (other.has_minimum)
            {
                if (!has_minimum)
                {
                    int_minimum = other.int_minimum;
                    int_maximum = other.int_maximum;
                }
                else
                {
                    int_minimum = std::min(int_minimum, other.int_minimum);
                    int_maximum = std::max(int_maximum, other.int_maximum);
                }
                has_minimum = true;
            }
            total_children += other.total_children;
            break;
        default:
            break;
    }
}

void StatisticsBuilder::reset()
{
    *this = StatisticsBuilder(kind);
}

Proto::ColumnStatistics StatisticsBuilder::toProto() const
{
    Proto::ColumnStatistics res;
    res.has_null = has_null;
    res.number_of_values = number_of_values;

    switch (kind)
    {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        {
            auto & stats = res.int_statistics.emplace();
            if (has_minimum)
            {
                stats.minimum = int_minimum;
                stats.maximum = int_maximum;
            }
            if (has_int_sum)
                stats.sum = int_sum;
            break;
        }
        case DATE:
        {
            auto & stats = res.date_statistics.emplace();
            if (has_minimum)
            {
                stats.minimum = static_cast<Int32>(int_minimum);
                stats.maximum = static_cast<Int32>(int_maximum);
            }
            break;
        }
        case FLOAT:
        case DOUBLE:
        {
            auto & stats = res.double_statistics.emplace();
            if (has_minimum)
            {
                stats.minimum = double_minimum;
                stats.maximum = double_maximum;
            }
            stats.sum = double_sum;
            break;
        }
        case STRING:
        case CHAR:
        case VARCHAR:
        {
            auto & stats = res.string_statistics.emplace();
            if (has_minimum)
            {
                (string_minimum_is_bound ? stats.lower_bound : stats.minimum) = string_minimum;
                (string_maximum_is_bound ? stats.upper_bound : stats.maximum) = string_maximum;
            }
            stats.sum = static_cast<Int64>(total_length);
            break;
        }
        case BINARY:
            res.binary_statistics.emplace().sum = static_cast<Int64>(total_length);
            break;
        case BOOLEAN:
            res.bucket_statistics.emplace().count.push_back(true_count);
            break;
        case DECIMAL:
        {
            auto & stats = res.decimal_statistics.emplace();
            if (has_minimum)
            {
                stats.minimum = decimalToString(decimal_minimum, decimal_minimum_scale);
                stats.maximum = decimalToString(decimal_maximum, decimal_maximum_scale);
            }
            if (has_decimal_sum)
                stats.sum = decimalToString(decimal_sum, decimal_sum_scale);
            break;
        }
        case TIMESTAMP:
        case TIMESTAMP_INSTANT:
        {
            auto & stats = res.timestamp_statistics.emplace();
            if (has_minimum)
            {
                stats.minimum_utc = int_minimum;
                stats.maximum_utc = int_maximum;
                if (minimum_nanos != 0)
                    stats.minimum_nanos = static_cast<UInt64>(minimum_nanos) + 1;
                if (maximum_nanos != 999999)
                    stats.maximum_nanos = static_cast<UInt64>(maximum_nanos) + 1;
            }
            break;
        }
        case LIST:
        case MAP:
        {
            auto & stats = res.collection_statistics.emplace();
            if (has_minimum)
            {
                stats.min_children = static_cast<UInt64>(int_minimum);
                stats.max_children = static_cast<UInt64>(int_maximum);
            }
            stats.total_children = total_children;
            break;
        }
        default:
            break;
    }
    return res;
}

}
