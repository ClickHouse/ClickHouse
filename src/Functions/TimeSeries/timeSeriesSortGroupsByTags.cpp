#include <Functions/FunctionFactory.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <bit>
#include <numeric>
#include <optional>
#include <span>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

namespace
{

using TagNamesAndValuesPtr = TimeSeriesTagsFunctionHelpers::TagNamesAndValuesPtr;

/// naturalLess is ported from github.com/facette/natsort, Copyright (c) 2015, Vincent Batoufflet and Marc Falzon. All rights reserved.
/// GoSort is ported from Go's slices package, Copyright 2022 The Go Authors. All rights reserved. Both are under the BSD-3-Clause license.

bool isDigit(char c) { return c >= '0' && c <= '9'; }

/// Go's strconv.Atoi for a run of digits: fails if the value is above the Int64 range.
std::optional<Int64> parseDigits(std::string_view digits)
{
    UInt64 value = 0;
    for (char c : digits)
    {
        UInt64 digit = c - '0';
        if (value > (static_cast<UInt64>(std::numeric_limits<Int64>::max()) - digit) / 10)
            return std::nullopt;
        value = value * 10 + digit;
    }
    return static_cast<Int64>(value);
}

/// Prometheus's natsort.Compare (github.com/facette/natsort 2cd4dd1e2dcb): a string is split
/// into runs of digits and non-digits, and the runs are compared one by one.
bool naturalLess(std::string_view a, std::string_view b)
{
    size_t pos_a = 0;
    size_t pos_b = 0;
    while (pos_a < a.size())
    {
        if (pos_b >= b.size())
            return false;

        auto next_run = [](std::string_view s, size_t & pos)
        {
            size_t start = pos;
            bool digits = isDigit(s[pos]);
            while (pos < s.size() && isDigit(s[pos]) == digits)
                ++pos;
            return std::pair{s.substr(start, pos - start), digits};
        };
        auto [run_a, digits_a] = next_run(a, pos_a);
        auto [run_b, digits_b] = next_run(b, pos_b);
        bool last_a = (pos_a == a.size());
        bool last_b = (pos_b == b.size());

        std::optional<Int64> number_a = digits_a ? parseDigits(run_a) : std::nullopt;
        std::optional<Int64> number_b = digits_b ? parseDigits(run_b) : std::nullopt;

        bool equal_runs = (number_a && number_b) ? (*number_a == *number_b) : (run_a == run_b);
        if (equal_runs)
        {
            if (last_a)
                return true;
            if (last_b)
                return false;
            continue;
        }

        if (number_a && number_b)
            return *number_a < *number_b;
        return run_a < run_b;
    }
    return false;
}

/// Prometheus's labels.Compare.
int compareTags(const TagNamesAndValuesPtr & a, const TagNamesAndValuesPtr & b)
{
    size_t count = std::min(a->size(), b->size());
    for (size_t i = 0; i < count; ++i)
    {
        if (int c = (*a)[i].first.compare((*b)[i].first))
            return c;
        if (int c = (*a)[i].second.compare((*b)[i].second))
            return c;
    }
    return (a->size() < b->size()) ? -1 : (a->size() > b->size());
}

/// A port of Go 1.24's slices.SortFunc (pdqsort). Prometheus's sort_by_label comparator is not
/// always consistent, so only the same algorithm gives the same order.
template <typename Cmp>
class GoSort
{
public:
    GoSort(VectorWithMemoryTracking<size_t> & data_, Cmp cmp_) : data(data_), cmp(cmp_) {}

    void sort()
    {
        size_t n = data.size();
        pdqsort(0, n, std::bit_width(n));
    }

private:
    VectorWithMemoryTracking<size_t> & data;
    Cmp cmp;

    bool less(size_t i, size_t j) const { return cmp(data[i], data[j]) < 0; }
    void swap(size_t i, size_t j) { std::swap(data[i], data[j]); }

    void insertionSort(size_t a, size_t b)
    {
        for (size_t i = a + 1; i < b; ++i)
            for (size_t j = i; j > a && less(j, j - 1); --j)
                swap(j, j - 1);
    }

    void siftDown(size_t lo, size_t hi, size_t first)
    {
        size_t root = lo;
        while (true)
        {
            size_t child = 2 * root + 1;
            if (child >= hi)
                break;
            if (child + 1 < hi && less(first + child, first + child + 1))
                ++child;
            if (!less(first + root, first + child))
                return;
            swap(first + root, first + child);
            root = child;
        }
    }

    void heapSort(size_t a, size_t b)
    {
        size_t first = a;
        size_t hi = b - a;
        for (size_t i = (hi - 1) / 2 + 1; i-- > 0;)
            siftDown(i, hi, first);
        for (size_t i = hi; i-- > 0;)
        {
            swap(first, first + i);
            siftDown(0, i, first);
        }
    }

    enum class Hint { Unknown, Increasing, Decreasing };

    void pdqsort(size_t a, size_t b, size_t limit)
    {
        constexpr size_t max_insertion = 12;
        bool was_balanced = true;
        bool was_partitioned = true;

        while (true)
        {
            size_t length = b - a;
            if (length <= max_insertion)
            {
                insertionSort(a, b);
                return;
            }

            if (limit == 0)
            {
                heapSort(a, b);
                return;
            }

            if (!was_balanced)
            {
                breakPatterns(a, b);
                --limit;
            }

            auto [pivot, hint] = choosePivot(a, b);
            if (hint == Hint::Decreasing)
            {
                std::reverse(data.begin() + a, data.begin() + b);
                pivot = (b - 1) - (pivot - a);
                hint = Hint::Increasing;
            }

            if (was_balanced && was_partitioned && hint == Hint::Increasing && partialInsertionSort(a, b))
                return;

            if (a > 0 && !less(a - 1, pivot))
            {
                a = partitionEqual(a, b, pivot);
                continue;
            }

            auto [mid, already_partitioned] = partition(a, b, pivot);
            was_partitioned = already_partitioned;

            size_t left_length = mid - a;
            size_t right_length = b - mid;
            size_t balance_threshold = length / 8;
            if (left_length < right_length)
            {
                was_balanced = left_length >= balance_threshold;
                pdqsort(a, mid, limit);
                a = mid + 1;
            }
            else
            {
                was_balanced = right_length >= balance_threshold;
                pdqsort(mid + 1, b, limit);
                b = mid;
            }
        }
    }

    std::pair<size_t, bool> partition(size_t a, size_t b, size_t pivot)
    {
        swap(a, pivot);
        size_t i = a + 1;
        size_t j = b - 1;

        while (i <= j && less(i, a))
            ++i;
        while (i <= j && !less(j, a))
            --j;
        if (i > j)
        {
            swap(j, a);
            return {j, true};
        }
        swap(i, j);
        ++i;
        --j;

        while (true)
        {
            while (i <= j && less(i, a))
                ++i;
            while (i <= j && !less(j, a))
                --j;
            if (i > j)
                break;
            swap(i, j);
            ++i;
            --j;
        }
        swap(j, a);
        return {j, false};
    }

    size_t partitionEqual(size_t a, size_t b, size_t pivot)
    {
        swap(a, pivot);
        size_t i = a + 1;
        size_t j = b - 1;

        while (true)
        {
            while (i <= j && !less(a, i))
                ++i;
            while (i <= j && less(a, j))
                --j;
            if (i > j)
                break;
            swap(i, j);
            ++i;
            --j;
        }
        return i;
    }

    bool partialInsertionSort(size_t a, size_t b)
    {
        constexpr size_t max_steps = 5;
        constexpr size_t shortest_shifting = 50;
        size_t i = a + 1;
        for (size_t step = 0; step < max_steps; ++step)
        {
            while (i < b && !less(i, i - 1))
                ++i;

            if (i == b)
                return true;

            if (b - a < shortest_shifting)
                return false;

            swap(i, i - 1);

            /// Go's code shifts down to index 1, not to `a`; kept as is.
            if (i - a >= 2)
            {
                for (size_t j = i - 1; j >= 1; --j)
                {
                    if (!less(j, j - 1))
                        break;
                    swap(j, j - 1);
                }
            }
            if (b - i >= 2)
            {
                for (size_t j = i + 1; j < b; ++j)
                {
                    if (!less(j, j - 1))
                        break;
                    swap(j, j - 1);
                }
            }
        }
        return false;
    }

    void breakPatterns(size_t a, size_t b)
    {
        size_t length = b - a;
        if (length < 8)
            return;

        UInt64 random = length;
        UInt64 modulus = UInt64{1} << std::bit_width(length);
        for (size_t idx = a + (length / 4) * 2 - 1; idx <= a + (length / 4) * 2 + 1; ++idx)
        {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            size_t other = random & (modulus - 1);
            if (other >= length)
                other -= length;
            swap(idx, a + other);
        }
    }

    void order2(size_t & a, size_t & b, size_t & swaps) const
    {
        if (less(b, a))
        {
            ++swaps;
            std::swap(a, b);
        }
    }

    size_t median(size_t a, size_t b, size_t c, size_t & swaps) const
    {
        order2(a, b, swaps);
        order2(b, c, swaps);
        order2(a, b, swaps);
        return b;
    }

    std::pair<size_t, Hint> choosePivot(size_t a, size_t b)
    {
        constexpr size_t shortest_ninther = 50;
        constexpr size_t max_swaps = 4 * 3;

        size_t l = b - a;
        size_t swaps = 0;
        size_t i = a + l / 4 * 1;
        size_t j = a + l / 4 * 2;
        size_t k = a + l / 4 * 3;

        if (l >= 8)
        {
            if (l >= shortest_ninther)
            {
                i = median(i - 1, i, i + 1, swaps);
                j = median(j - 1, j, j + 1, swaps);
                k = median(k - 1, k, k + 1, swaps);
            }
            j = median(i, j, k, swaps);
        }

        if (swaps == 0)
            return {j, Hint::Increasing};
        if (swaps == max_swaps)
            return {j, Hint::Decreasing};
        return {j, Hint::Unknown};
    }
};

}

/// Function timeSeriesSortGroupsByTags(groups, tag_names, descending[, ranks]) orders groups like Prometheus's
/// sort_by_label() (or sort_by_label_desc() if `descending` is true).
class FunctionTimeSeriesSortGroupsByTags final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesSortGroupsByTags";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesSortGroupsByTags>(context); }
    explicit FunctionTimeSeriesSortGroupsByTags(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    /// The result depends on the tags stored in the query context, so it's deterministic only in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isStateful() const override { return true; }
    bool isSuitableForConstantFolding() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with three or four arguments: {}(groups, tag_names, descending[, ranks])",
                            name, name);
        }

        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[0].type.get());
        if (!array_type || !isUInt64(array_type->getNestedType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #1 of function {} has wrong type {}, it must be {}",
                            name, arguments[0].type, "Array(UInt64)");

        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagNames(name, arguments, 1);

        if (!isBool(arguments[2].type) && !isUInt8(arguments[2].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #3 of function {} has wrong type {}, it must be {}",
                            name, arguments[2].type, "Bool");

        if (arguments.size() == 4)
        {
            const auto * ranks_type = typeid_cast<const DataTypeArray *>(arguments[3].type.get());
            const auto * rank_type = ranks_type ? typeid_cast<const DataTypeArray *>(ranks_type->getNestedType().get()) : nullptr;
            if (!rank_type || !isUInt64(rank_type->getNestedType()))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #4 of function {} has wrong type {}, it must be {}",
                                name, arguments[3].type, "Array(Array(UInt64))");
        }

        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        /// An empty name is allowed: as in Prometheus, it names a label no series has.
        auto tag_names = TimeSeriesTagsFunctionHelpers::extractConstStringsFromArgument(name, arguments, 1);
        bool descending = arguments[2].column->getBool(0);

        auto groups_column = arguments[0].column->convertToFullColumnIfConst();
        const auto & groups_array = assert_cast<const ColumnArray &>(*groups_column);
        const auto & groups = assert_cast<const ColumnUInt64 &>(groups_array.getData()).getData();
        const auto & offsets = groups_array.getOffsets();

        /// The input order of the groups: ranks[i] is the position of groups[i] fixed by an inner sort*() call.
        ColumnPtr ranks_column;
        const ColumnArray * ranks_array = nullptr;
        if (arguments.size() == 4)
        {
            ranks_column = arguments[3].column->convertToFullColumnIfConst();
            ranks_array = &assert_cast<const ColumnArray &>(*ranks_column);
            if (ranks_array->getOffsets() != offsets)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Arguments #1 and #4 of function {} must have equal sizes", name);
        }

        auto result_data = ColumnUInt64::create();
        result_data->reserve(groups.size());

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            VectorWithMemoryTracking<Group> row_groups(groups.begin() + offsets[row - 1], groups.begin() + offsets[row]);
            auto tags = tags_collector->getTagsByGroup(row_groups);

            /// The values of the tags to sort by; a missing tag is an empty string, as in Prometheus.
            VectorWithMemoryTracking<VectorWithMemoryTracking<std::string_view>> values(tags.size());
            for (size_t i = 0; i < tags.size(); ++i)
            {
                for (const auto & tag_name : tag_names)
                {
                    auto it = std::find_if(tags[i]->begin(), tags[i]->end(), [&](const auto & tag) { return tag.first == tag_name; });
                    values[i].push_back(it == tags[i]->end() ? std::string_view{} : std::string_view{it->second});
                }
            }

            auto cmp = [&](size_t a, size_t b)
            {
                for (size_t k = 0; k < tag_names.size(); ++k)
                {
                    if (values[a][k] == values[b][k])
                        continue;
                    bool less = naturalLess(values[a][k], values[b][k]);
                    return (less != descending) ? -1 : 1;
                }
                int c = compareTags(tags[a], tags[b]);
                return descending ? -c : c;
            };

            /// Prometheus sorts the vector in place: start from the ranks of an inner sort, or from storage order (by tags).
            VectorWithMemoryTracking<size_t> order(tags.size());
            std::iota(order.begin(), order.end(), 0);
            if (ranks_array)
            {
                const auto & rank_offsets = assert_cast<const ColumnArray &>(ranks_array->getData()).getOffsets();
                const auto & rank_data = assert_cast<const ColumnUInt64 &>(assert_cast<const ColumnArray &>(ranks_array->getData()).getData()).getData();
                size_t first = offsets[row - 1];
                auto rank = [&](size_t i)
                {
                    return std::span<const UInt64>(rank_data.data() + rank_offsets[first + i - 1], rank_data.data() + rank_offsets[first + i]);
                };
                std::sort(order.begin(), order.end(), [&](size_t a, size_t b)
                {
                    auto ra = rank(a);
                    auto rb = rank(b);
                    if (auto c = std::lexicographical_compare_three_way(ra.begin(), ra.end(), rb.begin(), rb.end()); c != 0)
                        return c < 0;
                    return compareTags(tags[a], tags[b]) < 0;
                });
            }
            else
                std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return compareTags(tags[a], tags[b]) < 0; });

            GoSort<decltype(cmp)>(order, cmp).sort();

            for (size_t i : order)
                result_data->insertValue(row_groups[i]);
        }

        return ColumnArray::create(std::move(result_data), groups_array.getOffsetsPtr());
    }

private:
    using Group = TimeSeriesTagsFunctionHelpers::Group;
    std::shared_ptr<const ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesSortGroupsByTags)
{
    FunctionDocumentation::Description description = R"(
Sorts an array of groups by the values of the specified tags in natural order, the way the PromQL functions
`sort_by_label()` and `sort_by_label_desc()` of Prometheus 3.5 do. Groups with equal tag values are ordered by all their tags.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesSortGroupsByTags(groups, tag_names, descending[, ranks])";
    FunctionDocumentation::Arguments arguments = {
        {"groups", "Groups of tags.", {"Array(UInt64)"}},
        {"tag_names", "Names of the tags to sort by. A missing tag counts as an empty string.", {"const Array(String)"}},
        {"descending", "Whether to sort in descending order.", {"const Bool"}},
        {"ranks", "Optional. The input order of the groups, as ranks compared lexicographically; by default the groups are first ordered by all their tags.", {"Array(Array(UInt64))"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the sorted groups.", {"Array(UInt64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT arrayMap(g -> timeSeriesExtractTag(g, 'host'),
    timeSeriesSortGroupsByTags(
        [timeSeriesTagsToGroup([('host', 'host10')]), timeSeriesTagsToGroup([('host', 'host9')]), timeSeriesTagsToGroup([('host', 'host1')])],
        ['host'], false)) AS hosts
        )",
        R"(
┌─hosts──────────────────────┐
│ ['host1','host9','host10'] │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesSortGroupsByTags>(documentation);
}

}
