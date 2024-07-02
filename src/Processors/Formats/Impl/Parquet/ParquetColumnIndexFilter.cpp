
#include <memory>
#include <Processors/Formats/Impl/Parquet/ParquetColumnIndexFilter.h>
#include "ParquetColumnReader.h"
#include "RowRanges.h"

#if USE_PARQUET
#include <ranges>
#include <Interpreters/misc.h>
#include <Processors/Formats/Impl/Parquet/ParquetConverter.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <arrow/io/memory.h>
#include <arrow/util/int_util_overflow.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int PARQUET_EXCEPTION;
}
struct BoundaryOrder;
struct UnorderedBoundary;
struct AscendingBoundary;
struct DescendingBoundary;

template <typename DType>
class TypedComparator;

struct NoneNullParquetIndexsBuilder
{
    explicit NoneNullParquetIndexsBuilder(const std::vector<Int32> & non_null_page_indices_) : non_null_page_indices(non_null_page_indices_) { }
    const std::vector<Int32> & non_null_page_indices;

    ParquetIndexs all() const { return non_null_page_indices; }
    ParquetIndexs range(const size_t from, const size_t to) const
    {
        const auto begin = non_null_page_indices.begin() + from;
        const auto end = non_null_page_indices.begin() + to;
        return ParquetIndexs{begin, end};
    }

    template <typename Predict>
    ParquetIndexs filter(size_t from, const size_t to, Predict predict) const
    {
        ParquetIndexs pages;
        for (; from != to; ++from)
            if (predict(from))
                pages.emplace_back(non_null_page_indices[from]);
        return pages;
    }

    template <typename Predict>
    ParquetIndexs filter(Predict predict) const
    {
        return filter(0, non_null_page_indices.size(), predict);
    }
};

class EmptyColumnIndex final : public ParquetColumnIndex
{
public:
    const parquet::OffsetIndex & offsetIndex() const override { return *offset_index; }
    bool hasParquetColumnIndex() const override { return false; }

    ParquetIndexs notEq(const Field &) const override { abort(); }
    ParquetIndexs eq(const Field &) const override { abort(); }
    ParquetIndexs gt(const Field &) const override { abort(); }
    ParquetIndexs gtEg(const Field &) const override { abort(); }
    ParquetIndexs lt(const Field &) const override { abort(); }
    ParquetIndexs ltEg(const Field &) const override { abort(); }
    ParquetIndexs in(const ColumnPtr &) const override { abort(); }

    explicit EmptyColumnIndex(const std::shared_ptr<parquet::OffsetIndex> & offset_index_) : offset_index(offset_index_) { }

private:
    std::shared_ptr<parquet::OffsetIndex> offset_index;
};

template <typename T, typename Base>
concept Derived = std::is_base_of_v<Base, T>;

template <typename DType, Derived<BoundaryOrder> ORDER>
class TypedColumnIndexImpl final : public TypedColumnIndex<DType>
{
    const parquet::ColumnDescriptor * descr_;
    std::shared_ptr<parquet::TypedColumnIndex<DType>> column_index;
    std::shared_ptr<parquet::OffsetIndex> offset_index;
    std::shared_ptr<parquet::TypedComparator<DType>> comparator;

public:
    using T = typename DType::c_type;

    TypedColumnIndexImpl(
        const parquet::ColumnDescriptor * descr,
        const std::shared_ptr<parquet::TypedColumnIndex<DType>> & column_index_,
        const std::shared_ptr<parquet::OffsetIndex> & offset_index_)
        : descr_(descr), column_index(column_index_), offset_index(offset_index_), comparator(parquet::MakeComparator<DType>(descr))
    {
    }
    ParquetIndexs notEq(const Field & value) const override;
    ParquetIndexs eq(const Field & value) const override;
    ParquetIndexs gt(const Field & value) const override;
    ParquetIndexs gtEg(const Field & value) const override;
    ParquetIndexs lt(const Field & value) const override;
    ParquetIndexs ltEg(const Field & value) const override;
    ParquetIndexs in(const ColumnPtr & column) const override;

    const parquet::OffsetIndex & offsetIndex() const override { return *offset_index; }
    bool hasParquetColumnIndex() const override { return true; }
};

/*
 * A class containing the value to be compared to the min/max values. This way we only need to do the deboxing once
 * per predicate execution instead for every comparison.
 */
template <typename DType>
class TypedComparator
{
    using T = typename DType::c_type;
    const T & value;
    const std::vector<T> & min;
    const std::vector<T> & max;
    const std::vector<Int32> & non_null_page_indices;
    parquet::TypedComparator<DType> & comparator;
    friend UnorderedBoundary;
    friend AscendingBoundary;
    friend DescendingBoundary;

public:
    TypedComparator(const T & value_, const parquet::TypedColumnIndex<DType> & index, parquet::TypedComparator<DType> & comparator_)
        : value(value_)
        , min(index.min_values())
        , max(index.max_values())
        , non_null_page_indices(index.non_null_page_indices())
        , comparator(comparator_)
    {
    }
    size_t size() const { return non_null_page_indices.size(); }
    Int32 compareValueToMin(size_t non_null_page_index) const
    {
        const T & min_ = min[non_null_page_indices[non_null_page_index]];
        return comparator.Compare(value, min_) ? -1 : comparator.Compare(min_, value) ? 1 : 0;
    }
    Int32 compareValueToMax(size_t non_null_page_index) const
    {
        const T & max_ = max[non_null_page_indices[non_null_page_index]];
        return comparator.Compare(value, max_) ? -1 : comparator.Compare(max_, value) ? 1 : 0;
    }
};

struct Bounds
{
    const size_t lower;
    const size_t upper;

    Bounds(const size_t l, const size_t u) : lower(l), upper(u) { }
};

struct BoundaryOrder
{
    /// Avoid the possible overflow might happen in case of (left + right) / 2
    static Int32 floorMid(const Int32 lower, const Int32 upper) { return lower + (upper - lower) / 2; }

    /// Avoid the possible overflow might happen in case of (left + right + 1) / 2
    static Int32 ceilingMid(const Int32 lower, const Int32 upper) { return lower + (upper - lower + 1) / 2; }
};

struct UnorderedBoundary : BoundaryOrder
{
    template <typename DType>
    static ParquetIndexs eq(const TypedComparator<DType> & comparator)
    {
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.filter(
            [&](const size_t i) { return comparator.compareValueToMin(i) >= 0 && comparator.compareValueToMax(i) <= 0; });
    }

    template <typename DType>
    static ParquetIndexs gt(const TypedComparator<DType> & comparator)
    {
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.filter([&](const size_t i)
                                                                                   { return comparator.compareValueToMax(i) < 0; });
    }

    template <typename DType>
    static ParquetIndexs gtEq(const TypedComparator<DType> & comparator)
    {
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.filter([&](const size_t i)
                                                                                   { return comparator.compareValueToMax(i) <= 0; });
    }

    template <typename DType>
    static ParquetIndexs lt(const TypedComparator<DType> & comparator)
    {
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.filter([&](const size_t i)
                                                                                   { return comparator.compareValueToMin(i) > 0; });
    }

    template <typename DType>
    static ParquetIndexs ltEq(const TypedComparator<DType> & comparator)
    {
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.filter([&](const size_t i)
                                                                                   { return comparator.compareValueToMin(i) >= 0; });
    }

    template <typename DType>
    static ParquetIndexs notEq(const TypedComparator<DType> & comparator)
    {
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.filter(
            [&](const size_t i) { return comparator.compareValueToMin(i) != 0 || comparator.compareValueToMax(i) != 0; });
    }
};

struct AscendingBoundary : BoundaryOrder
{
    template <typename DType>
    static std::optional<Bounds> findBounds(const TypedComparator<DType> & comparator)
    {
        const Int32 length = static_cast<Int32>(comparator.size());
        if (!length)
            return std::nullopt;
        Int32 lower_left = 0;
        Int32 upper_left = 0;
        auto lower_right = length - 1;
        auto upper_right = length - 1;

        do
        {
            if (lower_left > lower_right)
                return std::nullopt;

            auto i = floorMid(lower_left, lower_right);
            if (comparator.compareValueToMin(i) < 0)
                lower_right = upper_right = i - 1;
            else if (comparator.compareValueToMax(i) > 0)
                lower_left = upper_left = i + 1;
            else
                lower_right = upper_left = i;
        } while (lower_left != lower_right);

        do
        {
            if (upper_left > upper_right)
                return std::nullopt;

            auto i = ceilingMid(upper_left, upper_right);
            if (comparator.compareValueToMin(i) < 0)
                upper_right = i - 1;
            else if (comparator.compareValueToMax(i) > 0)
                upper_left = i + 1;
            else
                upper_left = i;
        } while (upper_left != upper_right);

        return Bounds(static_cast<size_t>(lower_left), static_cast<size_t>(upper_right));
    }

    template <typename DType>
    static ParquetIndexs eq(const TypedComparator<DType> & comparator)
    {
        const NoneNullParquetIndexsBuilder builder(comparator.non_null_page_indices);
        return findBounds(comparator)
            .transform([&](const Bounds & b) { return builder.range(b.lower, b.upper + 1); })
            .value_or(ParquetIndexs{});
    }

    template <typename DType>
    static ParquetIndexs gt(const TypedComparator<DType> & comparator)
    {
        const Int32 length = static_cast<Int32>(comparator.size());
        if (!length)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = 0;
        Int32 right = length;
        do
        {
            auto i = floorMid(left, right);
            if (comparator.compareValueToMax(i) >= 0)
                left = i + 1;
            else
                right = i;
        } while (left < right);
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.range(static_cast<size_t>(right), static_cast<size_t>(length));
    }

    template <typename DType>
    static ParquetIndexs gtEq(const TypedComparator<DType> & comparator)
    {
        const Int32 length = static_cast<Int32>(comparator.size());
        if (!length)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = 0;
        Int32 right = length;
        do
        {
            auto i = floorMid(left, right);
            if (comparator.compareValueToMax(i) > 0)
                left = i + 1;
            else
                right = i;
        } while (left < right);
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.range(static_cast<size_t>(right), static_cast<size_t>(length));
    }

    template <typename DType>
    static ParquetIndexs lt(const TypedComparator<DType> & comparator)
    {
        const Int32 length = static_cast<Int32>(comparator.size());
        if (!length)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = -1;
        Int32 right = length - 1;
        do
        {
            auto i = ceilingMid(left, right);
            if (comparator.compareValueToMin(i) <= 0)
                right = i - 1;
            else
                left = i;
        } while (left < right);

        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.range(0, static_cast<size_t>(left + 1));
    }

    template <typename DType>
    static ParquetIndexs ltEq(const TypedComparator<DType> & comparator)
    {
        const Int32 length = static_cast<Int32>(comparator.size());
        if (!length)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = -1;
        Int32 right = length - 1;
        do
        {
            Int32 i = ceilingMid(left, right);
            if (comparator.compareValueToMin(i) < 0)
                right = i - 1;
            else
                left = i;
        } while (left < right);
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.range(0, static_cast<size_t>(left + 1));
    }

    template <typename DType>
    static ParquetIndexs notEq(const TypedComparator<DType> & comparator)
    {
        NoneNullParquetIndexsBuilder builder{comparator.non_null_page_indices};
        return findBounds(comparator)
            .transform(
                [&](const Bounds & b)
                {
                    return builder.filter(
                        [&](const size_t i)
                        {
                            return i < b.lower || i > b.upper || comparator.compareValueToMin(i) != 0
                                || comparator.compareValueToMax(i) != 0;
                        });
                })
            .value_or(builder.all());
    }
};

struct DescendingBoundary : BoundaryOrder
{
    template <typename DType>
    static std::optional<Bounds> findBounds(const TypedComparator<DType> & comparator)
    {
        const Int32 length = static_cast<Int32>(comparator.size());
        Int32 lower_left = 0;
        Int32 upper_left = 0;
        Int32 lower_right = length - 1;
        Int32 upper_right = length - 1;

        do
        {
            if (lower_left > lower_right)
                return std::nullopt;
            Int32 i = static_cast<Int32>(std::floor((lower_left + lower_right) / 2));
            if (comparator.compareValueToMax(i) > 0)
                lower_right = upper_right = i - 1;
            else if (comparator.compareValueToMin(i) < 0)
                lower_left = upper_left = i + 1;
            else
                lower_right = upper_left = i;
        } while (lower_left != lower_right);

        do
        {
            if (upper_left > upper_right)
                return std::nullopt;
            Int32 i = static_cast<Int32>(std::ceil((upper_left + upper_right) / 2));
            if (comparator.compareValueToMax(i) > 0)
                upper_right = i - 1;
            else if (comparator.compareValueToMin(i) < 0)
                upper_left = i + 1;
            else
                upper_left = i;
        } while (upper_left != upper_right);

        return Bounds(static_cast<size_t>(lower_left), static_cast<size_t>(upper_right));
    }

    template <typename DType>
    static ParquetIndexs eq(const TypedComparator<DType> & comparator)
    {
        const NoneNullParquetIndexsBuilder builder{comparator.non_null_page_indices};
        return findBounds(comparator)
            .transform([&](const Bounds & b) { return builder.range(b.lower, b.upper + 1); })
            .value_or(ParquetIndexs{});
    }
    template <typename DType>
    static ParquetIndexs gt(const TypedComparator<DType> & comparator)
    {
        const Int32 length = static_cast<Int32>(comparator.size());
        if (!length)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = -1;
        Int32 right = length - 1;
        do
        {
            Int32 i = ceilingMid(left, right);
            if (comparator.compareValueToMax(i) >= 0)
                right = i - 1;
            else
                left = i;
        } while (left < right);
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.range(0, static_cast<size_t>(left + 1));
    }

    template <typename DType>
    static ParquetIndexs gtEq(const TypedComparator<DType> & comparator)
    {
        const Int32 length = static_cast<Int32>(comparator.size());
        if (!length)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = -1;
        Int32 right = length - 1;
        do
        {
            Int32 i = ceilingMid(left, right);
            if (comparator.compareValueToMax(i) > 0)
                right = i - 1;
            else
                left = i;
        } while (left < right);
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.range(0, static_cast<size_t>(left + 1));
    }

    template <typename DType>
    static ParquetIndexs lt(const TypedComparator<DType> & comparator)
    {
        const Int32 length = static_cast<Int32>(comparator.size());
        if (!length)
        {
            // No matching rows if the column index contains null pages only
            return ParquetIndexs{};
        }
        Int32 left = 0;
        Int32 right = length;
        do
        {
            Int32 i = floorMid(left, right);
            if (comparator.compareValueToMin(i) <= 0)
                left = i + 1;
            else
                right = i;
        } while (left < right);
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.range(static_cast<size_t>(right), static_cast<size_t>(length));
    }

    template <typename DType>
    static ParquetIndexs ltEq(const TypedComparator<DType> & comparator)
    {
        const Int32 length = static_cast<Int32>(comparator.size());
        if (!length)
        {
            // No matching rows if the column index contains null pages only
            return ParquetIndexs{};
        }
        Int32 left = 0;
        Int32 right = length;
        do
        {
            Int32 i = floorMid(left, right);
            if (comparator.compareValueToMin(i) < 0)
                left = i + 1;
            else
                right = i;
        } while (left < right);
        return NoneNullParquetIndexsBuilder{comparator.non_null_page_indices}.range(static_cast<size_t>(right), static_cast<size_t>(length));
    }
    template <typename DType>
    static ParquetIndexs notEq(const TypedComparator<DType> & comparator)
    {
        const NoneNullParquetIndexsBuilder builder{comparator.non_null_page_indices};
        return findBounds(comparator)
            .transform(
                [&](const Bounds & b)
                {
                    return builder.filter(
                        [&](const size_t i)
                        {
                            return i < b.lower || i > b.upper || comparator.compareValueToMin(i) != 0
                                || comparator.compareValueToMin(i) != 0;
                        });
                })
            .value_or(builder.all());
    }
};

/// TODO: bencnmark
template <typename DType, Derived<BoundaryOrder> ORDER>
ParquetIndexs TypedColumnIndexImpl<DType, ORDER>::notEq(const Field & value) const
{
    if (value.isNull())
    {
        return ParquetIndexsBuilder::filter(
            column_index->null_pages().size(), [&](const size_t i) { return !column_index->null_pages()[i]; });
    }
    if (!column_index->has_null_counts())
    {
        // Nulls match so if we don't have null related statistics we have to return all pages
        return {ParquetIndexsBuilder::ALL_PAGES};
    }

    // Merging value filtering with pages containing nulls
    ToParquet<DType> to_parquet;
    auto real_value = to_parquet.as(value, *descr_);
    TypedComparator<DType> typed_comparator{real_value, *column_index, *comparator};
    auto pages = ORDER::notEq(typed_comparator);
    const std::set<size_t> matchingIndexes(pages.begin(), pages.end());

    return ParquetIndexsBuilder::filter(
        column_index->null_counts().size(),
        [&](const size_t i) { return column_index->null_counts()[i] > 0 || matchingIndexes.contains(i); });
}

template <typename DType, Derived<BoundaryOrder> ORDER>
ParquetIndexs TypedColumnIndexImpl<DType, ORDER>::eq(const Field & value) const
{
    if (value.isNull())
    {
        if (column_index->has_null_counts())
        {
            return ParquetIndexsBuilder::filter(
                column_index->null_counts().size(), [&](const size_t i) { return column_index->null_counts()[i] > 0; });
        }
        else
        {
            // Searching for nulls so if we don't have null related statistics we have to return all pages
            return {ParquetIndexsBuilder::ALL_PAGES};
        }
    }
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index, *comparator};
    return ORDER::eq(typed_comparator);
}

template <typename DType, Derived<BoundaryOrder> ORDER>
ParquetIndexs TypedColumnIndexImpl<DType, ORDER>::gt(const Field & value) const
{
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index, *comparator};
    return ORDER::gt(typed_comparator);
}

template <typename DType, Derived<BoundaryOrder> ORDER>
ParquetIndexs TypedColumnIndexImpl<DType, ORDER>::gtEg(const Field & value) const
{
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index, *comparator};
    return ORDER::gtEq(typed_comparator);
}

template <typename DType, Derived<BoundaryOrder> ORDER>
ParquetIndexs TypedColumnIndexImpl<DType, ORDER>::lt(const Field & value) const
{
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index, *comparator};
    return ORDER::lt(typed_comparator);
}

template <typename DType, Derived<BoundaryOrder> ORDER>
ParquetIndexs TypedColumnIndexImpl<DType, ORDER>::ltEg(const Field & value) const
{
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index, *comparator};
    return ORDER::ltEq(typed_comparator);
}

template <typename DType, Derived<BoundaryOrder> ORDER>
ParquetIndexs TypedColumnIndexImpl<DType, ORDER>::in(const ColumnPtr & column) const
{
    /// TDDO: handle null
    ///
    std::shared_ptr<ParquetConverter<DType>> converter = ParquetConverter<DType>::Make(column, *descr_);
    const auto * value = converter->getBatch(0, column->size());
    T min, max;
    std::tie(min, max) = comparator->GetMinMax(value, column->size());

    TypedComparator<DType> typed_comparator_min{min, *column_index, *comparator};
    ParquetIndexs min_page_indexs = ORDER::gtEq(typed_comparator_min);
    std::ranges::sort(min_page_indexs);


    TypedComparator<DType> typed_comparator_max{max, *column_index, *comparator};
    ParquetIndexs max_page_indexs = ORDER::ltEq(typed_comparator_max);
    std::ranges::sort(max_page_indexs);

    std::set<size_t> matchingIndex;
    std::ranges::set_intersection(min_page_indexs, max_page_indexs, std::inserter(matchingIndex, matchingIndex.begin()));

    return ParquetIndexsBuilder::filter(column_index->null_counts().size(), [&](const size_t i) { return matchingIndex.contains(i); });
}

template <typename T, typename S>
std::unique_ptr<T> dynamic_pointer_cast(std::unique_ptr<S> && p) noexcept
{
    if (T * const converted = dynamic_cast<T *>(p.get()))
    {
        // cast succeeded; clear input
        p.release();
        return std::unique_ptr<T>{converted};
    }
    // cast failed; leave input untouched
    throw Exception(
        ErrorCodes::PARQUET_EXCEPTION, "Bad cast from type {} to {}", demangle(typeid(S).name()), demangle(typeid(T).name()));
}

template <typename ORDER>
ParquetColumnIndexPtr internalMakeColumnIndex(
    const parquet::ColumnDescriptor * descr,
    const std::shared_ptr<parquet::ColumnIndex> & column_index,
    const std::shared_ptr<parquet::OffsetIndex> & offset_index)
{
    auto physical_type = descr->physical_type();
    switch (physical_type)
    {
        case parquet::Type::BOOLEAN:
            return std::make_unique<TypedColumnIndexImpl<parquet::BooleanType, ORDER>>(
                descr, dynamic_pointer_cast<parquet::BoolColumnIndex>(column_index), offset_index);
        case parquet::Type::INT32:
            return std::make_unique<TypedColumnIndexImpl<parquet::Int32Type, ORDER>>(
                descr, dynamic_pointer_cast<parquet::Int32ColumnIndex>(column_index), offset_index);
        case parquet::Type::INT64:
            return std::make_unique<TypedColumnIndexImpl<parquet::Int64Type, ORDER>>(
                descr, dynamic_pointer_cast<parquet::Int64ColumnIndex>(column_index), offset_index);
        case parquet::Type::INT96:
            break;
        case parquet::Type::FLOAT:
            return std::make_unique<TypedColumnIndexImpl<parquet::FloatType, ORDER>>(
                descr, dynamic_pointer_cast<parquet::FloatColumnIndex>(column_index), offset_index);
        case parquet::Type::DOUBLE:
            return std::make_unique<TypedColumnIndexImpl<parquet::DoubleType, ORDER>>(
                descr, dynamic_pointer_cast<parquet::DoubleColumnIndex>(column_index), offset_index);
        case parquet::Type::BYTE_ARRAY:
            return std::make_unique<TypedColumnIndexImpl<parquet::ByteArrayType, ORDER>>(
                descr, dynamic_pointer_cast<parquet::ByteArrayColumnIndex>(column_index), offset_index);
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            return std::make_unique<TypedColumnIndexImpl<parquet::FLBAType, ORDER>>(
                descr, dynamic_pointer_cast<parquet::FLBAColumnIndex>(column_index), offset_index);
        case parquet::Type::UNDEFINED:
            break;
    }
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported physical type {}", TypeToString(physical_type));
}

ParquetColumnIndexPtr ParquetColumnIndex::create(
    const parquet::ColumnDescriptor * descr,
    const std::shared_ptr<parquet::ColumnIndex> & column_index,
    const std::shared_ptr<parquet::OffsetIndex> & offset_index)
{
    if (!column_index)
        return std::make_unique<EmptyColumnIndex>(offset_index);

    const auto order = column_index->boundary_order();
    switch (order)
    {
        case parquet::BoundaryOrder::Unordered:
            return internalMakeColumnIndex<UnorderedBoundary>(descr, column_index, offset_index);
        case parquet::BoundaryOrder::Ascending:
            return internalMakeColumnIndex<AscendingBoundary>(descr, column_index, offset_index);
        case parquet::BoundaryOrder::Descending:
            return internalMakeColumnIndex<DescendingBoundary>(descr, column_index, offset_index);
        default:
            break;
    }
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported UNDEFINED BoundaryOrder: {}", order);
}

///
const ParquetColumnIndexFilter::AtomMap ParquetColumnIndexFilter::atom_map{
    {"notEquals",
     [](RPNElement & out, const Field & value)
     {
         out.function = RPNElement::FUNCTION_NOT_EQUALS;
         out.value = value;
         return true;
     }},
    {"equals",
     [](RPNElement & out, const Field & value)
     {
         out.function = RPNElement::FUNCTION_EQUALS;
         out.value = value;
         return true;
     }},
    {"less",
     [](RPNElement & out, const Field & value)
     {
         out.function = RPNElement::FUNCTION_LESS;
         out.value = value;
         return true;
     }},
    {"greater",
     [](RPNElement & out, const Field & value)
     {
         out.function = RPNElement::FUNCTION_GREATER;
         out.value = value;
         return true;
     }},
    {"lessOrEquals",
     [](RPNElement & out, const Field & value)
     {
         out.function = RPNElement::FUNCTION_LESS_OR_EQUALS;
         out.value = value;
         return true;
     }},
    {"greaterOrEquals",
     [](RPNElement & out, const Field & value)
     {
         out.function = RPNElement::FUNCTION_GREATER_OR_EQUALS;
         out.value = value;
         return true;
     }},
    {"in",
     [](RPNElement & out, const Field &)
     {
         out.function = RPNElement::FUNCTION_IN;
         return true;
     }},
    {"notIn",
     [](RPNElement & out, const Field &)
     {
         out.function = RPNElement::FUNCTION_NOT_IN;
         return true;
     }},
    {"isNotNull",
     [](RPNElement & out, const Field &)
     {
         /// Field's default constructor constructs a null value
         out.function = RPNElement::FUNCTION_NOT_EQUALS;
         return true;
     }},
    {"isNull",
     [](RPNElement & out, const Field &)
     {
         /// Field's default constructor constructs a null value
         out.function = RPNElement::FUNCTION_EQUALS;
         return true;
     }}};

ParquetColumnIndexFilter::ParquetColumnIndexFilter(std::shared_ptr<const KeyCondition> key_condition)
{
    /// If there is no filt condition, all rows will be read.
    if (!key_condition || !key_condition->getFilterActionsDAG())
        return;
    auto context = key_condition->getContext();
    const auto inverted_dag
        = DB::KeyCondition::cloneASTWithInversionPushDown({key_condition->getFilterActionsDAG()->getOutputs().at(0)}, context);

    assert(inverted_dag->getOutputs().size() == 1);

    const auto * inverted_dag_filter_node = inverted_dag->getOutputs()[0];

    DB::RPNBuilder<RPNElement> builder(
        inverted_dag_filter_node,
        std::move(context),
        [&](const DB::RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn = std::make_unique<RPN>(std::move(builder).extractRPN());
}

bool tryPrepareSetIndex(const RPNBuilderFunctionTreeNode & func, ParquetColumnIndexFilter::RPNElement & out)
{
    const auto right_arg = func.getArgumentAt(1);
    const auto future_set = right_arg.tryGetPreparedSet();
    if (future_set->getTypes().size() != 1)
        return false;

    if (!future_set)
        return false;

    const auto prepared_set = future_set->buildOrderedSetInplace(right_arg.getTreeContext().getQueryContext());
    if (!prepared_set)
        return false;

    /// The index can be prepared if the elements of the set were saved in advance.
    if (!prepared_set->hasExplicitSetElements())
        return false;

    const auto set_columns = prepared_set->getSetElements();
    assert(1 == set_columns.size());
    out.column = set_columns[0];
    out.columnName = func.getArgumentAt(0).getColumnName();
    return true;
}

bool ParquetColumnIndexFilter::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out)
{
    Field const_value;
    DataTypePtr const_type;
    if (node.isFunction())
    {
        const auto func = node.toFunctionNode();
        const std::string func_name = func.getFunctionName();
        if (!atom_map.contains(func_name))
            return false;

        const size_t num_args = func.getArgumentsSize();
        if (num_args == 1)
        {
            out.columnName = func.getArgumentAt(0).getColumnName();
        }
        else if (num_args == 2)
        {
            if (functionIsInOrGlobalInOperator(func_name))
            {
                if (!tryPrepareSetIndex(func, out))
                    return false;
            }
            else if (func.getArgumentAt(1).tryGetConstant(const_value, const_type))
            {
                /// If the const operand is null, the atom will be always false
                if (const_value.isNull())
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    return true;
                }
                out.columnName = func.getArgumentAt(0).getColumnName();
            }
            else
                return false;
        }
        else
            return false;

        if (out.columnName.empty())
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "`columnName` is empty. It is a bug.");

        const auto atom_it = atom_map.find(func_name);
        return atom_it->second(out, const_value);
    }
    else if (node.tryGetConstant(const_value, const_type))
    {
        /// For cases where it says, for example, `WHERE 0 AND something`
        if (const_value.getType() == Field::Types::UInt64)
        {
            out.function = const_value.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
        else if (const_value.getType() == Field::Types::Int64)
        {
            out.function = const_value.safeGet<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
        else if (const_value.getType() == Field::Types::Float64)
        {
            out.function = const_value.safeGet<Float64>() != 0.0 ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
    }
    return false;
}

RowRanges ParquetColumnIndexFilter::calculateRowRanges(const ParquetColumnIndexStore & index_store, size_t rowgroup_count) const
{
    if (!rpn)
    {
        // no filt condition
        return RowRanges::createSingle(rowgroup_count);
    }

    using LOGICAL_OP = RowRanges (*)(const RowRanges &, const RowRanges &);
    using OPERATOR = std::function<ParquetIndexs(const ParquetColumnIndex &, const RPNElement &)>;
    std::vector<RowRanges> rpn_stack;

    auto CALL_LOGICAL_OP = [&rpn_stack](const LOGICAL_OP & op)
    {
        assert(rpn_stack.size() >= 2);
        const auto arg1 = rpn_stack.back();
        rpn_stack.pop_back();
        const auto arg2 = rpn_stack.back();
        rpn_stack.back() = op(arg1, arg2);
    };

    auto CALL_OPERATOR = [&rpn_stack, &index_store, rowgroup_count](const RPNElement & element, const OPERATOR & callback)
    {
        const auto it = index_store.find(element.columnName);
        if (it != index_store.end() && it->second->hasParquetColumnIndex())
        {
            const ParquetColumnIndex & index = *it->second;
            const RowRangesBuilder rgbuilder(rowgroup_count, index.offsetIndex().page_locations());
            rpn_stack.emplace_back(rgbuilder.toRowRanges(callback(index, element)));
        }
        else
        {
            rpn_stack.emplace_back(RowRanges::createSingle(rowgroup_count));
            if (it == index_store.end())
            {
                LOG_WARNING(
                    getLogger("ParquetColumnIndexFilter"),
                    "Column {} not found in ParquetColumnIndexStore with Column name[{}], return all row ranges",
                    element.columnName,
                    fmt::join(index_store | std::ranges::views::transform([](const auto & kv) { return kv.first; }), ", "));
            }
        }
    };

    for (const auto & element : *rpn)
    {
        switch (element.function)
        {
            case RPNElement::FUNCTION_EQUALS:
                CALL_OPERATOR(element, [](const ParquetColumnIndex & index, const RPNElement & e) { return index.eq(e.value); });
                break;
            case RPNElement::FUNCTION_NOT_EQUALS:
                CALL_OPERATOR(element, [](const ParquetColumnIndex & index, const RPNElement & e) { return index.notEq(e.value); });
                break;
            case RPNElement::FUNCTION_LESS:
                CALL_OPERATOR(element, [](const ParquetColumnIndex & index, const RPNElement & e) { return index.lt(e.value); });
                break;
            case RPNElement::FUNCTION_GREATER:
                CALL_OPERATOR(element, [](const ParquetColumnIndex & index, const RPNElement & e) { return index.gt(e.value); });
                break;
            case RPNElement::FUNCTION_LESS_OR_EQUALS:
                CALL_OPERATOR(element, [](const ParquetColumnIndex & index, const RPNElement & e) { return index.ltEg(e.value); });
                break;
            case RPNElement::FUNCTION_GREATER_OR_EQUALS:
                CALL_OPERATOR(element, [](const ParquetColumnIndex & index, const RPNElement & e) { return index.gtEg(e.value); });
                break;
            case RPNElement::FUNCTION_IN:
                CALL_OPERATOR(element, [](const ParquetColumnIndex & index, const RPNElement & e) { return index.in(e.column); });
                break;
            case RPNElement::FUNCTION_NOT_IN:
                break;
            case RPNElement::FUNCTION_UNKNOWN:
                rpn_stack.emplace_back(RowRanges::createSingle(rowgroup_count));
                break;
            case RPNElement::FUNCTION_NOT:
                assert(false);
                break;
            case RPNElement::FUNCTION_AND:
                CALL_LOGICAL_OP(RowRanges::intersection);
                break;
            case RPNElement::FUNCTION_OR:
                CALL_LOGICAL_OP(RowRanges::unionRanges);
                break;
            case RPNElement::ALWAYS_FALSE:
                assert(false);
                break;
            case RPNElement::ALWAYS_TRUE:
                assert(false);
                break;
        }
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unexpected stack size in ParquetColumnIndexFilter::calculateRowRanges");

    return rpn_stack[0];
}

ParquetFileColumnIndexFilter::ParquetFileColumnIndexFilter(
    parquet::ParquetFileReader & file_reader_,
    size_t source_file_size_,
    const std::vector<Int32> & required_column_indices_,
    bool column_name_case_insenstive_,
    std::shared_ptr<const KeyCondition> key_condition_)
    : file_reader(file_reader_)
    , source_file_size(source_file_size_)
    , required_column_indices(required_column_indices_)
    , column_name_case_insenstive(column_name_case_insenstive_)
    , column_index_filter(std::make_unique<ParquetColumnIndexFilter>(key_condition_))
{
    const auto & schema = *file_reader.metadata()->schema();
    for (auto const column_index : required_column_indices)
    {
        auto col_desc = schema.Column(column_index);
        std::string column_name = col_desc->name();
        if (column_name_case_insenstive)
            boost::to_lower(column_name);
        col_name_to_index[column_name] = column_index;
    }
}

const RowRanges & ParquetFileColumnIndexFilter::calculateRowRanges(int row_group)
{
    auto it = row_group_row_ranges.find(row_group);
    if (it == row_group_row_ranges.end())
    {
        auto row_group_metadata = file_reader.metadata()->RowGroup(row_group);
        auto * column_index_store = getColumnIndexStore(row_group);
        if (!column_index_store)
        {
            auto row_ranges = RowRanges::createSingle(row_group_metadata->num_rows());
            auto [insert_it, _] = row_group_row_ranges.emplace(row_group, std::make_unique<RowRanges>(row_ranges.getRanges()));
            it = insert_it;
        }
        else
        {
            auto row_ranges = column_index_filter->calculateRowRanges(*column_index_store, row_group_metadata->num_rows());
            std::vector<RowRange> row_ranges_vector = row_ranges.getRanges();
            std::sort(row_ranges_vector.begin(), row_ranges_vector.end(), [](const auto & a, const auto & b) { return a.from <= b.from; });
            auto [insert_it, _] = row_group_row_ranges.emplace(row_group, std::make_unique<RowRanges>(row_ranges_vector));
            it = insert_it;
            LOG_ERROR(getLogger("ParquetFileColumnIndexFilter"), "xxxx ranges:{}", it->second->toString());
        }
    }
    return *(it->second);
}

ParquetColumnIndexStore * ParquetFileColumnIndexFilter::getColumnIndexStore(int row_group)
{
    auto it = row_group_column_index_stores.find(row_group);
    if (it == row_group_column_index_stores.end())
    {
        auto row_group_metadata = file_reader.metadata()->RowGroup(row_group);
        auto page_index_reader = file_reader.GetPageIndexReader()->RowGroup(row_group);
        if (!page_index_reader)
        {
            LOG_ERROR(getLogger("ParquetFileColumnIndexFilter"), "xxx not found page_index_reader");
            auto [insert_it, _] = row_group_column_index_stores.emplace(row_group, nullptr);
            it = insert_it;
        }
        else
        {
            LOG_ERROR(getLogger("ParquetFileColumnIndexFilter"), "xxx found page_index_reader");
            auto column_index_store = std::make_unique<ParquetColumnIndexStore>();
            column_index_store->reserve(required_column_indices.size());

            for (auto const column_index : required_column_indices)
            {
                const auto * col_desc = row_group_metadata->schema()->Column(column_index);
                const auto col_index = page_index_reader->GetColumnIndex(column_index);
                const auto offset_index = page_index_reader->GetOffsetIndex(column_index);
                std::string column_name = col_desc->name();
                if (column_name_case_insenstive)
                    boost::to_lower(column_name);
                column_index_store->emplace(column_name, ParquetColumnIndex::create(col_desc, col_index, offset_index));
            }
            auto [insert_it, _] = row_group_column_index_stores.emplace(row_group, std::move(column_index_store));
            it = insert_it;
        }
    }
    if (!it->second)
        return nullptr;
    return &*(it->second);
}

/// Compute the section of the file that should be read for the given
/// row group and column chunk.
static ::arrow::io::ReadRange computeColumnChunkRange(
    const parquet::FileMetaData & file_metadata, const parquet::ColumnChunkMetaData & column_metadata, const int64_t source_size)
{
    // For PARQUET-816
    static constexpr int64_t max_dict_header_size = 100;

    int64_t col_start = column_metadata.data_page_offset();
    if (column_metadata.has_dictionary_page() && column_metadata.dictionary_page_offset() > 0
        && col_start > column_metadata.dictionary_page_offset())
        col_start = column_metadata.dictionary_page_offset();

    int64_t col_length = column_metadata.total_compressed_size();
    int64_t col_end;
    if (col_start < 0 || col_length < 0)
        throw parquet::ParquetException("Invalid column metadata (corrupt file?)");

    if (arrow::internal::AddWithOverflow(col_start, col_length, &col_end) || col_end > source_size)
        throw parquet::ParquetException("Invalid column metadata (corrupt file?)");

    // PARQUET-816 workaround for old files created by older parquet-mr
    const parquet::ApplicationVersion & version = file_metadata.writer_version();
    if (version.VersionLt(parquet::ApplicationVersion::PARQUET_816_FIXED_VERSION()))
    {
        // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
        // dictionary page header size in total_compressed_size and total_uncompressed_size
        // (see IMPALA-694). We add padding to compensate.
        const int64_t bytes_remaining = source_size - col_end;
        const int64_t padding = std::min<int64_t>(max_dict_header_size, bytes_remaining);
        col_length += padding;
    }
    return {col_start, col_length};
}

static bool extend(arrow::io::ReadRange & read_range, const int64_t offset, const int64_t length)
{
    if (read_range.offset + read_range.length == offset)
    {
        read_range.length += length;
        return true;
    }
    return false;
}

static void emplaceReadRange(std::vector<arrow::io::ReadRange> & read_ranges, const int64_t offset, const int64_t length)
{
    if (read_ranges.empty() || !extend(read_ranges.back(), offset, length))
        read_ranges.emplace_back(arrow::io::ReadRange{offset, length});
}
static std::pair<std::vector<arrow::io::ReadRange>, ParquetColumnReadSequence> buildReadSequece(
    const int64_t row_group_rows,
    const arrow::io::ReadRange & chunk_range,
    const std::vector<parquet::PageLocation> & page_locations,
    const RowRanges & row_ranges)
{
    LOG_ERROR(getLogger("ParquetFileColumnIndexFilter"), "xxx pages: {}", page_locations.size());
    if (static_cast<size_t>(row_group_rows) == row_ranges.rowCount())
        return {{chunk_range}, ParquetColumnReadSequence{static_cast<Int32>(row_group_rows)}};
    auto skip = [](ParquetColumnReadSequence & read_sequence, const size_t number) -> void
    {
        if (read_sequence.empty() || read_sequence.back() > 0)
            read_sequence.push_back(-static_cast<Int32>(number));
        else
            read_sequence.back() -= number;
    };

    auto read = [](ParquetColumnReadSequence & read_sequence, const size_t number) -> void
    {
        if (read_sequence.empty() || read_sequence.back() < 0)
            read_sequence.push_back(static_cast<Int32>(number));
        else
            read_sequence.back() += number;
    };

    std::vector<arrow::io::ReadRange> chunk_ranges;
    // Add a range for dictionary page if required
    if (chunk_range.offset < page_locations[0].offset)
    {
        emplaceReadRange(chunk_ranges, chunk_range.offset, page_locations[0].offset - chunk_range.offset);
    }

    const RowRangesBuilder row_range_builder{row_group_rows, page_locations};
    std::vector<RowRange> page_row_ranges;
    const size_t page_size = page_locations.size();
    for (size_t page_index = 0; page_index < page_size; ++page_index)
    {
        size_t from = row_range_builder.firstRowIndex(page_index);
        size_t to = row_range_builder.lastRowIndex(page_index);
        if (row_ranges.isOverlapping(from, to))
        {
            emplaceReadRange(chunk_ranges, page_locations[page_index].offset, page_locations[page_index].compressed_page_size);
            page_row_ranges.push_back({from, to});
        }
    }

    ParquetColumnReadSequence read_sequence;

    auto row_range_begin = row_ranges.getRanges().begin();
    const auto row_range_end = row_ranges.getRanges().end();
    size_t row_index = row_range_begin->from;

    for (size_t i = 0; i < page_row_ranges.size() && row_range_begin != row_range_end; ++i)
    {
        const size_t last_row_index_in_page = page_row_ranges[i].to;
        size_t read_row_index_in_page = page_row_ranges[i].from;

        if (row_index <= last_row_index_in_page)
        {
            assert(row_index >= read_row_index_in_page);
            do
            {
                if (row_index > read_row_index_in_page)
                    skip(read_sequence, row_index - read_row_index_in_page);
                const size_t read_number = std::min(row_range_begin->to, last_row_index_in_page) - row_index + 1;
                read(read_sequence, read_number);
                row_index += read_number;
                read_row_index_in_page = row_index;

                /// we already read cuurent page, so we need to read next page.
                if (row_range_begin->to > last_row_index_in_page)
                {
                    assert(read_row_index_in_page > last_row_index_in_page);
                    break;
                }

                /// we already read current range so we need read next range
                ++row_range_begin;
                if (row_range_begin != row_range_end)
                    row_index = row_range_begin->from;
            } while (row_range_begin != row_range_end && row_index <= last_row_index_in_page);

            /// skip read remain records in current page.
            if (read_row_index_in_page <= last_row_index_in_page)
                skip(read_sequence, last_row_index_in_page - read_row_index_in_page + 1);
        }
        assert(!read_sequence.empty());
    }
    return {chunk_ranges, read_sequence};
}

std::pair<ParquetFileColumnIndexFilter::ReadRanges, ParquetColumnReadSequence>
ParquetFileColumnIndexFilter::calculateReadSequence(int row_group, const String & col_name_)
{
    String col_name = col_name_;
    if (column_name_case_insenstive)
        boost::to_lower(col_name);
    auto col_pos_it = col_name_to_index.find(col_name);
    if (col_pos_it == col_name_to_index.end())
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unknown column name: {}", col_name);
    auto col_pos = col_pos_it->second;

    auto file_metadata = file_reader.metadata();
    auto row_group_metadata = file_metadata->RowGroup(row_group);
    const auto column_metadata = row_group_metadata->ColumnChunk(col_pos);
    const auto col_range = computeColumnChunkRange(*file_metadata, *column_metadata, source_file_size);

    auto * col_index_store = getColumnIndexStore(row_group);
    if (!col_index_store)
    {
        LOG_ERROR(getLogger("xxx"), "xcxdfsdf");
        return {{col_range}, ParquetColumnReadSequence{static_cast<Int32>(row_group_metadata->num_rows())}};
    }
    const auto & col_index = *(col_index_store->find(col_name)->second);

    return buildReadSequece(row_group_metadata->num_rows(), col_range, col_index.offsetIndex().page_locations(), calculateRowRanges(row_group));
}

}
#endif //USE_PARQUET
