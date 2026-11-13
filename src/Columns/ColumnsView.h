#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/defines.h>

#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <variant>

namespace DB
{

/// Non-owning borrowed view over source columns.
/// The referenced storage must outlive the call that receives this range.
/// Do not store `ColumnsView` or pointers obtained from it.
/// The view is expected to yield non-null `IColumn` pointers.
/// Only mappers passed to `filterProject` may return `nullptr`, which means skipping this source column.
class ColumnsView
{
    template <typename... Visitors>
    struct Overloaded : Visitors...
    {
        using Visitors::operator()...;
    };

    template <typename... Visitors>
    Overloaded(Visitors...) -> Overloaded<Visitors...>;

public:
    /// Mappers are evaluated lazily and can run multiple times while traversing nested views.
    /// They must be cheap and pure, and must not return pointers to temporary objects.
    using Mapper = const IColumn * (*)(const IColumn *, const void *);

    ColumnsView(const Columns & columns_) // NOLINT(google-explicit-constructor)
        : storage(ColumnPtrRange{columns_.data(), columns_.size()})
    {
    }

    ColumnsView(Columns &&) = delete;

    ColumnsView(const ColumnPtr & column_) // NOLINT(google-explicit-constructor)
        : storage(ColumnPtrRange{&column_, 1})
    {
    }

    ColumnsView(ColumnPtr &&) = delete;

    ColumnsView(const VectorWithMemoryTracking<ColumnPtr> & columns_) // NOLINT(google-explicit-constructor)
        : storage(ColumnPtrRange{columns_.data(), columns_.size()})
    {
    }

    ColumnsView(VectorWithMemoryTracking<ColumnPtr> &&) = delete;

    explicit ColumnsView(std::span<const ColumnPtr> columns_)
        : storage(ColumnPtrRange{columns_.data(), columns_.size()})
    {
    }

    ColumnsView(const ColumnRawPtrs & columns_) // NOLINT(google-explicit-constructor)
        : storage(RawPtrRange{columns_.data(), columns_.size()})
    {
    }

    ColumnsView(ColumnRawPtrs &&) = delete;

    explicit ColumnsView(std::span<const IColumn * const> columns_)
        : storage(RawPtrRange{columns_.data(), columns_.size()})
    {
    }

    ColumnsView(const IColumn * column_) // NOLINT(google-explicit-constructor)
        : storage(SingleRawPtr{column_})
    {
    }

    /// The mapper must return a non-null `IColumn` pointer.
    /// Use `filterProject` when no mapped column exists for some source columns.
    ColumnsView project(Mapper mapper_, const void * context_ = nullptr) const &
    {
        return ColumnsView(ProjectedRange<false>{this, mapper_, context_});
    }

    ColumnsView project(Mapper, const void * = nullptr) const && = delete;

    /// Like `project`, but skips source columns for which mapper returns `nullptr`.
    /// Every traversal calls the mapper for every source column. To select the children of the source columns by key for
    /// many keys (one `filterProject` per key costs keys * sources mapper calls), use `groupColumns` instead.
    ColumnsView filterProject(Mapper mapper_, const void * context_ = nullptr) const &
    {
        return ColumnsView(ProjectedRange<true>{this, mapper_, context_});
    }

    ColumnsView filterProject(Mapper, const void * = nullptr) const && = delete;

    template <typename Callback>
    bool forEach(Callback && callback) const
    {
        if (const auto * range = std::get_if<ColumnPtrRange>(&storage))
        {
            for (size_t i = 0; i != range->num_columns; ++i)
            {
                if (!invokeCallback(callback, range->columns[i].get()))
                    return false;
            }
            return true;
        }

        if (const auto * range = std::get_if<RawPtrRange>(&storage))
        {
            for (size_t i = 0; i != range->num_columns; ++i)
            {
                if (!invokeCallback(callback, range->columns[i]))
                    return false;
            }
            return true;
        }

        if (const auto * range = std::get_if<SingleRawPtr>(&storage))
            return invokeCallback(callback, range->column);

        using CallbackType = std::remove_reference_t<Callback>;
        return forEachImpl(
            [](const IColumn * column, void * context)
            { return ColumnsView::invokeCallback(*static_cast<CallbackType *>(context), column); },
            const_cast<void *>(static_cast<const void *>(std::addressof(callback))));
    }

    std::optional<const IColumn *> tryGetSingle() const
    {
        return std::visit(
            Overloaded{
                [](const ColumnPtrRange & range) -> std::optional<const IColumn *>
                {
                    if (range.num_columns != 1)
                        return std::nullopt;
                    return range.columns[0].get();
                },
                [](const RawPtrRange & range) -> std::optional<const IColumn *>
                {
                    if (range.num_columns != 1)
                        return std::nullopt;
                    return range.columns[0];
                },
                [](const SingleRawPtr & range) -> std::optional<const IColumn *> { return range.column; },
                [](const ProjectedRange<false> & range) -> std::optional<const IColumn *>
                {
                    std::optional<const IColumn *> base_column = range.base->tryGetSingle();
                    if (!base_column)
                        return std::nullopt;
                    return range.mapper(*base_column, range.context);
                },
                [&](const ProjectedRange<true> &) -> std::optional<const IColumn *>
                {
                    std::optional<const IColumn *> result;
                    bool found_more = false;

                    forEach(
                        [&](const IColumn * current)
                        {
                            if (!result)
                            {
                                result = current;
                                return true;
                            }

                            found_more = true;
                            return false;
                        });

                    if (result && !found_more)
                        return result;

                    return std::nullopt;
                },
            },
            storage);
    }

    size_t size() const
    {
        return std::visit(
            Overloaded{
                [](const ColumnPtrRange & range) -> size_t { return range.num_columns; },
                [](const RawPtrRange & range) -> size_t { return range.num_columns; },
                [](const SingleRawPtr &) -> size_t { return 1; },
                [](const ProjectedRange<false> & range) -> size_t { return range.base->size(); },
                [&](const ProjectedRange<true> &) -> size_t
                {
                    size_t num_columns = 0;
                    forEach([&](const IColumn *) { ++num_columns; });
                    return num_columns;
                },
            },
            storage);
    }

    bool empty() const
    {
        return std::visit(
            Overloaded{
                [](const ColumnPtrRange & range) -> bool { return range.num_columns == 0; },
                [](const RawPtrRange & range) -> bool { return range.num_columns == 0; },
                [](const SingleRawPtr &) -> bool { return false; },
                [](const ProjectedRange<false> & range) -> bool { return range.base->empty(); },
                [&](const ProjectedRange<true> &) -> bool
                {
                    bool is_empty = true;
                    forEach(
                        [&](const IColumn *)
                        {
                            is_empty = false;
                            return false;
                        });
                    return is_empty;
                },
            },
            storage);
    }

private:
    struct ColumnPtrRange
    {
        const ColumnPtr * columns = nullptr;
        size_t num_columns = 0;
    };

    struct RawPtrRange
    {
        const IColumn * const * columns = nullptr;
        size_t num_columns = 0;
    };

    struct SingleRawPtr
    {
        const IColumn * column = nullptr;
    };

    template <bool is_filtered>
    struct ProjectedRange
    {
        static constexpr bool filtered = is_filtered;

        const ColumnsView * base;
        Mapper mapper;
        const void * context;
    };

    using Storage = std::variant<RawPtrRange, ColumnPtrRange, SingleRawPtr, ProjectedRange<false>, ProjectedRange<true>>;

    template <bool is_filtered>
    explicit ColumnsView(ProjectedRange<is_filtered> projected_)
        : storage(projected_)
    {
    }

    using ForEachCallback = bool (*)(const IColumn *, void *);

    template <typename Callback>
    static bool invokeCallback(Callback & callback, const IColumn * column)
    {
        if constexpr (std::is_void_v<std::invoke_result_t<Callback &, const IColumn *>>)
        {
            callback(column);
            return true;
        }
        else
        {
            return callback(column);
        }
    }

    bool forEachImpl(ForEachCallback callback, void * callback_context) const
    {
        struct ProjectedCallbackContext
        {
            Mapper mapper;
            const void * mapper_context;
            ForEachCallback callback;
            void * callback_context;
        };

        return std::visit(
            Overloaded{
                [&](const ColumnPtrRange & range) -> bool
                {
                    for (size_t i = 0; i != range.num_columns; ++i)
                    {
                        if (!callback(range.columns[i].get(), callback_context))
                            return false;
                    }
                    return true;
                },
                [&](const RawPtrRange & range) -> bool
                {
                    for (size_t i = 0; i != range.num_columns; ++i)
                    {
                        if (!callback(range.columns[i], callback_context))
                            return false;
                    }
                    return true;
                },
                [&](const SingleRawPtr & range) -> bool { return callback(range.column, callback_context); },
                [&](const ProjectedRange<false> & range) -> bool
                {
                    ProjectedCallbackContext context{range.mapper, range.context, callback, callback_context};

                    return range.base->forEachImpl(
                        [](const IColumn * column, void * opaque_context)
                        {
                            const auto & typed_context = *static_cast<const ProjectedCallbackContext *>(opaque_context);
                            return typed_context.callback(
                                typed_context.mapper(column, typed_context.mapper_context), typed_context.callback_context);
                        },
                        &context);
                },
                [&](const ProjectedRange<true> & range) -> bool
                {
                    ProjectedCallbackContext context{range.mapper, range.context, callback, callback_context};

                    return range.base->forEachImpl(
                        [](const IColumn * column, void * opaque_context)
                        {
                            const auto & typed_context = *static_cast<const ProjectedCallbackContext *>(opaque_context);
                            const IColumn * mapped_column = typed_context.mapper(column, typed_context.mapper_context);
                            if (!mapped_column)
                                return true;
                            return typed_context.callback(mapped_column, typed_context.callback_context);
                        },
                        &context);
                },
            },
            storage);
    }

    Storage storage;
};

/// Children of source columns grouped by key, for example the columns of every `JSON` path in the source `JSON` columns.
/// Unlike `ColumnsView`, it owns its storage: every group is a `ColumnsView` into one buffer of this object.
/// The source columns must outlive this object, and the views must not outlive it. Built by `groupColumns`.
class GroupedColumns
{
public:
    /// The number of groups.
    size_t size() const { return group_offsets.size() - 1; }

    /// The columns of `group`, in the order of the source columns.
    ColumnsView operator[](size_t group) const &
    {
        chassert(group < size());
        return ColumnsView(
            std::span<const IColumn * const>(columns.data() + group_offsets[group], group_offsets[group + 1] - group_offsets[group]));
    }

    ColumnsView operator[](size_t) const && = delete;

private:
    explicit GroupedColumns(size_t num_groups)
        : group_offsets(num_groups + 1, 0)
    {
    }

    template <typename Children>
    friend GroupedColumns groupColumns(const ColumnsView & source_columns, size_t num_groups, Children && children);

    /// The columns of group `i` are in [group_offsets[i], group_offsets[i + 1]).
    VectorWithMemoryTracking<const IColumn *> columns;
    VectorWithMemoryTracking<size_t> group_offsets;
};

/// Groups the children of the source columns by key in one pass over the children that exist.
/// `children(source_column, emit)` must call `emit(group, child)` for every child of `source_column` that belongs to a
/// group, with `group` < `num_groups` and non-null `child`. A source column may have children in any number of groups.
/// Every group keeps the order of the source columns.
///
/// Use it instead of one `filterProject` per key when the children are selected by key (e.g. by name in a hash map)
/// for many keys: that costs keys * sources lookups, mostly misses when every source has only a few of the keys,
/// while grouping costs one lookup per existing child.
template <typename Children>
GroupedColumns groupColumns(const ColumnsView & source_columns, size_t num_groups, Children && children)
{
    GroupedColumns result(num_groups);
    if (num_groups == 0)
        return result;

    /// Collect the children with their groups in one pass, so that `children` (usually a lookup per child) runs once
    /// per child, and then put them into the groups with a stable counting sort.
    VectorWithMemoryTracking<std::pair<size_t, const IColumn *>> children_with_groups;
    /// Usually every group has a child in at least one source column.
    children_with_groups.reserve(num_groups);
    source_columns.forEach(
        [&](const IColumn * source_column)
        {
            children(
                source_column,
                [&](size_t group, const IColumn * child)
                {
                    chassert(group < num_groups);
                    chassert(child);
                    children_with_groups.emplace_back(group, child);
                });
        });

    /// Count the children of every group, then turn the counts into the ends of the groups.
    auto & offsets = result.group_offsets;
    for (const auto & [group, _] : children_with_groups)
        ++offsets[group];
    for (size_t group = 1; group != num_groups; ++group)
        offsets[group] += offsets[group - 1];
    offsets[num_groups] = children_with_groups.size();

    /// Fill backwards, so that every group keeps the order of the source columns and every offset ends at the start of its group.
    result.columns.resize(children_with_groups.size());
    for (auto it = children_with_groups.rbegin(); it != children_with_groups.rend(); ++it)
        result.columns[--offsets[it->first]] = it->second;

    return result;
}
}
