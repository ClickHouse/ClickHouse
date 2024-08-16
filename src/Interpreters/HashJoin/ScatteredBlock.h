#pragma once

#include <memory>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <base/defines.h>
#include <Common/PODArray.h>

#include <boost/math/distributions/fwd.hpp>
#include <boost/noncopyable.hpp>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace detail
{

class Selector
{
public:
    using Range = std::pair<size_t, size_t>;
    using Indexes = ColumnUInt64;
    using IndexesPtr = ColumnUInt64::MutablePtr;

    Selector() : Selector(0, 0) { }

    /// [begin, end)
    Selector(size_t begin, size_t end) : data(Range{begin, end}) { }

    explicit Selector(size_t size) : Selector(0, size) { }
    explicit Selector(IndexesPtr && selector_) : data(initializeFromSelector(std::move(selector_))) { }

    class Iterator
    {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = size_t;
        using difference_type = std::ptrdiff_t;
        using pointer = size_t *;
        using reference = size_t &;

        Iterator(const Selector & selector_, size_t idx_) : selector(selector_), idx(idx_) { }

        size_t ALWAYS_INLINE operator*() const
        {
            chassert(idx < selector.size());
            if (idx >= selector.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Index {} out of range size {}", idx, selector.size());
            return selector[idx];
        }

        Iterator & ALWAYS_INLINE operator++()
        {
            if (idx >= selector.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Index {} out of range size {}", idx, selector.size());
            ++idx;
            return *this;
        }

        bool ALWAYS_INLINE operator!=(const Iterator & other) const { return idx != other.idx; }

    private:
        const Selector & selector;
        size_t idx;
    };

    Iterator begin() const { return Iterator(*this, 0); }

    Iterator end() const { return Iterator(*this, size()); }

    size_t ALWAYS_INLINE operator[](size_t idx) const
    {
        chassert(idx < size());

        if (std::holds_alternative<Range>(data))
        {
            const auto range = std::get<Range>(data);
            return range.first + idx;
        }
        else
        {
            return std::get<IndexesPtr>(data)->getData()[idx];
        }
    }

    size_t size() const
    {
        if (std::holds_alternative<Range>(data))
        {
            const auto range = std::get<Range>(data);
            return range.second - range.first;
        }
        else
        {
            return std::get<IndexesPtr>(data)->size();
        }
    }

    std::pair<Selector, Selector> split(size_t num_rows)
    {
        chassert(num_rows <= size());

        if (std::holds_alternative<Range>(data))
        {
            const auto range = std::get<Range>(data);

            if (num_rows == 0)
                return {Selector(), Selector{range.first, range.second}};

            if (num_rows == size())
                return {Selector{range.first, range.second}, Selector()};

            return {Selector(range.first, range.first + num_rows), Selector(range.first + num_rows, range.second)};
        }
        else
        {
            const auto & selector = std::get<IndexesPtr>(data)->getData();
            auto && left = Selector(Indexes::create(selector.begin(), selector.begin() + num_rows));
            auto && right = Selector(Indexes::create(selector.begin() + num_rows, selector.end()));
            return {std::move(left), std::move(right)};
        }
    }

    bool isContinuousRange() const { return std::holds_alternative<Range>(data); }

    Range getRange() const
    {
        chassert(isContinuousRange());
        return std::get<Range>(data);
    }

    const Indexes & getIndexes() const
    {
        chassert(!isContinuousRange());
        return *std::get<IndexesPtr>(data);
    }

    std::string toString() const
    {
        if (std::holds_alternative<Range>(data))
        {
            const auto range = std::get<Range>(data);
            return fmt::format("[{}, {})", range.first, range.second);
        }
        else
        {
            const auto & selector = std::get<IndexesPtr>(data)->getData();
            return fmt::format("({})", fmt::join(selector, ","));
        }
    }

private:
    using Data = std::variant<Range, IndexesPtr>;

    Data initializeFromSelector(IndexesPtr && selector_)
    {
        const auto & selector = selector_->getData();
        if (selector.empty())
            return Range{0, 0};

        /// selector represents continuous range
        if (selector.back() == selector.front() + selector.size() - 1)
            return Range{selector.front(), selector.front() + selector.size()};

        return std::move(selector_);
    }

    Data data;
};

}

struct ScatteredBlock : private boost::noncopyable
{
    using Selector = detail::Selector;
    using Indexes = Selector::Indexes;
    using IndexesPtr = Selector::IndexesPtr;

    ScatteredBlock() = default;

    explicit ScatteredBlock(Block block_) : block(std::move(block_)), selector(block.rows()) { }

    ScatteredBlock(Block block_, IndexesPtr && selector_) : block(std::move(block_)), selector(std::move(selector_)) { }

    ScatteredBlock(Block block_, Selector selector_) : block(std::move(block_)), selector(std::move(selector_)) { }

    ScatteredBlock(ScatteredBlock && other) noexcept : block(std::move(other.block)), selector(std::move(other.selector))
    {
        other.block.clear();
        other.selector = {};
    }

    ScatteredBlock & operator=(ScatteredBlock && other) noexcept
    {
        if (this != &other)
        {
            block = std::move(other.block);
            selector = std::move(other.selector);

            other.block.clear();
            other.selector = {};
        }
        return *this;
    }

    Block & getSourceBlock() & { return block; }
    const Block & getSourceBlock() const & { return block; }

    Block && getSourceBlock() && { return std::move(block); }

    const auto & getSelector() const { return selector; }

    explicit operator bool() const { return !!block; }

    /// Accounts only selected rows
    size_t rows() const { return selector.size(); }

    /// Whether block was scattered, i.e. has non-trivial selector
    bool wasScattered() const
    {
        chassert(block);
        return selector.size() != block.rows();
    }

    const ColumnWithTypeAndName & getByName(const std::string & name) const
    {
        chassert(block);
        return block.getByName(name);
    }

    /// Filters selector by mask discarding rows for which filter is false
    void filter(const IColumn::Filter & filter)
    {
        chassert(block && block.rows() == filter.size());
        IndexesPtr new_selector = Indexes::create();
        new_selector->reserve(selector.size());
        std::copy_if(
            selector.begin(), selector.end(), std::back_inserter(new_selector->getData()), [&](size_t idx) { return filter[idx]; });
        selector = Selector(std::move(new_selector));
    }

    /// Applies selector to block in place
    void filterBySelector()
    {
        chassert(block);

        if (!wasScattered())
            return;

        if (selector.isContinuousRange())
        {
            const auto range = selector.getRange();
            for (size_t i = 0; i < block.columns(); ++i)
            {
                auto & col = block.getByPosition(i);
                col.column = col.column->cut(range.first, range.second - range.first);
            }
            selector = Selector(block.rows());
            return;
        }

        /// The general case when selector is non-trivial (likely the result of applying a filter)
        auto columns = block.getColumns();
        for (auto & col : columns)
            col = col->index(selector.getIndexes(), /*limit*/ 0);
        block.setColumns(columns);
        selector = Selector(block.rows());
    }

    /// Cut first num_rows rows from block in place and returns block with remaining rows
    ScatteredBlock cut(size_t num_rows)
    {
        SCOPE_EXIT(filterBySelector());

        if (num_rows >= rows())
            return ScatteredBlock{Block{}};

        chassert(block);

        auto && [first_num_rows, remaining_selector] = selector.split(num_rows);

        auto remaining = ScatteredBlock{block, std::move(remaining_selector)};

        selector = std::move(first_num_rows);

        return remaining;
    }

    void replicate(const IColumn::Offsets & offsets, size_t existing_columns, const std::vector<size_t> & right_keys_to_replicate)
    {
        chassert(block);
        chassert(offsets.size() == rows());

        auto && columns = block.getColumns();
        for (size_t i = 0; i < existing_columns; ++i)
            columns[i] = columns[i]->replicate(offsets);
        for (size_t pos : right_keys_to_replicate)
            columns[pos] = columns[pos]->replicate(offsets);

        block.setColumns(std::move(columns));
        selector = Selector(block.rows());
    }

private:
    Block block;
    Selector selector;
};

using ScatteredBlocks = std::vector<ScatteredBlock>;

}
