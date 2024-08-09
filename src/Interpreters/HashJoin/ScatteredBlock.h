#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Common/PODArray.h>

#include <boost/noncopyable.hpp>

namespace DB
{

struct ScatteredBlock : private boost::noncopyable
{
    ScatteredBlock() = default;

    explicit ScatteredBlock(Block block_) : block(std::move(block_)), selector(createTrivialSelector(block.rows())) { }

    ScatteredBlock(Block block_, IColumn::Selector && selector_) : block(std::move(block_)), selector(std::move(selector_)) { }

    ScatteredBlock(ScatteredBlock && other) noexcept : block(std::move(other.block)), selector(std::move(other.selector))
    {
        other.block.clear();
        other.selector.clear();
    }

    ScatteredBlock & operator=(ScatteredBlock && other) noexcept
    {
        if (this != &other)
        {
            block = std::move(other.block);
            selector = std::move(other.selector);

            other.block.clear();
            other.selector.clear();
        }
        return *this;
    }

    Block & getSourceBlock() & { return block; }
    const Block & getSourceBlock() const & { return block; }

    Block && getSourceBlock() && { return std::move(block); }

    const auto & getSelector() const { return selector; }

    bool contains(size_t idx) const { return std::find(selector.begin(), selector.end(), idx) != selector.end(); }

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
        auto * it = std::remove_if(selector.begin(), selector.end(), [&](size_t idx) { return !filter[idx]; });
        selector.resize(std::distance(selector.begin(), it));
    }

    /// Applies selector to block in place
    void filterBySelector()
    {
        chassert(block);

        if (!wasScattered())
            return;

        auto columns = block.getColumns();
        for (auto & col : columns)
        {
            auto c = col->cloneEmpty();
            c->reserve(selector.size());
            /// TODO: create new method in IColumnHelper to devirtualize
            for (const auto idx : selector)
                c->insertFrom(*col, idx);
            col = std::move(c);
        }

        /// We have to to id that way because references to the block should remain valid
        block.setColumns(columns);
        selector = createTrivialSelector(block.rows());
    }

    /// Cut first num_rows rows from block in place and returns block with remaining rows
    ScatteredBlock cut(size_t num_rows)
    {
        SCOPE_EXIT(filterBySelector());

        if (num_rows >= rows())
            return ScatteredBlock{block.cloneEmpty()};

        chassert(block);

        IColumn::Selector remaining_selector(selector.begin() + num_rows, selector.end());
        auto remaining = ScatteredBlock{block, std::move(remaining_selector)};

        selector.erase(selector.begin() + num_rows, selector.end());

        return remaining;
    }

    void replicate(const IColumn::Offsets & offsets, size_t existing_columns, const std::vector<size_t> & right_keys_to_replicate)
    {
        chassert(block);
        chassert(offsets.size() == rows());

        auto columns = block.getColumns();
        for (size_t i = 0; i < existing_columns; ++i)
        {
            auto c = columns[i]->replicate(offsets);
            columns[i] = std::move(c);
        }
        for (size_t pos : right_keys_to_replicate)
        {
            auto c = columns[pos]->replicate(offsets);
            columns[pos] = std::move(c);
        }

        block.setColumns(columns);
        selector = createTrivialSelector(block.rows());
    }

private:
    IColumn::Selector createTrivialSelector(size_t size)
    {
        IColumn::Selector res(size);
        std::iota(res.begin(), res.end(), 0);
        return res;
    }

    Block block;
    IColumn::Selector selector;
};

using ScatteredBlocks = std::vector<ScatteredBlock>;

}
