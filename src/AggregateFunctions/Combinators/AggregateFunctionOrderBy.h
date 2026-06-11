#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/PODArray.h>
#include <Common/ArenaAllocator.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/sortBlock.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromMemory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/** Stores one serialized value (one column cell) in arena memory.
  * Used to accumulate (sort_key, nested_argument) pairs inside the OrderBy combinator state.
  *
  * Layout in arena: [ size: UInt64 ][ raw bytes: size bytes ]
  * The "raw bytes" are produced by IColumn::serializeValueIntoArena and consumed by
  * IColumn::deserializeAndInsertFromArena, so the format is opaque to us — the column knows
  * how to read its own bytes.
  */
struct OrderByArrayNode
{
    UInt64 size;

    char * data() { return reinterpret_cast<char *>(this) + sizeof(OrderByArrayNode); }
    const char * data() const { return reinterpret_cast<const char *>(this) + sizeof(OrderByArrayNode); }

    /// Allocates a new node with the same payload as *this in the given arena.
    OrderByArrayNode * clone(Arena * arena) const
    {
        char * place = arena->alignedAlloc(sizeof(OrderByArrayNode) + size, alignof(OrderByArrayNode));
        auto * node = reinterpret_cast<OrderByArrayNode *>(place);
        node->size = size;
        memcpy(node->data(), data(), size);
        return node;
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(size, buf);
        buf.write(data(), size);
    }

    static OrderByArrayNode * read(ReadBuffer & buf, Arena * arena)
    {
        UInt64 size;
        readVarUInt(size, buf);
        char * place = arena->alignedAlloc(sizeof(OrderByArrayNode) + size, alignof(OrderByArrayNode));
        auto * node = reinterpret_cast<OrderByArrayNode *>(place);
        node->size = size;
        buf.readStrict(node->data(), size);
        return node;
    }

    /// Serialize one cell of `column` at `row_num` into a freshly-allocated node in `arena`.
    /// IColumn::serializeValueIntoArena writes the bytes contiguously after `begin`, returning
    /// a StringRef that tells us how many bytes were written.
    static OrderByArrayNode * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        IColumn::SerializationSettings settings;
        const char * begin = arena->alignedAlloc(sizeof(OrderByArrayNode), alignof(OrderByArrayNode));
        auto value = column.serializeValueIntoArena(row_num, *arena, begin, &settings);

        auto * node = reinterpret_cast<OrderByArrayNode *>(const_cast<char *>(begin));
        node->size = value.size();
        return node;
    }

    void insertInto(IColumn & column) const
    {
        IColumn::SerializationSettings settings;
        ReadBufferFromMemory buf(data(), size);
        column.deserializeAndInsertFromArena(buf, &settings);
    }
};


/** Per-group accumulator for the OrderBy combinator.
  *
  * `value` is a flat array of nodes laid out as
  *
  *     [ row_0_arg_0, row_0_arg_1, ..., row_0_arg_{N-1},
  *       row_1_arg_0, row_1_arg_1, ..., row_1_arg_{N-1},
  *       ... ]
  *
  * where N is the total number of arguments to the combined function (nested args + sort keys).
  * In insertResultInto we deinterleave this back into N columns, sort by the trailing sort-key
  * columns, and pass the leading nested-argument columns to the inner aggregate.
  *
  * The MixedAlignedArenaAllocator strategy (small-buffer-then-arena) follows the convention used
  * by other "heavy" combinators such as -Distinct.
  */
struct OrderByData
{
    using Allocator = MixedAlignedArenaAllocator<alignof(OrderByArrayNode *), 4096>;
    using Array = PODArray<OrderByArrayNode *, 32, Allocator>;
    Array value;
};


/** Wrapper combinator that buffers (nested_args ++ sort_keys) tuples into per-group arrays,
  * sorts each array on finalization, and feeds the sorted nested arguments to the inner aggregate.
  *
  * Memory layout of one aggregate state:
  *
  *     [ OrderByData (this combinator's accumulator) ][ nested function's state ]
  *
  * The nested state is constructed lazily in insertResultInto, because we don't add anything to
  * it during accumulation — we accumulate into our own buffer and only call nested's add() once,
  * in finalization, on the sorted data.
  */
class AggregateFunctionOrderBy final
    : public IAggregateFunctionDataHelper<OrderByData, AggregateFunctionOrderBy>
{
private:
    using Base = IAggregateFunctionDataHelper<OrderByData, AggregateFunctionOrderBy>;
    using Node = OrderByArrayNode;

    AggregateFunctionPtr nested_func;
    SortDescription sort_description;
    std::optional<UInt64> limit;

public:
    AggregateFunctionOrderBy(
        const DataTypes & arguments_,
        const Array & parameters_,
        const AggregateFunctionPtr & nested_func_,
        const SortDescription & sort_description_,
        std::optional<UInt64> limit_)
        : IAggregateFunctionDataHelper<OrderByData, AggregateFunctionOrderBy>(
              arguments_, parameters_, nested_func_->getResultType())
        , nested_func(nested_func_)
        , sort_description(sort_description_)
        , limit(limit_)
    {
    }

    String getName() const override { return nested_func->getName() + "OrderBy"; }

    bool allocatesMemoryInArena() const override { return true; }

    /// The combined state lives in one buffer: our accumulator first, nested state right after.
    static AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place)
    {
        return place + sizeof(OrderByData);
    }

    static ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place)
    {
        return place + sizeof(OrderByData);
    }

    size_t sizeOfData() const override
    {
        return sizeof(OrderByData) + nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return std::max(alignof(OrderByData), nested_func->alignOfData());
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) OrderByData();
        /// Do NOT create the nested state here. We construct it lazily in insertResultInto,
        /// after sorting, because we only feed sorted data into nested.
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        Base::data(place).~OrderByData();
        /// Nested state is created and destroyed within insertResultInto — nothing to clean up here.
    }

    bool hasTrivialDestructor() const override { return false; }

    /// On each input row, serialize ALL arguments (nested args + sort keys) into our buffer.
    /// We don't separate them yet — the layout is row-major flat.
    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & cur_elems = Base::data(place);
        for (size_t i = 0; i < this->argument_types.size(); ++i)
        {
            Node * node = Node::allocate(*columns[i], row_num, arena);
            cur_elems.value.push_back(node, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur = Base::data(place);
        const auto & rhs_data = Base::data(rhs);
        for (auto * node : rhs_data.value)
            cur.value.push_back(node->clone(arena), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /*version*/) const override
    {
        const auto & value = Base::data(place).value;
        writeVarUInt(value.size(), buf);
        for (const auto * node : value)
            node->write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /*version*/, Arena * arena) const override
    {
        UInt64 elems;
        readVarUInt(elems, buf);
        auto & value = Base::data(place).value;
        value.resize_exact(elems, arena);
        for (UInt64 i = 0; i < elems; ++i)
            value[i] = Node::read(buf, arena);
    }

    /// Finalization: deinterleave into columns, sort, feed sorted nested-arg columns into nested,
    /// finalize nested into the output column.
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        const auto & value = Base::data(place).value;
        const size_t columns_num = this->argument_types.size();

        /// Empty group: still emit a default value via the nested function.
        if (value.empty() || columns_num == 0)
        {
            nested_func->create(getNestedPlace(place));
            try
            {
                nested_func->insertResultInto(getNestedPlace(place), to, arena);
            }
            catch (...)
            {
                nested_func->destroy(getNestedPlace(place));
                throw;
            }
            nested_func->destroy(getNestedPlace(place));
            return;
        }

        if (value.size() % columns_num != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Bad state in -OrderBy combinator: buffer size {} not divisible by argument count {}",
                value.size(), columns_num);

        const size_t rows = value.size() / columns_num;

        /// Build columns from the flat buffer.
        std::vector<MutableColumnPtr> columns; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        columns.reserve(columns_num);
        for (const auto & type : this->argument_types)
        {
            columns.emplace_back(type->createColumn());
            columns.back()->reserve(rows);
        }

        for (size_t i = 0; i < value.size(); ++i)
        {
            const size_t col_idx = i % columns_num;
            value[i]->insertInto(*columns[col_idx]);
        }

        /// Pack into a Block. The names "0", "1", ... match the column references in sort_description
        /// (the OrderBy combinator factory builds sort_description with names std::to_string(idx)).
        Block block;
        for (size_t i = 0; i < columns_num; ++i)
            block.insert(ColumnWithTypeAndName(std::move(columns[i]), this->argument_types[i], std::to_string(i)));

        /// Decide how many rows we need at the top, and sort just enough of the block.
        const size_t rows_to_take = limit.has_value() ? std::min<size_t>(*limit, rows) : rows;
        if (limit.has_value() && *limit < rows)
            sortBlock(block, sort_description, rows_to_take);
        else
            sortBlock(block, sort_description);

        /// Pass only the leading columns (nested-function arguments) to the nested aggregate.
        const size_t nested_args_count = nested_func->getArgumentTypes().size();
        std::vector<const IColumn *> nested_columns; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        nested_columns.reserve(nested_args_count);
        for (size_t i = 0; i < nested_args_count; ++i)
            nested_columns.push_back(block.getByPosition(i).column.get());

        nested_func->create(getNestedPlace(place));
        try
        {
            nested_func->addBatchSinglePlace(0, rows_to_take, getNestedPlace(place), nested_columns.data(), arena);
            nested_func->insertResultInto(getNestedPlace(place), to, arena);
        }
        catch (...)
        {
            nested_func->destroy(getNestedPlace(place));
            throw;
        }
        nested_func->destroy(getNestedPlace(place));
    }
};

}
