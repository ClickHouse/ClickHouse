#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <Interpreters/Context.h>
#include <Core/ServerSettings.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <type_traits>

#include "Interpreters/sortBlock.h"


namespace DB {
    struct Settings;

    namespace ErrorCodes {
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
        extern const int BAD_ARGUMENTS;
        extern const int TOO_LARGE_ARRAY_SIZE;
    }

    struct OrderByArrayNode {
        using Node = OrderByArrayNode;
        UInt64 size; // size of payload

        /// Returns pointer to actual payload
        char *data() { return reinterpret_cast<char *>(this) + sizeof(Node); }

        const char *data() const { return reinterpret_cast<const char *>(this) + sizeof(Node); }

        /// Clones existing node (does not modify next field)
        Node *clone(Arena *arena) const {
            return reinterpret_cast<Node *>(
                const_cast<char *>(arena->alignedInsert(reinterpret_cast<const char *>(this), sizeof(Node) + size,
                                                        alignof(Node))));
        }

        /// Write node to buffer
        void write(WriteBuffer &buf) const {
            writeVarUInt(size, buf);
            buf.write(data(), size);
        }

        /// Reads and allocates node from ReadBuffer's data (doesn't set next)
        static Node *read(ReadBuffer &buf, Arena *arena) {
            UInt64 size;
            readVarUInt(size, buf);
            Node *node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + size, alignof(Node)));
            node->size = size;
            buf.readStrict(node->data(), size);
            return node;
        }

        static Node *allocate(const IColumn &column, size_t row_num, Arena *arena) {
            const char *begin = arena->alignedAlloc(sizeof(Node), alignof(Node));
            StringRef value = column.serializeValueIntoArena(row_num, *arena, begin);

            Node *node = reinterpret_cast<Node *>(const_cast<char *>(begin));
            node->size = value.size;

            return node;
        }

        void insertInto(IColumn &column) { std::ignore = column.deserializeAndInsertFromArena(data()); }
    };

    struct OrderByData {
        // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
        using Allocator = MixedAlignedArenaAllocator<alignof(OrderByArrayNode *), 4096>;
        using Array = PODArray<OrderByArrayNode *, 32, Allocator>;

        Array value;
    };

    /// Implementation of groupArray for String or any ComplexObject via Array
    class AggregateFunctionOrderBy final
            : public IAggregateFunctionDataHelper<OrderByData, AggregateFunctionOrderBy> {
        using Base = IAggregateFunctionDataHelper<OrderByData, AggregateFunctionOrderBy>;
        using Node = OrderByArrayNode;

        AggregateFunctionPtr nested_func_;
        SortDescription sort_description_;

    public:
        AggregateFunctionOrderBy(const DataTypes &data_types_, const Array &parameters_,
                                 const AggregateFunctionPtr &nested_func, const SortDescription &sort_description)
            : IAggregateFunctionDataHelper(
                  data_types_, parameters_, nested_func->getResultType())
              , nested_func_(nested_func)
              , sort_description_(sort_description) {
        }

        static AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) {
            return place + sizeof(OrderByData);
        }

        static ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) {
            return place + sizeof(OrderByData);
        }

        size_t sizeOfData() const override {
            return sizeof(Data) + nested_func_->sizeOfData();
        }

        size_t alignOfData() const override {
            return nested_func_->alignOfData();
        }

        String getName() const override { return nested_func_->getName() + "OrderBy"; }

        void create(AggregateDataPtr __restrict place) const override {
            new(place) Data();
        }

        void destroy(AggregateDataPtr place) const noexcept override {
            data(place).~Data();
        }

        bool hasTrivialDestructor() const override {
            return std::is_trivially_destructible_v<Data> && nested_func_->hasTrivialDestructor();
        }

        void add(AggregateDataPtr __restrict place, const IColumn **columns, size_t row_num,
                 Arena *arena) const override {
            auto &cur_elems = Base::data(place);

            for (size_t i = 0; i < this->argument_types.size(); ++i) {
                Node *node = Node::allocate(*columns[i], row_num, arena);
                cur_elems.value.push_back(node, arena);
            }
        }

        void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *arena) const override {
            auto &cur_elems = data(place);
            const auto &rhs_elems = data(rhs);

            if (rhs_elems.value.empty())
                return;

            for (const auto &i: rhs_elems.value)
                cur_elems.value.push_back(i->clone(arena), arena);
        }

        void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer &buf,
                       std::optional<size_t> version) const override {
            UInt64 elems = data(place).value.size();
            writeVarUInt(elems, buf);

            const auto &value = data(place).value;
            for (const auto &node: value)
                node->write(buf);

            nested_func_->serialize(getNestedPlace(place), buf, version);
        }

        void deserialize(AggregateDataPtr __restrict place, ReadBuffer &buf, std::optional<size_t> version,
                         Arena *arena) const override {
            UInt64 elems;
            readVarUInt(elems, buf);

            if (unlikely(elems == 0))
                return;

            auto &value = Base::data(place).value;

            value.resize_exact(elems, arena);
            for (UInt64 i = 0; i < elems; ++i)
                value[i] = Node::read(buf, arena);

            nested_func_->deserialize(getNestedPlace(place), buf, version, arena);
        }

        void insertResultInto(AggregateDataPtr __restrict place, IColumn &to, Arena *arena) const override {
            size_t columns_num = getArgumentTypes().size();

            std::vector<MutableColumnPtr> columns;
            columns.reserve(getArgumentTypes().size());

            for (const auto &type: this->getArgumentTypes()) {
                columns.emplace_back(type->createColumn());
                columns.back()->reserve(data(place).value.size() / columns_num);
            }

            size_t column_idx = 0;
            for (const auto &elem: data(place).value) {
                elem->insertInto(*columns[column_idx++ % columns_num]);
            }

            Block block;
            for (size_t i = 0; i < columns_num; ++i) {
                block.insert(ColumnWithTypeAndName(std::move(columns[i]), argument_types[i], std::to_string(i)));
            }

            sortBlock(block, sort_description_);

            std::vector<AggregateDataPtr> nested_places(block.rows(), getNestedPlace(place));
            std::vector<const IColumn *> sorted_columns;

            for (const auto &column: block.getColumns()) {
                sorted_columns.push_back(column.get());
            }

            nested_func_->create(getNestedPlace(place));

            nested_func_->addBatchSinglePlace(0, block.rows(), getNestedPlace(place), sorted_columns.data(), arena);
            nested_func_->insertResultInto(getNestedPlace(place), to, arena);

            nested_func_->destroy(getNestedPlace(place));
        }

        bool allocatesMemoryInArena() const override { return true; }
    };
}
