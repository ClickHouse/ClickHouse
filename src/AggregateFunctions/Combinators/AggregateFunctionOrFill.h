#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Combinators/AggregateFunctionNull.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Common/memory.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
}

/**
  * -OrDefault and -OrNull combinators for aggregate functions.
  * If there are no input values, return NULL or a default value, accordingly.
  * Use a single additional byte of data after the nested function data:
  * 0 means there was no input, 1 means there was some.
  */
template <bool UseNull>
class AggregateFunctionOrFill final : public IAggregateFunctionHelper<AggregateFunctionOrFill<UseNull>>
{
private:
    AggregateFunctionPtr nested_function;

    size_t size_of_data;
    bool inner_nullable;
    bool result_is_nullable;

public:
    AggregateFunctionOrFill(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionOrFill>{arguments, params, createResultType(nested_function_->getResultType())}
        , nested_function{nested_function_}
        , size_of_data{nested_function->sizeOfData()}
        , inner_nullable{nested_function->getResultType()->isNullable()}
        , result_is_nullable{createResultType(nested_function_->getResultType())->isNullable()}
    {
        // nothing
    }

    String getName() const override
    {
        if constexpr (UseNull)
            return nested_function->getName() + "OrNull";
        else
            return nested_function->getName() + "OrDefault";
    }

    bool canMergeStateFromDifferentVariant(const IAggregateFunction & rhs) const override
    {
        if (!this->haveSameDefinition(rhs))
            return false;

        auto rhs_nested = rhs.getNestedFunction();
        chassert(rhs_nested != nullptr);

        return nested_function->canMergeStateFromDifferentVariant(*rhs_nested);
    }

    void mergeStateFromDifferentVariant(
        AggregateDataPtr __restrict place, const IAggregateFunction & rhs, ConstAggregateDataPtr rhs_place, Arena * arena) const override
    {
        auto rhs_nested = rhs.getNestedFunction();
        chassert(rhs_nested != nullptr);

        nested_function->mergeStateFromDifferentVariant(place, *rhs_nested, rhs_place, arena);

        const size_t rhs_size_of_data = rhs_nested->sizeOfData();
        place[size_of_data] |= rhs_place[rhs_size_of_data];
    }

    bool isVersioned() const override
    {
        return nested_function->isVersioned();
    }

    size_t getDefaultVersion() const override
    {
        return nested_function->getDefaultVersion();
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        /// Pad to alignment so that arrays of states (e.g. in -ForEach) keep each element aligned.
        return ::Memory::alignUp(size_of_data + sizeof(char), alignOfData());
    }

    size_t alignOfData() const override
    {
        return nested_function->alignOfData();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_function->create(place);
        place[size_of_data] = 0;
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroy(place);
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroyUpToState(place);
    }

    void add(
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        size_t row_num,
        Arena * arena) const override
    {
        nested_function->add(place, columns, row_num, arena);
        place[size_of_data] = 1;
    }

    void addBatch( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i] && places[i])
                    add(places[i] + place_offset, columns, i, arena);
            }
        }
        else
        {
            nested_function->addBatch(row_begin, row_end, places, place_offset, columns, arena, if_argument_pos);
            for (size_t i = row_begin; i < row_end; ++i)
                if (places[i])
                    (places[i] + place_offset)[size_of_data] = 1;
        }
    }

    void addBatchSinglePlace( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            nested_function->addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos);
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i])
                {
                    place[size_of_data] = 1;
                    break;
                }
            }
        }
        else
        {
            if (row_end != row_begin)
            {
                nested_function->addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos);
                place[size_of_data] = 1;
            }
        }
    }

    void addBatchSinglePlaceNotNull( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            nested_function->addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, if_argument_pos);
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i] && !null_map[i])
                {
                    place[size_of_data] = 1;
                    break;
                }
            }
        }
        else
        {
            if (row_end != row_begin)
            {
                nested_function->addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, if_argument_pos);
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    if (!null_map[i])
                    {
                        place[size_of_data] = 1;
                        break;
                    }
                }
            }
        }
    }

    void merge(
        AggregateDataPtr __restrict place,
        ConstAggregateDataPtr rhs,
        Arena * arena) const override
    {
        nested_function->merge(place, rhs, arena);
        place[size_of_data] |= rhs[size_of_data];
    }

    void mergeBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        ThreadPool & thread_pool,
        std::atomic<bool> & is_cancelled,
        Arena * arena) const override
    {
        nested_function->mergeBatch(row_begin, row_end, places, place_offset, rhs, thread_pool, is_cancelled, arena);
        for (size_t i = row_begin; i < row_end; ++i)
            (places[i] + place_offset)[size_of_data] |= rhs[i][size_of_data];
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_function->serialize(place, buf, version);

        writeChar(place[size_of_data], buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_function->deserialize(place, buf, version, arena);

        readChar(place[size_of_data], buf);
    }

    static DataTypePtr createResultType(const DataTypePtr & inner_type_)
    {
        if constexpr (UseNull)
        {
            // -OrNull

            if (inner_type_->isNullable())
                return inner_type_;

            if (!inner_type_->canBeInsideNullable())
                return inner_type_;

            return std::make_shared<DataTypeNullable>(inner_type_);
        }
        else
        {
            // -OrDefault

            return inner_type_;
        }
    }

    template <bool merge>
    void insertResultIntoImpl(
        AggregateDataPtr __restrict place,
        IColumn & to,
        Arena * arena) const
    {
        if (place[size_of_data])
        {
            if constexpr (UseNull)
            {
                // -OrNull

                if (!result_is_nullable || inner_nullable)
                {
                    if constexpr (merge)
                        nested_function->insertMergeResultInto(place, to, arena);
                    else
                        nested_function->insertResultInto(place, to, arena);
                }
                else
                {
                    ColumnNullable & col = typeid_cast<ColumnNullable &>(to);

                    col.getNullMapColumn().insertDefault();
                    if constexpr (merge)
                        nested_function->insertMergeResultInto(place, col.getNestedColumn(), arena);
                    else
                        nested_function->insertResultInto(place, col.getNestedColumn(), arena);
                }
            }
            else
            {
                // -OrDefault
                if constexpr (merge)
                    nested_function->insertMergeResultInto(place, to, arena);
                else
                    nested_function->insertResultInto(place, to, arena);
            }
        }
        else
            to.insertDefault();
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }

    /// After `Nullable(Tuple)` was introduced, Tuple's `canBeInsideNullable` now returns true,
    /// which changed the default null adapter for Tuple-returning functions:
    ///   - single-arg: from `<false, false>` to `<true, true>` (flag byte added to serialization).
    ///   - multi-arg: from `<false, true>` to `<true, true>` (flag byte was already present).
    /// Only single-arg functions are affected because the multi-arg (variadic) Null combinator
    /// always serialized the flag byte unconditionally, so its serialization format did not change.
    /// Only OrDefault is affected. OrNull also has no backward compat concern since it
    /// didn't work for Tuple-returning functions before `Nullable(Tuple)` was introduced.
    /// Currently, the only single-arg Tuple-returning aggregate function is `sumCount`.
    /// We hardcode the check for `sumCount` rather than matching all single-arg Tuple-returning
    /// functions, so that future functions with the same shape get the correct new behavior
    /// (`<true, true>`) by default and are not silently forced into the legacy adapter.
    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function_,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        if constexpr (!UseNull) /// OrDefault only
        {
            if (nested_function->getName() == "sumCount")
                return std::make_shared<AggregateFunctionNullUnary<false, false>>(nested_function_, arguments, params);
        }
        return nullptr;
    }
};

}
