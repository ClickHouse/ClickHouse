#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/FailPoint.h>
#include <Common/assert_cast.h>
#include <Common/memory.h>
#include <base/arithmeticOverflow.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace FailPoints
{
extern const char aggregate_function_state_transfer_throw[];
}

template <typename Key>
class AggregateFunctionResample final : public IAggregateFunctionHelper<AggregateFunctionResample<Key>>
{
private:
    /// Sanity threshold to avoid creation of too large arrays. The choice of this number is arbitrary.
    static constexpr size_t max_elements = 1048576;

    /// Sanity threshold for the total size of the state. Nested Resample combinators multiply
    /// the sizes, and a product that avoids the overflow check can still be absurdly large:
    /// the allocator treats sizes of 2^63 and more as a logical error, and anything close
    /// to this threshold could never be allocated anyway.
    static constexpr size_t max_state_size = 1ULL << 40;

    AggregateFunctionPtr nested_function;

    size_t last_col;

    Key begin;
    Key end;
    size_t step;

    size_t total;
    size_t align_of_data;
    size_t size_of_data;

public:
    AggregateFunctionResample(
        AggregateFunctionPtr nested_function_,
        Key begin_,
        Key end_,
        size_t step_,
        const DataTypes & arguments,
        const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionResample<Key>>{arguments, params, createResultType(nested_function_)}
        , nested_function{nested_function_}
        , last_col{arguments.size() - 1}
        , begin{begin_}
        , end{end_}
        , step{step_}
        , total{0}
        , align_of_data{nested_function->alignOfData()}
        , size_of_data{::Memory::alignUp(nested_function->sizeOfData(), align_of_data)}
    {
        // notice: argument types has been checked before
        if (step == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The step given in function {} should not be zero", getName());

        if (end < begin)
            total = 0;
        else
        {
            Key dif;
            size_t sum = 0;
            if (common::subOverflow(end, begin, dif)
                || common::addOverflow(static_cast<size_t>(dif), step, sum))
            {
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Overflow in internal computations in function {}. "
                    "Too large arguments", getName());
            }

            total = (sum - 1) / step; // total = (end - begin + step - 1) / step
        }

        /// A state of an empty range holds no nested state and serializes to zero bytes, and a column of
        /// states is read back one state at a time with no length in front of any of them.
        if (total == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The range given in function {} is empty", getName());

        if (total > max_elements)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The range given in function {} contains too many elements",
                    getName());
    }

    String getName() const override
    {
        return nested_function->getName() + "Resample";
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

        const size_t rhs_align_of_data = rhs_nested->alignOfData();
        const size_t rhs_size_of_data = ::Memory::alignUp(rhs_nested->sizeOfData(), rhs_align_of_data);

        for (size_t i = 0; i < total; ++i)
            nested_function->mergeStateFromDifferentVariant(place + i * size_of_data, *rhs_nested, rhs_place + i * rhs_size_of_data, arena);
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    bool isVersioned() const override
    {
        return nested_function->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_function->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_function->getDefaultVersion();
    }

    DataTypePtr getStateType() const override
    {
        return this->getStateTypeWithVersionOf(*nested_function);
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
        /// Nested Resample combinators multiply the sizes, and every layer is only checked against
        /// `max_elements` on its own, so the product can wrap around or exceed any sane allocation.
        size_t result = 0;
        if (common::mulOverflow(total, size_of_data, result) || result > max_state_size)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Overflow in internal computations in function {}. The state is too large", getName());
        return result;
    }

    size_t alignOfData() const override
    {
        return align_of_data;
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        for (size_t i = 0; i < total; ++i)
        {
            try
            {
                nested_function->create(place + i * size_of_data);
            }
            catch (...)
            {
                for (size_t j = 0; j < i; ++j)
                    nested_function->destroy(place + j * size_of_data);
                throw;
            }
        }
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->destroy(place + i * size_of_data);
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->destroyUpToState(place + i * size_of_data);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Key key;

        if constexpr (static_cast<Key>(-1) < 0)
            key = columns[last_col]->getInt(row_num);
        else
            key = columns[last_col]->getUInt(row_num);

        if (key < begin || key >= end)
            return;

        size_t pos = (key - begin) / step;

        nested_function->add(place + pos * size_of_data, columns, row_num, arena);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->merge(place + i * size_of_data, rhs + i * size_of_data, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->serialize(place + i * size_of_data, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->deserialize(place + i * size_of_data, buf, version, arena);
    }

    static DataTypePtr createResultType(const AggregateFunctionPtr & nested_function_)
    {
        return std::make_shared<DataTypeArray>(nested_function_->getResultType());
    }

    /// `transferred` counts the buckets whose transfer returned, so a caller that catches can undo those.
    template <bool merge>
    void transferBuckets(AggregateDataPtr __restrict place, ColumnArray & col, size_t & transferred, Arena * arena) const
    {
        for (; transferred < total; ++transferred)
        {
            if constexpr (merge)
                nested_function->insertMergeResultInto(place + transferred * size_of_data, col.getData(), arena);
            else
                nested_function->insertResultInto(place + transferred * size_of_data, col.getData(), arena);
        }

        if constexpr (!merge)
        {
            fiu_do_on(FailPoints::aggregate_function_state_transfer_throw,
            {
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Injected failure in AggregateFunctionResample::insertResultInto");
            });
        }

        auto & col_offsets = assert_cast<ColumnArray::ColumnOffsets &>(col.getOffsetsColumn());
        col_offsets.getData().push_back(col.getData().size());
    }

    template <bool merge>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        auto & col = assert_cast<ColumnArray &>(to);
        size_t transferred = 0;

        if constexpr (!merge)
        {
            /// A nested function that is not a state aliases nothing and need not be atomic.
            if (nested_function->isState())
            {
                const size_t offsets_before = assert_cast<ColumnArray::ColumnOffsets &>(col.getOffsetsColumn()).size();

                try
                {
                    transferBuckets<false>(place, col, transferred, arena);
                }
                catch (...)
                {
                    auto & col_offsets = assert_cast<ColumnArray::ColumnOffsets &>(col.getOffsetsColumn());
                    col_offsets.getData().resize_assume_reserved(offsets_before);
                    for (size_t i = transferred; i-- > 0;)
                        nested_function->rollbackInsertResult(place + i * size_of_data, col.getData());
                    throw;
                }

                return;
            }
        }

        transferBuckets<merge>(place, col, transferred, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
    }

    void rollbackInsertResult(ConstAggregateDataPtr __restrict place, IColumn & to) const noexcept override
    {
        auto & col = assert_cast<ColumnArray &>(to);
        auto & col_offsets = assert_cast<ColumnArray::ColumnOffsets &>(col.getOffsetsColumn());

        col_offsets.getData().resize_assume_reserved(col_offsets.size() - 1);
        for (size_t i = total; i-- > 0;)
            nested_function->rollbackInsertResult(place + i * size_of_data, col.getData());
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

}
