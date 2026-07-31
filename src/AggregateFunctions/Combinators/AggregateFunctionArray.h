#pragma once

#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/** Not an aggregate function, but an adapter of aggregate functions,
  *  which any aggregate function `agg(x)` makes an aggregate function of the form `aggArray(x)`.
  * The adapted aggregate function calculates nested aggregate function for each element of the array.
  */
class AggregateFunctionArray final : public IAggregateFunctionHelper<AggregateFunctionArray>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;

public:
    AggregateFunctionArray(AggregateFunctionPtr nested_, const DataTypes & arguments, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionArray>(arguments, params_, createResultType(nested_))
        , nested_func(nested_), num_arguments(arguments.size())
    {
        if (parameters != nested_func->getParameters())
        {
            /// This invariant should always hold: the Array combinator does not transform
            /// parameters, so the wrapped function must have been created with the same
            /// parameter set. If this fires, it means some code path in
            /// AggregateFunctionFactory or a combinator wrapper lost/modified parameters.
            /// The diagnostic info below will identify the exact mismatch.
            String outer_params_str;
            String nested_params_str;
            for (const auto & p : parameters)
            {
                if (!outer_params_str.empty()) outer_params_str += ", ";
                outer_params_str += p.dump();
            }
            for (const auto & p : nested_func->getParameters())
            {
                if (!nested_params_str.empty()) nested_params_str += ", ";
                nested_params_str += p.dump();
            }
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "AggregateFunctionArray: parameters mismatch between Array wrapper '{}' "
                "and nested function '{}'. Wrapper has {} parameter(s): [{}], "
                "nested function has {} parameter(s): [{}]",
                getName(), nested_func->getName(),
                parameters.size(), outer_params_str,
                nested_func->getParameters().size(), nested_params_str);
        }
        for (const auto & type : arguments)
            if (!isArray(type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "All arguments for aggregate function {} must be arrays", getName());
    }

    String getName() const override
    {
        return nested_func->getName() + "Array";
    }

    static DataTypePtr createResultType(const AggregateFunctionPtr & nested_)
    {
        return nested_->getResultType();
    }

    const IAggregateFunction & getBaseAggregateFunctionWithSameStateRepresentation() const override
    {
        return nested_func->getBaseAggregateFunctionWithSameStateRepresentation();
    }

    DataTypePtr getNormalizedStateType() const override
    {
        return nested_func->getNormalizedStateType();
    }

    bool canMergeStateFromDifferentVariant(const IAggregateFunction & rhs) const override
    {
        if (!this->haveSameDefinition(rhs))
            return false;

        chassert(rhs.getNestedFunction() != nullptr);

        return nested_func->canMergeStateFromDifferentVariant(*rhs.getNestedFunction());
    }

    void mergeStateFromDifferentVariant(
        AggregateDataPtr __restrict place, const IAggregateFunction & rhs, ConstAggregateDataPtr rhs_place, Arena * arena) const override
    {
        chassert(rhs.getNestedFunction() != nullptr);

        nested_func->mergeStateFromDifferentVariant(place, *rhs.getNestedFunction(), rhs_place, arena);
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_func->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_func->create(place);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_func->destroy(place);
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_func->destroyUpToState(place);
    }

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        /// Avoid `absl::InlinedVector`: its constructor zero-initializes the inline storage with 256-bit AVX stores on x86-64-v3,
        /// which forces the compiler to insert `vzeroupper` before every call in this function, including the virtual call in
        /// the inner loop below.
        constexpr size_t max_inline_args = 5;
        const IColumn * nested_inline[max_inline_args]; // NOLINT: intentionally uninitialized
        std::unique_ptr<const IColumn *[]> nested_heap;
        const IColumn ** nested = num_arguments <= max_inline_args
            ? nested_inline
            : (nested_heap = std::unique_ptr<const IColumn *[]>(new const IColumn *[num_arguments])).get();

        for (size_t i = 0; i < num_arguments; ++i)
            nested[i] = &assert_cast<const ColumnArray &>(*columns[i]).getData();

        const ColumnArray & first_array_column = assert_cast<const ColumnArray &>(*columns[0]);
        const IColumn::Offsets & offsets = first_array_column.getOffsets();

        size_t begin = offsets[row_num - 1];
        size_t end = offsets[row_num];

        /// Sanity check. NOTE We can implement specialization for a case with single argument, if the check will hurt performance.
        for (size_t i = 1; i < num_arguments; ++i)
        {
            const ColumnArray & ith_column = assert_cast<const ColumnArray &>(*columns[i]);
            const IColumn::Offsets & ith_offsets = ith_column.getOffsets();

            if (ith_offsets[row_num] != end || (row_num != 0 && ith_offsets[row_num - 1] != begin))
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Arrays passed to {} aggregate function have different sizes", getName());
        }

        for (size_t i = begin; i < end; ++i)
            nested_func->add(place, nested, i, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(place, rhs, arena);
    }

    bool isAbleToParallelizeMerge() const override { return nested_func->isAbleToParallelizeMerge(); }
    bool canOptimizeEqualKeysRanges() const override { return nested_func->canOptimizeEqualKeysRanges(); }

    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override
    {
        nested_func->parallelizeMergePrepare(places, thread_pool, is_cancelled);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        nested_func->merge(place, rhs, thread_pool, is_cancelled, arena);
    }

    void parallelizeMergeMulti(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        nested_func->parallelizeMergeMulti(places, thread_pool, is_cancelled, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_func->serialize(place, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_func->deserialize(place, buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertMergeResultInto(place, to, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
