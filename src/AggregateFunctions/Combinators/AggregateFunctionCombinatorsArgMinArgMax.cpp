#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/SingleValueData.h>
#include <Common/memory.h>
#include <DataTypes/TypeTree.h>
#include <DataTypes/getLeastSupertype.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct AggregateFunctionCombinatorArgMinArgMaxData
{
private:
    /// Raw storage populated by `generateSingleValueFromType` via placement construction in the
    /// `DataTypePtr` constructor. Default-initializing with `{}` would zero the whole block on every
    /// aggregate-state creation (hot for high-cardinality `GROUP BY`); skip it deliberately.
    SingleValueDataBaseMemoryBlock v_data; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)

public:
    explicit AggregateFunctionCombinatorArgMinArgMaxData(const DataTypePtr & value_type) // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    {
        generateSingleValueFromType(value_type, v_data);
    }

    ~AggregateFunctionCombinatorArgMinArgMaxData() { data().~SingleValueDataBase(); }

    SingleValueDataBase & data() { return v_data.get(); }
    const SingleValueDataBase & data() const { return v_data.get(); }
};

template <bool isMin>
class AggregateFunctionCombinatorArgMinArgMax final : public IAggregateFunctionHelper<AggregateFunctionCombinatorArgMinArgMax<isMin>>
{
    using Key = AggregateFunctionCombinatorArgMinArgMaxData;

private:
    AggregateFunctionPtr nested_function;
    DataTypePtr data_type;
    SerializationPtr serialization;
    const size_t key_col;
    const size_t key_offset;
    const DataTypePtr key_type;

    AggregateFunctionCombinatorArgMinArgMaxData & data(AggregateDataPtr __restrict place) const /// NOLINT
    {
        return *reinterpret_cast<Key *>(place + key_offset);
    }
    const AggregateFunctionCombinatorArgMinArgMaxData & data(ConstAggregateDataPtr __restrict place) const
    {
        return *reinterpret_cast<const Key *>(place + key_offset);
    }

public:
    AggregateFunctionCombinatorArgMinArgMax(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionCombinatorArgMinArgMax<isMin>>{arguments, params, nested_function_->getResultType()}
        , nested_function{nested_function_}
        , data_type(arguments.back())
        , serialization(arguments.back()->getDefaultSerialization())
        , key_col{arguments.size() - 1}
        , key_offset{::Memory::alignUp(nested_function->sizeOfData(), alignof(Key))}
        , key_type(arguments[key_col])
    {
        if (!arguments[key_col]->isComparable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} for combinator {} because the values of that data type are not comparable",
                arguments[key_col]->getName(),
                getName());

        auto check_not_dynamic_or_variant = [&](const IDataType & type)
        {
            if (isDynamic(type) || isVariant(type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of aggregate function {} because the values of that data type can contain values with "
                    "different data types. Consider using typed subcolumns or cast column to a specific data type{}",
                    arguments[key_col]->getName(),
                    getName(),
                    getNumericVariantSupertypeHint(type.getPtr()));
        };
        forEachInTypeTree(*arguments[key_col], check_not_dynamic_or_variant);
    }

    String getName() const override
    {
        if constexpr (isMin)
            return nested_function->getName() + "ArgMin";
        else
            return nested_function->getName() + "ArgMax";
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

        const size_t rhs_key_offset = ::Memory::alignUp(rhs_nested->sizeOfData(), alignof(Key));
        const auto & rhs_key = *reinterpret_cast<const Key *>(rhs_place + rhs_key_offset);

        if ((isMin && data(place).data().setIfSmaller(rhs_key.data(), arena))
            || (!isMin && data(place).data().setIfGreater(rhs_key.data(), arena)))
        {
            nested_function->destroy(place);
            nested_function->create(place);
            nested_function->mergeStateFromDifferentVariant(place, *rhs_nested, rhs_place, arena);
        }
        else if (data(place).data().isEqualTo(rhs_key.data()))
        {
            nested_function->mergeStateFromDifferentVariant(place, *rhs_nested, rhs_place, arena);
        }
    }

    bool isState() const override { return nested_function->isState(); }

    bool isVersioned() const override { return nested_function->isVersioned(); }

    size_t getVersionFromRevision(size_t revision) const override { return nested_function->getVersionFromRevision(revision); }

    size_t getDefaultVersion() const override { return nested_function->getDefaultVersion(); }

    DataTypePtr getStateType() const override { return this->getStateTypeWithVersionOf(*nested_function); }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena() || singleValueTypeAllocatesMemoryInArena(key_type->getTypeId());
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor() && /*false*/ std::is_trivially_destructible_v<SingleValueDataBase>;
    }

    size_t sizeOfData() const override { return key_offset + sizeof(Key); }

    size_t alignOfData() const override { return std::max(nested_function->alignOfData(), alignof(SingleValueDataBaseMemoryBlock)); }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_function->create(place);
        new (place + key_offset) Key(key_type);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place).~Key();
        nested_function->destroy(place);
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place).~Key();
        nested_function->destroyUpToState(place);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if ((isMin && data(place).data().setIfSmaller(*columns[key_col], row_num, arena))
            || (!isMin && data(place).data().setIfGreater(*columns[key_col], row_num, arena)))
        {
            nested_function->destroy(place);
            nested_function->create(place);
            nested_function->add(place, columns, row_num, arena);
        }
        else if (data(place).data().isEqualTo(*columns[key_col], row_num))
        {
            nested_function->add(place, columns, row_num, arena);
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if ((isMin && data(place).data().setIfSmaller(data(rhs).data(), arena))
            || (!isMin && data(place).data().setIfGreater(data(rhs).data(), arena)))
        {
            nested_function->destroy(place);
            nested_function->create(place);
            nested_function->merge(place, rhs, arena);
        }
        else if (data(place).data().isEqualTo(data(rhs).data()))
        {
            nested_function->merge(place, rhs, arena);
        }
    }

    /// The nested state is at the beginning of the state, so the parallel merge is forwarded to the nested
    /// function with the same places, restricted to the states that have the extremal key.
    bool isAbleToParallelizeMerge() const override { return nested_function->isAbleToParallelizeMerge(); }

    /// `parallelizeMergePrepare` is not forwarded: it would prepare (e.g. convert to two-level) the nested states
    /// of all places, including those that are discarded because their key is not extremal. The nested
    /// `parallelizeMergeMulti` handles the unprepared states of the selected places itself.
    bool isParallelizeMergePrepareNeeded() const override { return false; }

    bool isLargeMergePair(ConstAggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs) const override
    {
        if (!nested_function->isLargeMergePair(place, rhs))
            return false;

        /// A source whose key is already worse than the key of `place` can never become relevant, because later merges
        /// only move the key of `place` further toward the extremum. Let it be merged (that is, discarded) serially,
        /// so that its large nested state is destroyed right away instead of being kept until the deferred merge.
        /// Otherwise the key of `place` can still change before the deferred merge, and `parallelizeMergeMulti`
        /// skips the sources whose key is not extremal.
        const auto & place_key = data(place).data();
        const auto & rhs_key = data(rhs).data();
        if (place_key.has() && rhs_key.has() && !place_key.isEqualTo(rhs_key)
            && !(isMin ? place_key.wouldSetIfSmaller(rhs_key) : place_key.wouldSetIfGreater(rhs_key)))
            return false;

        return true;
    }

    void mergeImpl(
        AggregateDataPtr __restrict place,
        ConstAggregateDataPtr rhs,
        ThreadPool & thread_pool,
        std::atomic<bool> & is_cancelled,
        Arena * arena) const override
    {
        if ((isMin && data(place).data().setIfSmaller(data(rhs).data(), arena))
            || (!isMin && data(place).data().setIfGreater(data(rhs).data(), arena)))
        {
            nested_function->destroy(place);
            nested_function->create(place);
            nested_function->merge(place, rhs, thread_pool, is_cancelled, arena);
        }
        else if (data(place).data().isEqualTo(data(rhs).data()))
        {
            nested_function->merge(place, rhs, thread_pool, is_cancelled, arena);
        }
    }

    void parallelizeMergeMulti(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        if (places.size() <= 1)
            return;

        /// Find the extremal key first, resetting the nested state of the destination whenever its key is replaced,
        /// then merge the nested states of all sources having that key in one pass.
        AggregateDataPtr dst = places[0];
        size_t last_setter = 0;
        for (size_t i = 1; i < places.size(); ++i)
        {
            if ((isMin && data(dst).data().setIfSmaller(data(places[i]).data(), arena))
                || (!isMin && data(dst).data().setIfGreater(data(places[i]).data(), arena)))
            {
                nested_function->destroy(dst);
                nested_function->create(dst);
                last_setter = i;
            }
        }

        /// Select the same sources as the serial merge: the one that installed the final key, even if the key is not
        /// equal to itself (`NaN`), and the later ones having an equal key. The earlier ones were discarded by the reset.
        AggregateDataPtrs selected;
        selected.reserve(places.size() - last_setter + 1);
        selected.push_back(dst);
        if (last_setter)
            selected.push_back(places[last_setter]);
        for (size_t i = last_setter + 1; i < places.size(); ++i)
            if (data(dst).data().isEqualTo(data(places[i]).data()))
                selected.push_back(places[i]);

        nested_function->parallelizeMergeMulti(selected, thread_pool, is_cancelled, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_function->serialize(place, buf, version);
        data(place).data().write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_function->deserialize(place, buf, version, arena);
        data(place).data().read(buf, *serialization, data_type, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertResultInto(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertMergeResultInto(place, to, arena);
    }

    void rollbackInsertResult(ConstAggregateDataPtr __restrict place, IColumn & to) const noexcept override
    {
        nested_function->rollbackInsertResult(place, to);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

template <bool isMin>
class CombinatorArgMinArgMax final : public IAggregateFunctionCombinator
{
public:
    String getName() const override
    {
        if constexpr (isMin)
            return "ArgMin";
        else
            return "ArgMax";
    }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix",
                getName());

        return DataTypes(arguments.begin(), arguments.end() - 1);
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionCombinatorArgMinArgMax<isMin>>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorsArgMinArgMax(AggregateFunctionCombinatorFactory & factory);
void registerAggregateFunctionCombinatorsArgMinArgMax(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<CombinatorArgMinArgMax<true>>(), Documentation{
        .description = "Applied as a suffix to an aggregate function name (e.g. `sumArgMin`), it adds an extra trailing argument that should be any comparable expression. The nested aggregate function processes only the rows that have the minimum value of that extra expression, using all of the preceding arguments (which may be zero, one, or several, e.g. `countArgMin(key)`, `sumArgMin(x, key)`, `corrArgMin(x, y, key)`).",
        .syntax = "<aggregate_function>ArgMin([arg, ...], key)",
        .related = {"ArgMax"}});
    factory.registerCombinator(std::make_shared<CombinatorArgMinArgMax<false>>(), Documentation{
        .description = "Applied as a suffix to an aggregate function name (e.g. `sumArgMax`), it adds an extra trailing argument that should be any comparable expression. The nested aggregate function processes only the rows that have the maximum value of that extra expression, using all of the preceding arguments (which may be zero, one, or several, e.g. `countArgMax(key)`, `sumArgMax(x, key)`, `corrArgMax(x, y, key)`).",
        .syntax = "<aggregate_function>ArgMax([arg, ...], key)",
        .related = {"ArgMin"}});
}

}
