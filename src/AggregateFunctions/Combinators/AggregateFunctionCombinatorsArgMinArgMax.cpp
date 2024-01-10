#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/SingleValueData.h>

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
    char v_data[SingleValueDataBase::MAX_STORAGE_SIZE];

public:
    explicit AggregateFunctionCombinatorArgMinArgMaxData(TypeIndex value_type) { generateSingleValueFromTypeIndex(value_type, v_data); }

    SingleValueDataBase & data() { return *reinterpret_cast<SingleValueDataBase *>(v_data); }

    const SingleValueDataBase & data() const { return *reinterpret_cast<const SingleValueDataBase *>(v_data); }
};

template <bool isMin>
class AggregateFunctionCombinatorArgMinArgMax final : public IAggregateFunctionHelper<AggregateFunctionCombinatorArgMinArgMax<isMin>>
{
    using Key = AggregateFunctionCombinatorArgMinArgMaxData;

private:
    AggregateFunctionPtr nested_function;
    SerializationPtr serialization;
    const size_t key_col;
    const size_t key_offset;
    const TypeIndex key_type_index;

    SingleValueDataBase & data(AggregateDataPtr __restrict place) const { return reinterpret_cast<Key *>(place + key_offset)->data(); }
    const SingleValueDataBase & data(ConstAggregateDataPtr __restrict place) const
    {
        return reinterpret_cast<const Key *>(place + key_offset)->data();
    }

public:
    AggregateFunctionCombinatorArgMinArgMax(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionCombinatorArgMinArgMax<isMin>>{arguments, params, nested_function_->getResultType()}
        , nested_function{nested_function_}
        , serialization(arguments.back()->getDefaultSerialization())
        , key_col{arguments.size() - 1}
        , key_offset{(nested_function->sizeOfData() + alignof(Key) - 1) / alignof(Key) * alignof(Key)}
        , key_type_index(WhichDataType(arguments[key_col]).idx)
    {
        if (!arguments[key_col]->isComparable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} for combinator {} because the values of that data type are not comparable",
                arguments[key_col]->getName(),
                getName());
    }

    String getName() const override
    {
        if constexpr (isMin)
            return "ArgMin";
        else
            return "ArgMax";
    }

    bool isState() const override { return nested_function->isState(); }

    bool isVersioned() const override { return nested_function->isVersioned(); }

    size_t getVersionFromRevision(size_t revision) const override { return nested_function->getVersionFromRevision(revision); }

    size_t getDefaultVersion() const override { return nested_function->getDefaultVersion(); }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena() || key_type_index == TypeIndex::String;
    }

    bool hasTrivialDestructor() const override { return nested_function->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return key_offset + sizeof(AggregateFunctionCombinatorArgMinArgMaxData); }

    size_t alignOfData() const override { return nested_function->alignOfData(); }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_function->create(place);
        new (place + key_offset) Key(key_type_index);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override { nested_function->destroy(place); }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override { nested_function->destroyUpToState(place); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if ((isMin && data(place).setIfSmaller(*columns[key_col], row_num, arena))
            || (!isMin && data(place).setIfGreater(*columns[key_col], row_num, arena)))
        {
            nested_function->destroy(place);
            nested_function->create(place);
            nested_function->add(place, columns, row_num, arena);
        }
        else if (data(place).isEqualTo(*columns[key_col], row_num))
        {
            nested_function->add(place, columns, row_num, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if ((isMin && data(place).setIfSmaller(data(rhs), arena)) || (!isMin && data(place).setIfGreater(data(rhs), arena)))
        {
            nested_function->destroy(place);
            nested_function->create(place);
            nested_function->merge(place, rhs, arena);
        }
        else if (data(place).isEqualTo(data(rhs)))
        {
            nested_function->merge(place, rhs, arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_function->serialize(place, buf, version);
        data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_function->deserialize(place, buf, version, arena);
        data(place).read(buf, *serialization, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertResultInto(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertMergeResultInto(place, to, arena);
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

void registerAggregateFunctionCombinatorsArgMinArgMax(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<CombinatorArgMinArgMax<true>>());
    factory.registerCombinator(std::make_shared<CombinatorArgMinArgMax<false>>());
}

}
