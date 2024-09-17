#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/SingleValueData.h>
#include <Common/Concepts.h>
#include <Common/findExtreme.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NOT_IMPLEMENTED;
}

namespace
{

template <typename Data, bool isMin>
class AggregateFunctionMinMax final : public IAggregateFunctionDataHelper<Data, AggregateFunctionMinMax<Data, isMin>>
{
private:
    SerializationPtr serialization;

public:
    explicit AggregateFunctionMinMax(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionMinMax<Data, isMin>>(argument_types_, {}, argument_types_[0])
        , serialization(this->result_type->getDefaultSerialization())
    {
        if (!this->result_type->isComparable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of aggregate function {} because the values of that data type are not comparable",
                this->result_type->getName(),
                getName());
    }

    String getName() const override
    {
        if constexpr (isMin)
            return "min";
        else
            return "max";
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if constexpr (isMin)
            this->data(place).setIfSmaller(*columns[0], row_num, arena);
        else
            this->data(place).setIfGreater(*columns[0], row_num, arena);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        add(place, columns, 0, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            if constexpr (isMin)
                this->data(place).setSmallestNotNullIf(*columns[0], nullptr, if_map.data(), row_begin, row_end, arena);
            else
                this->data(place).setGreatestNotNullIf(*columns[0], nullptr, if_map.data(), row_begin, row_end, arena);
        }
        else
        {
            if constexpr (isMin)
                this->data(place).setSmallest(*columns[0], row_begin, row_end, arena);
            else
                this->data(place).setGreatest(*columns[0], row_begin, row_end, arena);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        const UInt8 * __restrict null_map,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            if constexpr (isMin)
                this->data(place).setSmallestNotNullIf(*columns[0], null_map, if_map.data(), row_begin, row_end, arena);
            else
                this->data(place).setGreatestNotNullIf(*columns[0], null_map, if_map.data(), row_begin, row_end, arena);
        }
        else
        {
            if constexpr (isMin)
                this->data(place).setSmallestNotNullIf(*columns[0], null_map, nullptr, row_begin, row_end, arena);
            else
                this->data(place).setGreatestNotNullIf(*columns[0], null_map, nullptr, row_begin, row_end, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if constexpr (isMin)
            this->data(place).setIfSmaller(this->data(rhs), arena);
        else
            this->data(place).setIfGreater(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::allocatesMemoryInArena(); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilable() const override
    {
        if constexpr (!Data::is_compilable)
            return false;
        else
            return Data::isCompilable(*this->argument_types[0]);
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (Data::is_compilable)
            Data::compileCreate(builder, aggregate_data_ptr);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        if constexpr (Data::is_compilable)
            if constexpr (isMin)
                Data::compileMin(builder, aggregate_data_ptr, arguments[0].value);
            else
                Data::compileMax(builder, aggregate_data_ptr, arguments[0].value);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        if constexpr (Data::is_compilable)
            if constexpr (isMin)
                Data::compileMinMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
            else
                Data::compileMaxMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (Data::is_compilable)
            return Data::compileGetResult(builder, aggregate_data_ptr);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }
#endif
};

template <bool isMin>
AggregateFunctionPtr createAggregateFunctionMinMax(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleValue<AggregateFunctionMinMax, /* unary */ true, isMin>(name, argument_types, parameters, settings));
}
}

void registerAggregateFunctionsMinMax(AggregateFunctionFactory & factory)
{
    factory.registerFunction("min", createAggregateFunctionMinMax<true>, AggregateFunctionFactory::Case::Insensitive);
    factory.registerFunction("max", createAggregateFunctionMinMax<false>, AggregateFunctionFactory::Case::Insensitive);
}

}
