#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/FactoryHelpers.h>

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct Settings;

/// Simply count number of not-NULL values.
class AggregateFunctionCountNotNullUnary final
    : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCountNotNullUnary>
{
public:
    AggregateFunctionCountNotNullUnary(const DataTypePtr & argument, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCountNotNullUnary>({argument}, params, createResultType())
    {
        if (!argument->isNullable())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not Nullable data type passed to AggregateFunctionCountNotNullUnary");
    }

    String getName() const override { return "count"; }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).count += !assert_cast<const ColumnNullable &>(*columns[0]).isNullAt(row_num);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto & nc = assert_cast<const ColumnNullable &>(*columns[0]);
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            data(place).count += countBytesInFilterWithNull(flags, nc.getNullMapData().data(), row_begin, row_end);
        }
        else
        {
            size_t rows = row_end - row_begin;
            data(place).count += rows - countBytesInFilter(nc.getNullMapData().data(), row_begin, row_end);
        }
    }

    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override
    {
        return this->getName() == rhs.getName();
    }

    DataTypePtr getNormalizedStateType() const override
    {
        /// Return normalized state type: count()
        AggregateFunctionProperties properties;
        return std::make_shared<DataTypeAggregateFunction>(
            AggregateFunctionFactory::instance().get(getName(), NullsAction::EMPTY, {}, {}, properties), DataTypes{}, Array{});
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
    }


#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override;
    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override;
    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override;
    void compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override;
    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override;

#endif

};


AggregateFunctionPtr AggregateFunctionCount::getOwnNullAdapter(
    const AggregateFunctionPtr &, const DataTypes & types, const Array & params, const AggregateFunctionProperties & /*properties*/) const
{
    return std::make_shared<AggregateFunctionCountNotNullUnary>(types[0], params);
}


#if USE_EMBEDDED_COMPILER

bool AggregateFunctionCount::isCompilable() const
{
    bool is_compilable = true;
    for (const auto & argument_type : argument_types)
    is_compilable &= canBeNativeType(*argument_type);

    return is_compilable;
}

void AggregateFunctionCount::compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
    b.CreateMemSet(aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), sizeof(AggregateFunctionCountData), llvm::assumeAligned(this->alignOfData()));
}

void AggregateFunctionCount::compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType &) const
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * return_type = toNativeType(b, this->getResultType());

    auto * count_value_ptr = aggregate_data_ptr;
    auto * count_value = b.CreateLoad(return_type, count_value_ptr);
    auto * updated_count_value = b.CreateAdd(count_value, llvm::ConstantInt::get(return_type, 1));

    b.CreateStore(updated_count_value, count_value_ptr);
}

void AggregateFunctionCount::compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * return_type = toNativeType(b, this->getResultType());

    auto * count_value_dst_ptr = aggregate_data_dst_ptr;
    auto * count_value_dst = b.CreateLoad(return_type, count_value_dst_ptr);

    auto * count_value_src_ptr = aggregate_data_src_ptr;
    auto * count_value_src = b.CreateLoad(return_type, count_value_src_ptr);

    auto * count_value_dst_updated = b.CreateAdd(count_value_dst, count_value_src);

    b.CreateStore(count_value_dst_updated, count_value_dst_ptr);
}

llvm::Value * AggregateFunctionCount::compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * return_type = toNativeType(b, this->getResultType());
    auto * count_value_ptr = aggregate_data_ptr;

    return b.CreateLoad(return_type, count_value_ptr);
}

bool AggregateFunctionCountNotNullUnary::isCompilable() const
{
    bool is_compilable = true;
    for (const auto & argument_type : argument_types)
    is_compilable &= canBeNativeType(*argument_type);
    return is_compilable;
}

void AggregateFunctionCountNotNullUnary::compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
    b.CreateMemSet(aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), sizeof(AggregateFunctionCountData), llvm::assumeAligned(this->alignOfData()));
}

void AggregateFunctionCountNotNullUnary::compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * return_type = toNativeType(b, this->getResultType());

    auto * is_null_value = b.CreateExtractValue(arguments[0].value, {1});
    auto * increment_value = b.CreateSelect(is_null_value, llvm::ConstantInt::get(return_type, 0), llvm::ConstantInt::get(return_type, 1));

    auto * count_value_ptr = aggregate_data_ptr;
    auto * count_value = b.CreateLoad(return_type, count_value_ptr);
    auto * updated_count_value = b.CreateAdd(count_value, increment_value);

    b.CreateStore(updated_count_value, count_value_ptr);
}

void AggregateFunctionCountNotNullUnary::compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * return_type = toNativeType(b, this->getResultType());

    auto * count_value_dst_ptr = aggregate_data_dst_ptr;
    auto * count_value_dst = b.CreateLoad(return_type, count_value_dst_ptr);

    auto * count_value_src_ptr = aggregate_data_src_ptr;
    auto * count_value_src = b.CreateLoad(return_type, count_value_src_ptr);

    auto * count_value_dst_updated = b.CreateAdd(count_value_dst, count_value_src);

    b.CreateStore(count_value_dst_updated, count_value_dst_ptr);
}

llvm::Value * AggregateFunctionCountNotNullUnary::compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * return_type = toNativeType(b, this->getResultType());
    auto * count_value_ptr = aggregate_data_ptr;

    return b.CreateLoad(return_type, count_value_ptr);
}

#endif


namespace
{

AggregateFunctionPtr createAggregateFunctionCount(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);

    if (argument_types.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires zero or one argument", name);

    return std::make_shared<AggregateFunctionCount>(argument_types);
}

}

void registerAggregateFunctionCount(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };
    factory.registerFunction("count", {createAggregateFunctionCount, properties}, AggregateFunctionFactory::Case::Insensitive);
}

}
