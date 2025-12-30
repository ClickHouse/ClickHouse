#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename T>
struct AggregateFunctionGroupBitOrData
{
    T value = 0;
    static const char * name() { return "groupBitOr"; }
    void update(T x) { value |= x; }

#if USE_EMBEDDED_COMPILER

    static void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * value_ptr)
    {
        auto type = toNativeType<T>(builder);
        builder.CreateStore(llvm::Constant::getNullValue(type), value_ptr);
    }

    static llvm::Value* compileUpdate(llvm::IRBuilderBase & builder, llvm::Value * lhs, llvm::Value * rhs)
    {
        return builder.CreateOr(lhs, rhs);
    }

#endif
};

template <typename T>
struct AggregateFunctionGroupBitAndData
{
    T value = -1; /// Two's complement arithmetic, sign extension.
    static const char * name() { return "groupBitAnd"; }
    void update(T x) { value &= x; }

#if USE_EMBEDDED_COMPILER

    static void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * value_ptr)
    {
        auto type = toNativeType<T>(builder);
        builder.CreateStore(llvm::ConstantInt::get(type, -1), value_ptr);
    }

    static llvm::Value* compileUpdate(llvm::IRBuilderBase & builder, llvm::Value * lhs, llvm::Value * rhs)
    {
        return builder.CreateAnd(lhs, rhs);
    }

#endif
};

template <typename T>
struct AggregateFunctionGroupBitXorData
{
    T value = 0;
    static const char * name() { return "groupBitXor"; }
    void update(T x) { value ^= x; }

#if USE_EMBEDDED_COMPILER

    static void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * value_ptr)
    {
        auto type = toNativeType<T>(builder);
        builder.CreateStore(llvm::Constant::getNullValue(type), value_ptr);
    }

    static llvm::Value* compileUpdate(llvm::IRBuilderBase & builder, llvm::Value * lhs, llvm::Value * rhs)
    {
        return builder.CreateXor(lhs, rhs);
    }

#endif
};


/// Counts bitwise operation on numbers.
template <typename T, typename Data>
class AggregateFunctionBitwise final : public IAggregateFunctionDataHelper<Data, AggregateFunctionBitwise<T, Data>>
{
public:
    explicit AggregateFunctionBitwise(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionBitwise<T, Data>>({type}, {}, createResultType())
    {}

    String getName() const override { return Data::name(); }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeNumber<T>>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).update(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).update(this->data(rhs).value);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(this->data(place).value, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinary(this->data(place).value, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(this->data(place).value);
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        auto return_type = this->getResultType();
        return canBeNativeType(*return_type);
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        auto * value_ptr = aggregate_data_ptr;
        Data::compileCreate(builder, value_ptr);
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, this->getResultType());

        auto * value_ptr = aggregate_data_ptr;
        auto * value = b.CreateLoad(return_type, value_ptr);

        auto * result_value = Data::compileUpdate(builder, value, arguments[0].value);

        b.CreateStore(result_value, value_ptr);
    }

    void compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, this->getResultType());

        auto * value_dst_ptr = aggregate_data_dst_ptr;
        auto * value_dst = b.CreateLoad(return_type, value_dst_ptr);

        auto * value_src_ptr = aggregate_data_src_ptr;
        auto * value_src = b.CreateLoad(return_type, value_src_ptr);

        auto * result_value = Data::compileUpdate(builder, value_dst, value_src);

        b.CreateStore(result_value, value_dst_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, this->getResultType());
        auto * value_ptr = aggregate_data_ptr;

        return b.CreateLoad(return_type, value_ptr);
    }

#endif

};


template <template <typename> class Data>
AggregateFunctionPtr createAggregateFunctionBitwise(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    if (!argument_types[0]->canBeUsedInBitOperations())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The type {} of argument for aggregate function {} "
                        "is illegal, because it cannot be used in bitwise operations",
                        argument_types[0]->getName(), name);

    AggregateFunctionPtr res(createWithIntegerType<AggregateFunctionBitwise, Data>(*argument_types[0], argument_types[0]));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument for aggregate function {}", argument_types[0]->getName(), name);

    return res;
}

}

void registerAggregateFunctionsBitwise(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupBitOr", createAggregateFunctionBitwise<AggregateFunctionGroupBitOrData>);
    factory.registerFunction("groupBitAnd", createAggregateFunctionBitwise<AggregateFunctionGroupBitAndData>);
    factory.registerFunction("groupBitXor", createAggregateFunctionBitwise<AggregateFunctionGroupBitXorData>);

    /// Aliases for compatibility with MySQL.
    factory.registerAlias("BIT_OR", "groupBitOr", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("BIT_AND", "groupBitAnd", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("BIT_XOR", "groupBitXor", AggregateFunctionFactory::Case::Insensitive);
}

}
