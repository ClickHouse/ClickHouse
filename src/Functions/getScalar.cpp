#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <Common/Macros.h>
#include <Core/Field.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Get scalar value of sub queries from query context via IAST::Hash.
  */
class FunctionGetScalar : public IFunction, WithContext
{
public:
    static constexpr auto name = "__getScalar";
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionGetScalar>(context_);
    }

    explicit FunctionGetScalar(ContextPtr context_) : WithContext(context_) {}

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1 || !isString(arguments[0].type) || !arguments[0].column || !isColumnConst(*arguments[0].column))
            throw Exception("Function " + getName() + " accepts one const string argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        auto scalar_name = assert_cast<const ColumnConst &>(*arguments[0].column).getValue<String>();
        ContextPtr query_context = getContext()->hasQueryContext() ? getContext()->getQueryContext() : getContext();
        scalar = query_context->getScalar(scalar_name).getByPosition(0);
        return scalar.type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnConst::create(scalar.column, input_rows_count);
    }

private:
    mutable ColumnWithTypeAndName scalar;
};


/** Get special scalar values
  */
template <typename Scalar>
class FunctionGetSpecialScalar : public IFunction
{
public:
    static constexpr auto name = Scalar::name;
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionGetSpecialScalar<Scalar>>(context_);
    }

    static ColumnWithTypeAndName createScalar(ContextPtr context_)
    {
        if (const auto * block = context_->tryGetSpecialScalar(Scalar::scalar_name))
            return block->getByPosition(0);
        else if (context_->hasQueryContext())
        {
            if (context_->getQueryContext()->hasScalar(Scalar::scalar_name))
                return context_->getQueryContext()->getScalar(Scalar::scalar_name).getByPosition(0);
        }
        return {DataTypeUInt32().createColumnConst(1, 0), std::make_shared<DataTypeUInt32>(), Scalar::scalar_name};
    }

    explicit FunctionGetSpecialScalar(ContextPtr context_)
        : scalar(createScalar(context_)), is_distributed(context_->isDistributed())
    {
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return true;
    }

    bool isSuitableForConstantFolding() const override { return !is_distributed; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override
    {
        return scalar.type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnConst::create(scalar.column, input_rows_count);
    }

private:
    ColumnWithTypeAndName scalar;
    bool is_distributed;
};

struct GetShardNum
{
    static constexpr auto name = "shardNum";
    static constexpr auto scalar_name = "_shard_num";
};

struct GetShardCount
{
    static constexpr auto name = "shardCount";
    static constexpr auto scalar_name = "_shard_count";
};

}

REGISTER_FUNCTION(GetScalar)
{
    factory.registerFunction<FunctionGetScalar>();
    factory.registerFunction<FunctionGetSpecialScalar<GetShardNum>>();
    factory.registerFunction<FunctionGetSpecialScalar<GetShardCount>>();
}

}
