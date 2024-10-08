#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <base/map.h>
#include "reverse.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
class FunctionReverse : public IFunction
{
public:
    static constexpr auto name = "reverse";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionReverse>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0])
            && !isArray(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            ReverseImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), input_rows_count);
            return col_res;
        }
        if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col_fixed->getN());
            ReverseImpl::vectorFixed(col_fixed->getChars(), col_fixed->getN(), col_res->getChars(), input_rows_count);
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};


/// Also works with arrays.
class ReverseOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "reverse";
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<ReverseOverloadResolver>(context); }

    explicit ReverseOverloadResolver(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (isArray(arguments.at(0).type))
            return FunctionFactory::instance().getImpl("arrayReverse", context)->build(arguments);
        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            FunctionReverse::create(context),
            collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
            return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments.at(0);
    }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(Reverse)
{
    factory.registerFunction<ReverseOverloadResolver>({}, FunctionFactory::Case::Insensitive);
}

}
