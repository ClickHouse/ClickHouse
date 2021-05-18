#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{
namespace
{

/** toTypeName(x) - get the type name
  * Returns name of IDataType instance (name of data type).
  */
class ExecutableFunctionToTypeName : public IExecutableFunction
{
public:
    static constexpr auto name = "toTypeName";
    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    /// Execute the function on the columns.
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, arguments[0].type->getName());
    }
};


class BaseFunctionToTypeName : public IFunctionBase
{
public:
    BaseFunctionToTypeName(DataTypes argument_types_, DataTypePtr return_type_)
        : argument_types(std::move(argument_types_)), return_type(std::move(return_type_)) {}

    static constexpr auto name = "toTypeName";
    String getName() const override { return name; }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionToTypeName>();
    }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const ColumnsWithTypeAndName &) const override
    {
        return DataTypeString().createColumnConst(1, argument_types.at(0)->getName());
    }

private:
    DataTypes argument_types;
    DataTypePtr return_type;
};


class FunctionToTypeNameBuilder : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "toTypeName";
    String getName() const override { return name; }
    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<FunctionToTypeNameBuilder>(); }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes types;
        types.reserve(arguments.size());
        for (const auto & elem : arguments)
            types.emplace_back(elem.type);

        return std::make_unique<BaseFunctionToTypeName>(types, return_type);
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }
};

}

void registerFunctionToTypeName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToTypeNameBuilder>();
}

}
