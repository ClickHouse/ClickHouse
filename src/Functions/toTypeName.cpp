#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{

/** toTypeName(x) - get the type name
  * Returns name of IDataType instance (name of data type).
  */
class ExecutableFunctionToTypeName : public IExecutableFunctionImpl
{
public:
    static constexpr auto name = "toTypeName";
    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    /// Execute the function on the block.
    void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column
            = DataTypeString().createColumnConst(input_rows_count, block.getByPosition(arguments[0]).type->getName());
    }
};


class BaseFunctionToTypeName : public IFunctionBaseImpl
{
public:
    BaseFunctionToTypeName(DataTypes argument_types_, DataTypePtr return_type_)
        : argument_types(std::move(argument_types_)), return_type(std::move(return_type_)) {}

    static constexpr auto name = "toTypeName";
    String getName() const override { return name; }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getReturnType() const override { return return_type; }

    ExecutableFunctionImplPtr prepare(const Block &, const ColumnNumbers &, size_t) const override
    {
        return std::make_unique<ExecutableFunctionToTypeName>();
    }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block &, const ColumnNumbers &) const override
    {
        return DataTypeString().createColumnConst(1, argument_types.at(0)->getName());
    }

private:
    DataTypes argument_types;
    DataTypePtr return_type;
};


class FunctionToTypeNameBuilder : public IFunctionOverloadResolverImpl
{
public:
    static constexpr auto name = "toTypeName";
    String getName() const override { return name; }
    static FunctionOverloadResolverImplPtr create(const Context &) { return std::make_unique<FunctionToTypeNameBuilder>(); }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnType(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
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


void registerFunctionToTypeName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToTypeNameBuilder>();
}

}
