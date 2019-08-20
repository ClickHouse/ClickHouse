#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{

/** toTypeName(x) - get the type name
  * Returns name of IDataType instance (name of data type).
  */
class PreparedFunctionToTypeName : public PreparedFunctionImpl
{
public:
    static constexpr auto name = "toTypeName";
    String getName() const override { return name; }

protected:
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    /// Execute the function on the block.
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column
            = DataTypeString().createColumnConst(input_rows_count, block.getByPosition(arguments[0]).type->getName());
    }
};


class BaseFunctionToTypeName : public IFunctionBase
{
public:
    BaseFunctionToTypeName(DataTypes argument_types_, DataTypePtr return_type_)
        : argument_types(std::move(argument_types_)), return_type(std::move(return_type_)) {}

    static constexpr auto name = "toTypeName";
    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getReturnType() const override { return return_type; }

    PreparedFunctionPtr prepare(const Block &, const ColumnNumbers &, size_t) const override
    {
        return std::make_shared<PreparedFunctionToTypeName>();
    }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block &, const ColumnNumbers &) const override
    {
        return DataTypeString().createColumnConst(1, argument_types.at(0)->getName());
    }

private:
    DataTypes argument_types;
    DataTypePtr return_type;
};


class FunctionToTypeNameBuilder : public FunctionBuilderImpl
{
public:
    static constexpr auto name = "toTypeName";
    String getName() const override { return name; }
    static FunctionBuilderPtr create(const Context &) { return std::make_shared<FunctionToTypeNameBuilder>(); }

    size_t getNumberOfArguments() const override { return 1; }

protected:
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes types;
        types.reserve(arguments.size());
        for (auto & elem : arguments)
            types.emplace_back(elem.type);

        return std::make_shared<BaseFunctionToTypeName>(types, return_type);
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
};


void registerFunctionToTypeName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToTypeNameBuilder>();
}

}
