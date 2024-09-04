#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Transform a named tuple into names, which is a constant array of strings.
  */
class ExecutableFunctionTupleNames : public IExecutableFunction
{
public:
    static constexpr auto name = "tupleNames";

    explicit ExecutableFunctionTupleNames(Array name_fields_) : name_fields(std::move(name_fields_)) { }

    String getName() const override { return name; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, name_fields);
    }

private:
    Array name_fields;
};

class FunctionBaseTupleNames : public IFunctionBase
{
public:
    static constexpr auto name = "tupleNames";

    explicit FunctionBaseTupleNames(DataTypePtr argument_type, DataTypePtr result_type_, Array name_fields_)
        : argument_types({std::move(argument_type)}), result_type(std::move(result_type_)), name_fields(std::move(name_fields_))
    {
    }

    String getName() const override { return name; }

    bool isSuitableForConstantFolding() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }

    const DataTypePtr & getResultType() const override { return result_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionTupleNames>(name_fields);
    }

private:
    DataTypes argument_types;
    DataTypePtr result_type;
    Array name_fields;
};

class TupleNamesOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "tupleNames";

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<TupleNamesOverloadResolver>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a tuple", getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a tuple", getName());

        DataTypes types = tuple->getElements();
        Array name_fields;
        for (const auto & elem_name : tuple->getElementNames())
            name_fields.emplace_back(elem_name);

        return std::make_unique<FunctionBaseTupleNames>(arguments[0].type, result_type, std::move(name_fields));
    }
};

}

REGISTER_FUNCTION(TupleNames)
{
    factory.registerFunction<TupleNamesOverloadResolver>(FunctionDocumentation{
        .description = R"(
Converts a tuple into an array of column names. For a tuple in the form `Tuple(a T, b T, ...)`, it returns an array of strings representing the named columns of the tuple. If the tuple elements do not have explicit names, their indices will be used as the column names instead.
)",
        .examples{{"typical", "SELECT tupleNames(tuple(1 as a, 2 as b))", "['a','b']"}},
        .categories{"Miscellaneous"}});
}

}
