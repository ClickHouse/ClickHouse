#include <memory>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/assert_cast.h>

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

    explicit FunctionBaseTupleNames(DataTypes argument_types_, DataTypePtr result_type_, Array name_fields_)
        : argument_types(std::move(argument_types_)), result_type(std::move(result_type_)), name_fields(std::move(name_fields_))
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
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a named tuple", getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a named tuple", getName());

        DataTypes types = tuple->getElements();
        Array name_fields;
        for (const auto & elem_name : tuple->getElementNames())
            name_fields.emplace_back(elem_name);

        return std::make_unique<FunctionBaseTupleNames>(std::move(types), result_type, std::move(name_fields));
    }
};

}

REGISTER_FUNCTION(TupleNames)
{
    factory.registerFunction<TupleNamesOverloadResolver>(FunctionDocumentation{
        .description = R"(
Turns a named tuple into a constant array of names. For a `Tuple(a T, b T, ..., c T)` returns `Array(String, ...)`, in which the `Strings` represents the named fields of the tuple.
)",
        .examples{{"typical", "SELECT tupleNames(namedTuple(1, 2))", "['1','2']"}},
        .categories{"Miscellaneous"}});
}

}
