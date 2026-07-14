#pragma once

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

template <typename Impl, typename Name>
class FunctionStringReplace : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionStringReplace>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"haystack", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
            {"pattern", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"replacement", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };

        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr column_haystack = arguments[0].column;
        column_haystack = column_haystack->convertToFullColumnIfConst();

        const ColumnPtr column_needle = arguments[1].column;
        const ColumnPtr column_replacement = arguments[2].column;

        const ColumnString * col_haystack = checkAndGetColumn<ColumnString>(column_haystack.get());
        const ColumnFixedString * col_haystack_fixed = checkAndGetColumn<ColumnFixedString>(column_haystack.get());

        const ColumnString * col_needle_vector = checkAndGetColumn<ColumnString>(column_needle.get());
        const ColumnConst * col_needle_const = checkAndGetColumn<ColumnConst>(column_needle.get());

        const ColumnString * col_replacement_vector = checkAndGetColumn<ColumnString>(column_replacement.get());
        const ColumnConst * col_replacement_const = checkAndGetColumn<ColumnConst>(column_replacement.get());

        auto col_res = ColumnString::create();

        if (col_haystack && col_needle_const && col_replacement_const)
        {
            Impl::vectorConstantConstant(
                col_haystack->getChars(), col_haystack->getOffsets(),
                col_needle_const->getValue<String>(),
                col_replacement_const->getValue<String>(),
                col_res->getChars(), col_res->getOffsets(),
                input_rows_count);
            return col_res;
        }
        if (col_haystack && col_needle_vector && col_replacement_const)
        {
            Impl::vectorVectorConstant(
                col_haystack->getChars(),
                col_haystack->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                col_replacement_const->getValue<String>(),
                col_res->getChars(),
                col_res->getOffsets(),
                input_rows_count);
            return col_res;
        }
        if (col_haystack && col_needle_const && col_replacement_vector)
        {
            Impl::vectorConstantVector(
                col_haystack->getChars(),
                col_haystack->getOffsets(),
                col_needle_const->getValue<String>(),
                col_replacement_vector->getChars(),
                col_replacement_vector->getOffsets(),
                col_res->getChars(),
                col_res->getOffsets(),
                input_rows_count);
            return col_res;
        }
        if (col_haystack && col_needle_vector && col_replacement_vector)
        {
            Impl::vectorVectorVector(
                col_haystack->getChars(),
                col_haystack->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                col_replacement_vector->getChars(),
                col_replacement_vector->getOffsets(),
                col_res->getChars(),
                col_res->getOffsets(),
                input_rows_count);
            return col_res;
        }
        if (col_haystack_fixed && col_needle_const && col_replacement_const)
        {
            Impl::vectorFixedConstantConstant(
                col_haystack_fixed->getChars(),
                col_haystack_fixed->getN(),
                col_needle_const->getValue<String>(),
                col_replacement_const->getValue<String>(),
                col_res->getChars(),
                col_res->getOffsets(),
                input_rows_count);
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
    }
};

}
