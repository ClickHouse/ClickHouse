#pragma once

#include <limits>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>


namespace DB
{
/** Applies regexp re2 and extracts:
  * - the first subpattern, if the regexp has a subpattern;
  * - the zero subpattern (the match part, otherwise);
  * - if not match - an empty string.
  * extract(haystack, pattern)
  */

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace Setting
{
    extern const SettingsBool compile_regular_expressions;
    extern const SettingsUInt64 min_count_to_compile_regular_expression;
}


template <typename Impl, typename Name>
class FunctionsStringSearchToString final : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionsStringSearchToString>(context); }

    explicit FunctionsStringSearchToString(ContextPtr context)
    {
        if (context && context->getSettingsRef()[Setting::compile_regular_expressions])
            regexp_jit_min_count = context->getSettingsRef()[Setting::min_count_to_compile_regular_expression];
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[1]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        const ColumnPtr column_needle = arguments[1].column;

        const ColumnConst * col_needle = typeid_cast<const ColumnConst *>(&*column_needle);
        if (!col_needle)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be constant string", getName());

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            const String needle = col_needle->getValue<String>();
            /// Only impls that opt in (currently `ExtractImpl`) take the JIT compile-count threshold.
            if constexpr (requires { Impl::vector(col->getChars(), col->getOffsets(), needle, vec_res, offsets_res, input_rows_count, regexp_jit_min_count); })
                Impl::vector(col->getChars(), col->getOffsets(), needle, vec_res, offsets_res, input_rows_count, regexp_jit_min_count);
            else
                Impl::vector(col->getChars(), col->getOffsets(), needle, vec_res, offsets_res, input_rows_count);

            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }

private:
    /// Compile-count threshold for JIT-compiling regular expressions, or `size_t(-1)` to disable.
    size_t regexp_jit_min_count = std::numeric_limits<size_t>::max();
};

}
