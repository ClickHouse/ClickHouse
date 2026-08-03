#pragma once

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Common/CurrentThread.h>

#include <functional>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int TIMEOUT_EXCEEDED;
}

namespace Setting
{
    extern const SettingsBool compile_regular_expressions;
    extern const SettingsUInt64 min_count_to_compile_regular_expression;
}

template <typename Impl, typename Name>
class FunctionStringReplace final : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionStringReplace>(context); }

    explicit FunctionStringReplace(ContextPtr context)
    {
        if (context && context->getSettingsRef()[Setting::compile_regular_expressions])
            regexp_jit_min_count = context->getSettingsRef()[Setting::min_count_to_compile_regular_expression];
    }

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
        /// Resolved from the executing thread rather than captured: this instance can be stored in table
        /// metadata and then run by any later query.
        QueryStatusPtr process_list_element;
        if (auto query_context = CurrentThread::tryGetQueryContext())
            process_list_element = query_context->getProcessListElementSafe();

        /// `checkTimeLimit` returns false rather than throwing under the `break` overflow mode, but a
        /// partially rewritten string is a wrong value rather than a shorter one, so it becomes a throw.
        std::function<void()> check_cancellation;
        if (process_list_element)
            check_cancellation = [&process_list_element]
            {
                if (!process_list_element->checkTimeLimit())
                    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded: elapsed time limit reached in function {}", name);
            };

        /// Before materialization: expanding a constant into a full column is itself unbounded.
        if (check_cancellation)
            check_cancellation();

        ColumnPtr column_haystack = arguments[0].column;
        column_haystack = column_haystack->convertToFullColumnIfConst();

        /// After materialization, so that a deadline crossed by the expansion above is observed too.
        if (check_cancellation)
            check_cancellation();

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
            const String needle = col_needle_const->getValue<String>();
            const String replacement = col_replacement_const->getValue<String>();
            auto & res_chars = col_res->getChars();
            auto & res_offsets = col_res->getOffsets();
            /// Only impls that opt in (currently `ReplaceRegexpImpl`) take the JIT compile-count threshold.
            if constexpr (requires { Impl::vectorConstantConstant(col_haystack->getChars(), col_haystack->getOffsets(), needle, replacement, res_chars, res_offsets, input_rows_count, regexp_jit_min_count, check_cancellation); })
                Impl::vectorConstantConstant(
                    col_haystack->getChars(), col_haystack->getOffsets(), needle, replacement,
                    res_chars, res_offsets, input_rows_count, regexp_jit_min_count, check_cancellation);
            else
                Impl::vectorConstantConstant(
                    col_haystack->getChars(), col_haystack->getOffsets(), needle, replacement,
                    res_chars, res_offsets, input_rows_count, check_cancellation);
        }
        else if (col_haystack && col_needle_vector && col_replacement_const)
        {
            Impl::vectorVectorConstant(
                col_haystack->getChars(),
                col_haystack->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                col_replacement_const->getValue<String>(),
                col_res->getChars(),
                col_res->getOffsets(),
                input_rows_count,
                check_cancellation);
        }
        else if (col_haystack && col_needle_const && col_replacement_vector)
        {
            Impl::vectorConstantVector(
                col_haystack->getChars(),
                col_haystack->getOffsets(),
                col_needle_const->getValue<String>(),
                col_replacement_vector->getChars(),
                col_replacement_vector->getOffsets(),
                col_res->getChars(),
                col_res->getOffsets(),
                input_rows_count,
                check_cancellation);
        }
        else if (col_haystack && col_needle_vector && col_replacement_vector)
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
                input_rows_count,
                check_cancellation);
        }
        else if (col_haystack_fixed && col_needle_const && col_replacement_const)
        {
            Impl::vectorFixedConstantConstant(
                col_haystack_fixed->getChars(),
                col_haystack_fixed->getN(),
                col_needle_const->getValue<String>(),
                col_replacement_const->getValue<String>(),
                col_res->getChars(),
                col_res->getOffsets(),
                input_rows_count,
                check_cancellation);
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());

        /// One check on the single exit path, covering every fast path that copies the column in one bulk
        /// operation and returns without reaching a throttled loop, including any added later.
        if (check_cancellation)
            check_cancellation();

        return col_res;
    }

private:
    /// Compile-count threshold for JIT-compiling regular expressions, or `size_t(-1)` to disable.
    size_t regexp_jit_min_count = std::numeric_limits<size_t>::max();
};

}
