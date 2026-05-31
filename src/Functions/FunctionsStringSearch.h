#pragma once

#include <type_traits>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Common/likePatternToRegexp.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool function_locate_has_mysql_compatible_argument_order;
}

/** Search and replace functions in strings:
  * position(haystack, needle)     - the normal search for a substring in a string, returns the position (in bytes) of the found substring starting with 1, or 0 if no substring is found.
  * positionUTF8(haystack, needle) - the same, but the position is calculated at code points, provided that the string is encoded in UTF-8.
  * positionCaseInsensitive(haystack, needle)
  * positionCaseInsensitiveUTF8(haystack, needle)
  *
  * like(haystack, needle)        - search by the regular expression LIKE; Returns 0 or 1. Case-insensitive, but only for Latin.
  * notLike(haystack, needle)
  *
  * ilike(haystack, needle) - like 'like' but case-insensitive
  * notIlike(haystack, needle)
  *
  * match(haystack, needle)       - search by regular expression re2; Returns 0 or 1.
  *
  * countSubstrings(haystack, needle) -- count number of occurrences of needle in haystack.
  * countSubstringsCaseInsensitive(haystack, needle)
  * countSubstringsCaseInsensitiveUTF8(haystack, needle)
  *
  * hasToken()
  * hasTokenCaseInsensitive()
  *
  * JSON stuff:
  * visitParamExtractBool()
  * simpleJSONExtractBool()
  * visitParamExtractFloat()
  * simpleJSONExtractFloat()
  * visitParamExtractInt()
  * simpleJSONExtractInt()
  * visitParamExtractUInt()
  * simpleJSONExtractUInt()
  * visitParamHas()
  * simpleJSONHas()
  *
  * Applies regexp re2 and pulls:
  * - the first subpattern, if the regexp has a subpattern;
  * - the zero subpattern (the match part, otherwise);
  * - if not match - an empty string.
  * extract(haystack, needle)
  */

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

enum class ExecutionErrorPolicy : uint8_t
{
    Null,
    Throw
};

enum class HaystackNeedleOrderIsConfigurable : uint8_t
{
    No,     /// function arguments are always: (haystack, needle[, position])
    Yes     /// depending on a setting, the function arguments are (haystack, needle[, position]) or (needle, haystack[, position])
};

/// Detects whether `Impl` is a LIKE-style search implementation that supports the ESCAPE clause.
/// Uses a trait detection on `Impl::is_like` rather than a partial specialization on `MatchImpl`,
/// so this header does not need to pull in the heavy `MatchImpl` machinery.
template <typename T, typename = void>
struct ImplIsLike : std::false_type {};

template <typename T>
struct ImplIsLike<T, std::void_t<decltype(T::is_like)>> : std::bool_constant<T::is_like> {};

template <typename Impl,
         ExecutionErrorPolicy execution_error_policy = ExecutionErrorPolicy::Throw,
         HaystackNeedleOrderIsConfigurable haystack_needle_order_is_configurable = HaystackNeedleOrderIsConfigurable::No>
class FunctionsStringSearch final : public IFunction
{
private:
    enum class ArgumentOrder : uint8_t
    {
        HaystackNeedle,
        NeedleHaystack
    };

    ArgumentOrder argument_order = ArgumentOrder::HaystackNeedle;

public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionsStringSearch>(context); }

    explicit FunctionsStringSearch([[maybe_unused]] ContextPtr context)
    {
        if constexpr (haystack_needle_order_is_configurable == HaystackNeedleOrderIsConfigurable::Yes)
        {
            if (context->getSettingsRef()[Setting::function_locate_has_mysql_compatible_argument_order])
                argument_order = ArgumentOrder::NeedleHaystack;
        }
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return Impl::supports_start_pos || ImplIsLike<Impl>::value; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override
    {
        if (Impl::supports_start_pos || ImplIsLike<Impl>::value)
            return 0;
        return 2;
    }

    bool useDefaultImplementationForConstants() const override { return Impl::use_default_implementation_for_constants; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return Impl::getArgumentsThatAreAlwaysConstant();
    }

    String getSignatureString() const override
    {
        const String elem = DataTypeNumber<typename Impl::ResultType>{}.getName();
        const String ret = (execution_error_policy == ExecutionErrorPolicy::Null)
            ? ("Nullable(" + elem + ")")
            : elem;
        const String haystack = (argument_order == ArgumentOrder::HaystackNeedle)
            ? "StringOrFixedString | Enum, String"
            : "String, StringOrFixedString | Enum";
        if constexpr (ImplIsLike<Impl>::value)
            /// The optional 3rd argument for LIKE is the ESCAPE character (String).
            return "(" + haystack + ", [String]) -> " + ret;
        else if constexpr (Impl::supports_start_pos)
            return "(" + haystack + ", [NativeUInt]) -> " + ret;
        else
            return "(" + haystack + ") -> " + ret;
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto & haystack_argument = (argument_order == ArgumentOrder::HaystackNeedle) ? arguments[0] : arguments[1];
        ColumnPtr column_haystack = haystack_argument.column;
        const ColumnPtr & column_needle = (argument_order == ArgumentOrder::HaystackNeedle) ? arguments[1].column : arguments[0].column;

        if (isEnum(haystack_argument.type))
            column_haystack = castColumn(haystack_argument, std::make_shared<DataTypeString>());

        ColumnPtr column_start_pos = nullptr;
        ColumnPtr column_needle_rewritten;

        if constexpr (ImplIsLike<Impl>::value)
        {
            /// Is there an ESCAPE argument? Rewrite the needle with escape character into one without escape character.
            if (arguments.size() >= 3)
            {
                /// Extract escape character
                const auto * col_escape = typeid_cast<const ColumnConst *>(arguments[2].column.get());
                if (!col_escape)
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "The ESCAPE argument of function {} must be constant",
                        getName());
                const String escape_str = col_escape->getValue<String>();
                if (escape_str.size() != 1 || static_cast<unsigned char>(escape_str[0]) > 0x7F)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "The ESCAPE argument of function {} must be a single ASCII character, got '{}'",
                        getName(), escape_str);
                char escape_char = escape_str[0];

                /// Rewrite the needle from custom escape to standard backslash escape
                if (const auto * col_needle_const = typeid_cast<const ColumnConst *>(column_needle.get()))
                {
                    String rewritten_needle = likePatternWithCustomEscapeToLikePattern(col_needle_const->getValue<String>(), escape_char);
                    auto rewritten_needle_col = ColumnString::create();
                    rewritten_needle_col->insertData(rewritten_needle.data(), rewritten_needle.size());
                    column_needle_rewritten = ColumnConst::create(std::move(rewritten_needle_col), col_needle_const->size());
                }
                else if (const auto * col_needle_nonconst = typeid_cast<const ColumnString *>(column_needle.get()))
                {
                    auto rewritten_needle_col = ColumnString::create();
                    for (size_t i = 0; i < col_needle_nonconst->size(); ++i)
                    {
                        auto needle = col_needle_nonconst->getDataAt(i);
                        String rewritten = likePatternWithCustomEscapeToLikePattern({needle.data(), needle.size()}, escape_char);
                        rewritten_needle_col->insertData(rewritten.data(), rewritten.size());
                    }
                    column_needle_rewritten = std::move(rewritten_needle_col);
                }
            }
        }
        else
        {
            if (arguments.size() >= 3)
                column_start_pos = arguments[2].column;
        }

        const ColumnPtr & effective_needle = column_needle_rewritten ? column_needle_rewritten : column_needle;

        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(&*effective_needle);

        using ResultType = typename Impl::ResultType;
        auto col_res = ColumnVector<ResultType>::create();
        auto & vec_res = col_res->getData();

        const auto create_null_map = [&]() -> ColumnUInt8::MutablePtr
        {
            if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
                return ColumnUInt8::create(vec_res.size());

            return {};
        };

        if constexpr (!Impl::use_default_implementation_for_constants)
        {
            if (col_haystack_const && col_needle_const)
            {
                auto column_start_position_arg = column_start_pos;
                bool is_col_start_pos_const = false;
                if (column_start_pos)
                {
                    if (const ColumnConst * const_column_start_pos = typeid_cast<const ColumnConst *>(&*column_start_pos))
                    {
                        is_col_start_pos_const = true;
                        column_start_position_arg = const_column_start_pos->getDataColumnPtr();
                    }
                }
                else
                    is_col_start_pos_const = true;

                vec_res.resize(is_col_start_pos_const ? 1 : column_start_pos->size());
                const auto null_map = create_null_map();

                Impl::constantConstant(
                    col_haystack_const->getValue<String>(),
                    col_needle_const->getValue<String>(),
                    column_start_position_arg,
                    vec_res,
                    null_map.get());

                if (is_col_start_pos_const)
                    return result_type->createColumnConst(col_haystack_const->size(), toField(vec_res[0]));
                return col_res;
            }
        }

        vec_res.resize(column_haystack->size());
        auto null_map = create_null_map();

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);
        const ColumnFixedString * col_haystack_vector_fixed = checkAndGetColumn<ColumnFixedString>(&*column_haystack);
        const ColumnString * col_needle_vector = checkAndGetColumn<ColumnString>(&*effective_needle);

        if (col_haystack_vector && col_needle_vector)
            Impl::vectorVector(
                col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                column_start_pos,
                vec_res,
                null_map.get(),
                input_rows_count);
        else if (col_haystack_vector && col_needle_const)
            Impl::vectorConstant(
                col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                col_needle_const->getValue<String>(),
                column_start_pos,
                vec_res,
                null_map.get(),
                input_rows_count);
        else if (col_haystack_vector_fixed && col_needle_vector)
            Impl::vectorFixedVector(
                col_haystack_vector_fixed->getChars(),
                col_haystack_vector_fixed->getN(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                column_start_pos,
                vec_res,
                null_map.get(),
                input_rows_count);
        else if (col_haystack_vector_fixed && col_needle_const)
            Impl::vectorFixedConstant(
                col_haystack_vector_fixed->getChars(),
                col_haystack_vector_fixed->getN(),
                col_needle_const->getValue<String>(),
                vec_res,
                null_map.get(),
                input_rows_count);
        else if (col_haystack_const && col_needle_vector)
            Impl::constantVector(
                col_haystack_const->getValue<String>(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                column_start_pos,
                vec_res,
                null_map.get(),
                input_rows_count);
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal columns {} and {} of arguments of function {}",
                arguments[0].column->getName(),
                arguments[1].column->getName(),
                getName());

        if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
            return ColumnNullable::create(std::move(col_res), std::move(null_map));

        return col_res;
    }
};

}
