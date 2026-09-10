#pragma once

#include <limits>
#include <type_traits>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>
#include <Common/likePatternToRegexp.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool function_locate_has_mysql_compatible_argument_order;
    extern const SettingsBool compile_regular_expressions;
    extern const SettingsUInt64 min_count_to_compile_regular_expression;
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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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

    /// Compile-count threshold for JIT-compiling regular expressions, or `size_t(-1)` to disable.
    size_t regexp_jit_min_count = std::numeric_limits<size_t>::max();

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

        /// When JIT compilation of simple regular expressions is enabled, the impl receives the
        /// compile-count threshold; otherwise it gets a sentinel that disables the JIT path.
        if (context && context->getSettingsRef()[Setting::compile_regular_expressions])
            regexp_jit_min_count = context->getSettingsRef()[Setting::min_count_to_compile_regular_expression];
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2 || 3 < arguments.size())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                getName(), arguments.size());

        const auto & haystack_type = (argument_order == ArgumentOrder::HaystackNeedle) ? arguments[0] : arguments[1];
        const auto & needle_type = (argument_order == ArgumentOrder::HaystackNeedle) ? arguments[1] : arguments[0];

        if (!(isStringOrFixedString(haystack_type) || isEnum(haystack_type)))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                haystack_type->getName(), getName());

        if (!isString(needle_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                needle_type->getName(), getName());

        if (arguments.size() >= 3)
        {
            if constexpr (ImplIsLike<Impl>::value)
            {
                /// 3rd argument for LIKE is the ESCAPE character (String)
                if (!isString(arguments[2]))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of the ESCAPE argument of function {}",
                        arguments[2]->getName(), getName());
            }
            else
            {
                if (!isUInt(arguments[2]))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument of function {}",
                        arguments[2]->getName(), getName());
            }
        }

        auto return_type = std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
        if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
            return makeNullable(return_type);

        return return_type;
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    /// A value in `value_to_index` (see `buildEnumHaystackDictionary`) that marks a slot which does
    /// not correspond to any declared enum value - either a hole inside `[min_value, max_value]`, or
    /// (conceptually) a value outside that range. Undefined enum codes can still reach the column
    /// through binary deserialization (`SerializationEnum` inherits its binary format from
    /// `SerializationNumber`, which stores the raw code without validation), so such codes must be
    /// detected and rejected, exactly as the `castColumn(..., String)` path does via `getNameForValue`.
    static constexpr UInt32 enum_undefined_index = std::numeric_limits<UInt32>::max();

    /// Builds a deduplicated haystack for an `Enum` column, as requested in issue #73114:
    /// a `String` column holding the distinct enum names (in numeric-value order) and a plain
    /// array `value_to_index`, indexed by `enum_value - min_value`, that maps each enum value to
    /// the row of its name in the deduplicated column. `min_value` receives the smallest enum value.
    /// Slots for undefined codes are filled with `enum_undefined_index` so the caller can reject them.
    /// A plain array is used instead of a hash map because `Enum` values are at most `Int16`.
    template <typename EnumType>
    static ColumnPtr buildEnumHaystackDictionary(
        const EnumType & enum_type, PaddedPODArray<UInt32> & value_to_index, Int64 & min_value)
    {
        const auto & values = enum_type.getValues();

        /// `values` is sorted by numeric value, so the first and last elements give the min and max.
        min_value = static_cast<Int64>(values.front().second);
        const Int64 max_value = static_cast<Int64>(values.back().second);

        value_to_index.resize_fill(static_cast<size_t>(max_value - min_value) + 1, enum_undefined_index);

        auto distinct_names = ColumnString::create();
        distinct_names->reserve(values.size());
        for (UInt32 i = 0; i < values.size(); ++i)
        {
            const auto & enum_name = values[i].first;
            distinct_names->insertData(enum_name.data(), enum_name.size());
            value_to_index[static_cast<size_t>(static_cast<Int64>(values[i].second) - min_value)] = i;
        }
        return distinct_names;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto & haystack_argument = (argument_order == ArgumentOrder::HaystackNeedle) ? arguments[0] : arguments[1];
        ColumnPtr column_haystack = haystack_argument.column;
        const ColumnPtr & column_needle = (argument_order == ArgumentOrder::HaystackNeedle) ? arguments[1].column : arguments[0].column;

        ColumnPtr column_start_pos = nullptr;
        if constexpr (!ImplIsLike<Impl>::value)
        {
            if (arguments.size() >= 3)
                column_start_pos = arguments[2].column;
        }

        /// Optimization for `Enum` haystacks (https://github.com/ClickHouse/ClickHouse/issues/73114):
        /// when the needle and the start position are the same for every row, run the search only over
        /// the distinct enum names and map the results back per row through a plain array indexed by the
        /// enum value, instead of materializing and searching the whole column row by row.
        /// This block sits at the top of the function, where the non-optimized path calls
        /// `castColumn(..., String)`, so that undefined stored enum codes are reported eagerly, before
        /// any needle validation (ESCAPE arguments, token separators, ...) - preserving the exception
        /// ordering of the non-optimized path when a query has several errors at once.
        const size_t origin_input_rows_count = input_rows_count;
        bool enum_needs_transform = false;
        Int64 enum_min_value = 0;
        PaddedPODArray<UInt32> enum_value_to_index;
        ColumnPtr enum_source_column;

        if (isEnum(haystack_argument.type))
        {
            /// The ESCAPE rewrite below preserves the constness of the needle,
            /// so the decision can be made on the original needle column.
            const bool needle_is_const = isColumnConst(*column_needle);
            const bool start_pos_is_const = !column_start_pos || isColumnConst(*column_start_pos);

            if (needle_is_const && start_pos_is_const && !isColumnConst(*column_haystack))
            {
                const auto try_transform = [&](const auto & enum_type, const auto & enum_data)
                {
                    const auto & values = enum_type.getValues();

                    /// `values` is sorted by numeric value, so the first and last elements give the min and max.
                    const Int64 min_value = static_cast<Int64>(values.front().second);
                    const Int64 max_value = static_cast<Int64>(values.back().second);
                    /// `buildEnumHaystackDictionary` zero-fills a dense `value_to_index` array of this many slots.
                    const size_t span = static_cast<size_t>(max_value - min_value) + 1;
                    const size_t num_names = values.size();
                    const size_t num_rows = column_haystack->size();

                    /// The fast path searches `num_names` distinct names instead of `num_rows` rows, but it first
                    /// pays an `O(span)` dense-array fill (`span == max_value - min_value + 1`), which can dwarf
                    /// `num_names` for a sparse wide `Enum16` such as `Enum16('a' = -30000, 'b' = 30000)` (span
                    /// 60001, 2 names). Take the fast path only when it actually saves work: there must be more
                    /// rows than distinct names (otherwise fewer searches are not fewer), and the dense-array
                    /// setup must not exceed the per-row work the `castColumn` path would do anyway
                    /// (`span <= num_rows`). This keeps a small block over a wide sparse enum on the unchanged
                    /// `castColumn` path instead of clearing a huge array to search a handful of rows. The
                    /// decision is per-block, so a wide enum still takes the fast path once a block is large
                    /// enough for the amortized search savings to cover the fill.
                    if (num_rows <= num_names || span > num_rows)
                        return;

                    ColumnPtr distinct_names = buildEnumHaystackDictionary(enum_type, enum_value_to_index, enum_min_value);

                    /// Validate the stored codes eagerly, exactly where `castColumn(..., String)` would
                    /// have validated them (undefined codes can arrive through binary deserialization,
                    /// see `enum_undefined_index`).
                    const Int64 enum_max_shifted = static_cast<Int64>(enum_value_to_index.size()) - 1;
                    for (size_t i = 0; i < enum_data.size(); ++i)
                    {
                        const Int64 shifted = static_cast<Int64>(enum_data[i]) - enum_min_value;
                        if (shifted < 0 || shifted > enum_max_shifted
                            || enum_value_to_index[static_cast<size_t>(shifted)] == enum_undefined_index)
                        {
                            /// Throws `UNKNOWN_ELEMENT_OF_ENUM`, matching the `castColumn` behavior.
                            enum_type.getNameForValue(enum_data[i]);
                        }
                    }

                    enum_source_column = column_haystack;
                    column_haystack = std::move(distinct_names);
                    input_rows_count = column_haystack->size();
                    enum_needs_transform = true;
                };

                if (const auto * enum8 = typeid_cast<const DataTypeEnum8 *>(haystack_argument.type.get()))
                {
                    if (const auto * col_enum8 = checkAndGetColumn<ColumnVector<Int8>>(column_haystack.get()))
                        try_transform(*enum8, col_enum8->getData());
                }
                else if (const auto * enum16 = typeid_cast<const DataTypeEnum16 *>(haystack_argument.type.get()))
                {
                    if (const auto * col_enum16 = checkAndGetColumn<ColumnVector<Int16>>(column_haystack.get()))
                        try_transform(*enum16, col_enum16->getData());
                }
            }

            if (!enum_needs_transform)
                column_haystack = castColumn(haystack_argument, std::make_shared<DataTypeString>());
        }

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
        {
            const String needle = col_needle_const->getValue<String>();
            const auto & haystack_chars = col_haystack_vector->getChars();
            const auto & haystack_offsets = col_haystack_vector->getOffsets();
            /// Only impls that opt in (currently `MatchImpl`) take the JIT compile-count threshold;
            /// all others keep their original signature.
            if constexpr (requires { Impl::vectorConstant(haystack_chars, haystack_offsets, needle, column_start_pos, vec_res, null_map.get(), input_rows_count, regexp_jit_min_count); })
                Impl::vectorConstant(
                    haystack_chars, haystack_offsets, needle, column_start_pos,
                    vec_res, null_map.get(), input_rows_count, regexp_jit_min_count);
            else
                Impl::vectorConstant(
                    haystack_chars, haystack_offsets, needle, column_start_pos,
                    vec_res, null_map.get(), input_rows_count);
        }
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

        /// Map the per-distinct-name results back to the original rows of the `Enum` column.
        if (enum_needs_transform)
        {
            auto final_res = ColumnVector<ResultType>::create();
            auto & final_vec_res = final_res->getData();
            final_vec_res.resize(origin_input_rows_count);

            ColumnUInt8::MutablePtr final_null_map;
            if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
                final_null_map = ColumnUInt8::create(origin_input_rows_count);

            /// All stored codes were validated eagerly when the dictionary was built,
            /// so plain indexing is safe here.
            const auto expand = [&](const auto & enum_data)
            {
                for (size_t i = 0; i < origin_input_rows_count; ++i)
                {
                    const UInt32 idx = enum_value_to_index[static_cast<size_t>(static_cast<Int64>(enum_data[i]) - enum_min_value)];
                    final_vec_res[i] = vec_res[idx];
                    if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
                        final_null_map->getData()[i] = null_map->getData()[idx];
                }
            };

            if (const auto * col_enum8 = typeid_cast<const ColumnVector<Int8> *>(enum_source_column.get()))
                expand(col_enum8->getData());
            else
                expand(assert_cast<const ColumnVector<Int16> &>(*enum_source_column).getData());

            col_res = std::move(final_res);
            if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
                null_map = std::move(final_null_map);
        }

        if constexpr (execution_error_policy == ExecutionErrorPolicy::Null)
            return ColumnNullable::create(std::move(col_res), std::move(null_map));

        return col_res;
    }
};

}
