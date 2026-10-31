#include <Analyzer/Passes/OptimizeStringConversionComparisonPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>

#include <bitset>
#include <optional>
#include <string_view>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_prune_impossible_string_comparisons;
    extern const SettingsBool optimize_destructure_tuple_string_comparisons;
    extern const SettingsDateTimeOutputFormat date_time_output_format;
}

namespace
{

/// Set of characters (single bytes) that may appear in the text representation of a value.
using PossibleChars = std::bitset<256>;

void addChars(PossibleChars & chars, std::string_view str)
{
    for (char c : str)
        chars.set(static_cast<UInt8>(c));
}

/// Whether values of the type are rendered wrapped in single quotes inside composite types (Array, Tuple, Map),
/// i.e. whether `serializeTextQuoted` differs from `serializeText` for it.
/// String-like types are also quoted, but they never reach this check because their character set is unrestricted.
bool isQuotedInsideCompositeTypes(const DataTypePtr & type)
{
    DataTypePtr unwrapped = removeLowCardinality(type);
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(unwrapped.get()))
        unwrapped = nullable->getNestedType();

    WhichDataType which(unwrapped);
    return which.isDateOrDate32() || which.isDateTimeOrDateTime64() || which.isTimeOrTime64() || which.isUUID();
}

/// Whether the type's quoted-and-escaped rendering inside a composite type (Array, Tuple, Map) cannot be reasoned
/// about: `Dynamic`, `Variant` and `JSON` (`Object`) can hold an arbitrary underlying value at runtime (including
/// a String), which goes through the same quoting and escaping as a top-level String element (see
/// `SerializationDynamic`/`SerializationVariant`), but unlike a statically-typed String element we have no way to
/// tell in advance whether a given needle could only match the escaped rendering and not the raw value.
bool hasUnknownQuotingSemantics(const DataTypePtr & type)
{
    DataTypePtr unwrapped = removeLowCardinality(type);
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(unwrapped.get()))
        unwrapped = nullable->getNestedType();

    WhichDataType which(unwrapped);
    return which.isDynamic() || which.isVariant() || which.isObject();
}

/// Whether the type's rendering as a tuple element (via `serializeTextQuoted`) can differ from its rendering
/// by a standalone `toString`/`CAST(..., 'String')` call on the same value, depending on the `date_time_output_format`
/// setting. `SerializationDateTime`/`SerializationDateTime64` honor that setting, while `FormatImpl<DataTypeDateTime>`/
/// `FormatImpl<DataTypeDateTime64>` (used by the scalar conversion) always render the fixed `YYYY-MM-DD hh:mm:ss` form.
/// `Date`/`Date32`/`Time`/`Time64` are unaffected: their serialization does not branch on this setting.
bool hasSettingDependentCompositeRendering(const DataTypePtr & type)
{
    DataTypePtr unwrapped = removeLowCardinality(type);
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(unwrapped.get()))
        unwrapped = nullable->getNestedType();

    WhichDataType which(unwrapped);
    return which.isDateTimeOrDateTime64();
}

/** Compute the set of characters that may appear in the text representation (`toString` / `CAST(..., 'String')`)
  * of values of the given type. Returns std::nullopt if the type permits arbitrary characters (e.g. String, Enum)
  * or its representation is not analyzed (e.g. Bool, whose representation is configurable by settings).
  * The result is a conservative over-approximation: it is only guaranteed that characters outside
  * of the set can never appear.
  *
  * `inside_composite` means the type is rendered as an element of a composite type (Array, Tuple, Map).
  * The distinction matters for DateTime and DateTime64: the scalar `toString`/`CAST` conversion always renders
  * them in the simple format `YYYY-MM-DD hh:mm:ss`, while elements of composite types are rendered through
  * the serialization, which honors the `date_time_output_format` setting.
  */
std::optional<PossibleChars> getPossibleChars(const DataTypePtr & type, FormatSettings::DateTimeOutputFormat date_time_output_format, bool inside_composite = false)
{
    static constexpr std::string_view digits = "0123456789";

    DataTypePtr unwrapped = removeLowCardinality(type);
    WhichDataType which(unwrapped);

    PossibleChars chars;

    /// Bool is UInt8 with a custom text representation, configurable by the settings
    /// output_format_bool_true_representation and output_format_bool_false_representation.
    if (isBool(unwrapped))
        return std::nullopt;

    if (which.isUInt())
    {
        addChars(chars, digits);
    }
    else if (which.isInt())
    {
        addChars(chars, digits);
        addChars(chars, "-");
    }
    else if (which.isDecimal())
    {
        addChars(chars, digits);
        addChars(chars, "-.");
    }
    else if (which.isFloat())
    {
        /// Digits, sign, decimal point, exponent, and the special values `inf` and `nan`.
        addChars(chars, digits);
        addChars(chars, "-+.eEinfa");
    }
    else if (which.isDateOrDate32())
    {
        /// YYYY-MM-DD, independent of date_time_output_format.
        addChars(chars, digits);
        addChars(chars, "-");
    }
    else if (which.isDateTime() || which.isDateTime64())
    {
        addChars(chars, digits);
        if (!inside_composite)
        {
            /// The scalar toString/CAST conversion always renders DateTime in the simple format YYYY-MM-DD hh:mm:ss,
            /// regardless of the date_time_output_format setting.
            addChars(chars, "-: ");
        }
        else
        {
            switch (date_time_output_format)
            {
                case FormatSettings::DateTimeOutputFormat::Simple:
                    /// YYYY-MM-DD hh:mm:ss
                    addChars(chars, "-: ");
                    break;
                case FormatSettings::DateTimeOutputFormat::ISO:
                    /// YYYY-MM-DDThh:mm:ssZ
                    addChars(chars, "-:TZ");
                    break;
                case FormatSettings::DateTimeOutputFormat::UnixTimestamp:
                    /// DateTime64 timestamps can be negative (before 1970).
                    addChars(chars, "-");
                    break;
            }
        }
        if (which.isDateTime64())
            addChars(chars, ".");
    }
    else if (which.isTimeOrTime64())
    {
        /// hhh:mm:ss, possibly negative, with a fractional part for Time64.
        addChars(chars, digits);
        addChars(chars, "-:");
        if (which.isTime64())
            addChars(chars, ".");
    }
    else if (which.isUUID())
    {
        addChars(chars, digits);
        addChars(chars, "abcdef-");
    }
    else if (which.isNullable())
    {
        const auto & nested_type = typeid_cast<const DataTypeNullable &>(*unwrapped).getNestedType();
        auto nested_chars = getPossibleChars(nested_type, date_time_output_format, inside_composite);
        if (!nested_chars)
            return std::nullopt;
        chars = *nested_chars;
        addChars(chars, "NUL");
    }
    else if (which.isArray())
    {
        const auto & nested_type = typeid_cast<const DataTypeArray &>(*unwrapped).getNestedType();
        auto nested_chars = getPossibleChars(nested_type, date_time_output_format, /*inside_composite=*/ true);
        if (!nested_chars)
            return std::nullopt;
        chars = *nested_chars;
        addChars(chars, "[],");
        if (isQuotedInsideCompositeTypes(nested_type))
            addChars(chars, "'");
    }
    else if (which.isTuple())
    {
        const auto & element_types = typeid_cast<const DataTypeTuple &>(*unwrapped).getElements();
        for (const auto & element_type : element_types)
        {
            auto element_chars = getPossibleChars(element_type, date_time_output_format, /*inside_composite=*/ true);
            if (!element_chars)
                return std::nullopt;
            chars |= *element_chars;
            if (isQuotedInsideCompositeTypes(element_type))
                addChars(chars, "'");
        }
        addChars(chars, "(),");
    }
    else if (which.isMap())
    {
        const auto & map_type = typeid_cast<const DataTypeMap &>(*unwrapped);
        for (const auto & nested_type : {map_type.getKeyType(), map_type.getValueType()})
        {
            auto nested_chars = getPossibleChars(nested_type, date_time_output_format, /*inside_composite=*/ true);
            if (!nested_chars)
                return std::nullopt;
            chars |= *nested_chars;
            if (isQuotedInsideCompositeTypes(nested_type))
                addChars(chars, "'");
        }
        addChars(chars, "{}:,");
    }
    else
    {
        return std::nullopt;
    }

    return chars;
}

/** Extract the characters of the string that a LIKE pattern requires to be literally present in a matching string.
  * Wildcards `%` and `_` are skipped, backslash-escaped characters are taken literally.
  *
  * Returns std::nullopt if the pattern is invalid, i.e. it ends with an unescaped trailing backslash: such a pattern
  * makes `likePatternToRegexp` raise `CANNOT_PARSE_ESCAPE_SEQUENCE` at execution time, so it must not be folded here.
  */
std::optional<String> extractLikeRequiredChars(std::string_view pattern)
{
    String result;
    for (size_t i = 0; i < pattern.size(); ++i)
    {
        char c = pattern[i];
        if (c == '\\')
        {
            if (i + 1 >= pattern.size())
                return std::nullopt;
            result += pattern[++i];
        }
        else if (c != '%' && c != '_')
        {
            result += c;
        }
    }
    return result;
}

/** If the LIKE pattern has the form `%needle%` (any positive number of `%` on both sides, a single literal run,
  * no `_` wildcards), return the needle with the original escaping preserved. Otherwise return std::nullopt.
  * A pattern of this form matches iff the needle occurs somewhere in the string, so it is the only form
  * that can be checked per tuple element independently.
  */
std::optional<String> tryExtractEnclosedNeedle(std::string_view pattern)
{
    size_t begin = 0;
    while (begin < pattern.size() && pattern[begin] == '%')
        ++begin;

    size_t end = pattern.size();
    while (end > begin && pattern[end - 1] == '%')
    {
        /// A trailing `%` preceded by a backslash is an escaped literal percent, not a wildcard.
        /// Escaped backslashes complicate counting, so just refuse such patterns.
        if (end >= 2 && pattern[end - 2] == '\\')
            return std::nullopt;
        --end;
    }

    if (begin == 0 || end == pattern.size() || begin == end)
        return std::nullopt;

    /// The needle must be a single literal run: no unescaped wildcards inside.
    for (size_t i = begin; i < end; ++i)
    {
        if (pattern[i] == '\\')
            ++i;
        else if (pattern[i] == '%' || pattern[i] == '_')
            return std::nullopt;
    }

    return String(pattern.substr(begin, end - begin));
}

char alternateCaseASCII(char c)
{
    if (c >= 'a' && c <= 'z')
        return c - 'a' + 'A';
    if (c >= 'A' && c <= 'Z')
        return c - 'A' + 'a';
    return c;
}

class OptimizeStringConversionComparisonVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeStringConversionComparisonVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeStringConversionComparisonVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        const auto & settings = getSettings();
        const bool prune_enabled = settings[Setting::optimize_prune_impossible_string_comparisons];
        const bool destructure_enabled = settings[Setting::optimize_destructure_tuple_string_comparisons];
        if (!prune_enabled && !destructure_enabled)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        enum class Op : uint8_t
        {
            Equals,
            Like,
            ILike,
            Position,
            PositionCaseInsensitive,
        };

        const String & function_name = function_node->getFunctionName();

        Op op = {};
        bool negated = false;

        if (function_name == "equals")
            op = Op::Equals;
        else if (function_name == "notEquals")
        {
            op = Op::Equals;
            negated = true;
        }
        else if (function_name == "like")
            op = Op::Like;
        else if (function_name == "notLike")
        {
            op = Op::Like;
            negated = true;
        }
        else if (function_name == "ilike")
            op = Op::ILike;
        else if (function_name == "notILike")
        {
            op = Op::ILike;
            negated = true;
        }
        else if (function_name == "position")
            op = Op::Position;
        else if (function_name == "positionCaseInsensitive")
            op = Op::PositionCaseInsensitive;
        else
            return;

        const auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 2)
            return;

        /// `equals` matches both argument orders, the other functions have a fixed haystack/needle order.
        QueryTreeNodePtr converted_argument;
        const ConstantNode * constant_node = nullptr;

        if (op == Op::Equals)
        {
            for (size_t i = 0; i < 2; ++i)
            {
                if (const auto * candidate_constant = arguments[i]->as<ConstantNode>())
                {
                    constant_node = candidate_constant;
                    converted_argument = arguments[1 - i];
                    break;
                }
            }
        }
        else
        {
            constant_node = arguments[1]->as<ConstantNode>();
            converted_argument = arguments[0];
        }

        if (!constant_node || !converted_argument)
            return;

        const Field constant_value = constant_node->getValue();
        if (constant_value.getType() != Field::Types::String)
            return;
        const String & constant_string = constant_value.safeGet<String>();

        QueryTreeNodePtr inner_argument = tryGetStringConversionArgument(converted_argument);
        if (!inner_argument)
            return;

        DataTypePtr inner_type = removeLowCardinality(inner_argument->getResultType());

        /// For a top-level Nullable argument the expression evaluates to NULL (not false) when the argument is NULL,
        /// so it cannot be replaced with a constant, and destructuring would change NULL to false in the result.
        if (inner_type->isNullable())
            return;

        const bool case_insensitive = (op == Op::ILike || op == Op::PositionCaseInsensitive);

        if (prune_enabled)
        {
            String required_chars;
            if (op == Op::Like || op == Op::ILike)
            {
                auto required_chars_opt = extractLikeRequiredChars(constant_string);
                /// An invalid pattern (trailing unescaped backslash) must keep raising its runtime exception.
                if (!required_chars_opt)
                    return;
                required_chars = std::move(*required_chars_opt);
            }
            else
                required_chars = constant_string;

            if (auto possible_chars = getPossibleChars(inner_type, settings[Setting::date_time_output_format]))
            {
                if (isImpossibleMatch(required_chars, *possible_chars, case_insensitive))
                {
                    auto result_type = function_node->getResultType();
                    WhichDataType result_which(result_type);

                    /// The result type must be exactly what we expect (it could be e.g. wrapped in Nullable
                    /// due to group_by_use_nulls); otherwise skip the optimization.
                    Field replacement_value;
                    if ((op == Op::Position || op == Op::PositionCaseInsensitive) && result_which.isUInt64())
                        replacement_value = UInt64(0);
                    else if (op != Op::Position && op != Op::PositionCaseInsensitive && result_which.isUInt8())
                        replacement_value = UInt8(negated ? 1 : 0);
                    else
                        return;

                    node = std::make_shared<ConstantNode>(std::move(replacement_value), result_type);
                    return;
                }
            }
        }

        if (destructure_enabled && (op == Op::Like || op == Op::ILike) && !negated)
            tryDestructureTuple(node, *function_node, inner_argument, inner_type, constant_string, op == Op::ILike);
    }

private:
    /// If the node is a conversion of some expression to String (`toString(x)` or `CAST(x, 'String')`),
    /// return the converted expression, otherwise nullptr.
    static QueryTreeNodePtr tryGetStringConversionArgument(const QueryTreeNodePtr & node)
    {
        const auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return nullptr;

        const String & function_name = function_node->getFunctionName();
        const auto & arguments = function_node->getArguments().getNodes();

        const bool is_to_string = function_name == "toString" && arguments.size() == 1;
        const bool is_cast = (function_name == "CAST" || function_name == "_CAST") && arguments.size() == 2;
        if (!is_to_string && !is_cast)
            return nullptr;

        if (!isString(removeLowCardinality(function_node->getResultType())))
            return nullptr;

        return arguments[0];
    }

    static bool isImpossibleMatch(const String & required_chars, const PossibleChars & possible_chars, bool case_insensitive)
    {
        for (char c : required_chars)
        {
            UInt8 code = static_cast<UInt8>(c);
            if (case_insensitive)
            {
                /// Unicode case folding of non-ASCII characters is not modeled (e.g. the Kelvin sign can match `k`),
                /// so we cannot conclude anything about them. ASCII characters are unambiguous in UTF-8, so the rest
                /// of the pattern can still be analyzed byte-wise.
                if (code >= 0x80)
                    continue;
                if (!possible_chars[code] && !possible_chars[static_cast<UInt8>(alternateCaseASCII(c))])
                    return true;
            }
            else
            {
                if (!possible_chars[code])
                    return true;
            }
        }
        return false;
    }

    /** Rewrite `toString(tuple) LIKE '%needle%'` into `toString(x) LIKE '%needle%' OR toString(y) LIKE '%needle%' OR ...`.
      *
      * The rewrite is only performed when a match of the pattern must fall entirely within the rendering of a single
      * tuple element:
      *   - the pattern must have the form `%needle%` with a single literal needle and no `_` wildcards
      *     (a `_` could match an element separator, and multiple literal runs could match in different elements);
      *   - the needle must not contain `(`, `)`, `,` (every boundary between elements in the rendered tuple contains
      *     a comma, so a needle without these characters cannot span two elements), nor `'`, `\\` and control characters
      *     (which could match the quoting and escaping of quoted elements);
      *   - if some element is a String or FixedString, the needle must not start with one of `b`, `f`, `n`, `r`,
      *     `t`, `0` (see below).
      *
      * For String elements the rewritten condition matches the raw value, while the original expression matches the
      * quoted and escaped rendering inside the tuple. Escaping (see `writeAnyEscapedString`) can only insert the
      * characters `\\`, `'` and the letters of the escape sequences `\\b`, `\\f`, `\\n`, `\\r`, `\\t`, `\\0`; all other
      * characters pass through verbatim, so an occurrence of the needle in the raw value is also present in the
      * escaped rendering. In the other direction, a match in the escaped rendering that is absent from the raw value
      * must overlap a character inserted by escaping; it cannot overlap `\\` or `'` (the needle does not contain
      * them), so it must start exactly at the second character of an escape pair, which requires the first character
      * of the needle to be one of `bfnrt0`. Refusing such needles (in both cases for ILIKE) makes the rewrite exact.
      */
    void tryDestructureTuple(
        QueryTreeNodePtr & node,
        const FunctionNode & function_node,
        const QueryTreeNodePtr & inner_argument,
        const DataTypePtr & inner_type,
        const String & pattern,
        bool is_case_insensitive)
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(inner_type.get());
        if (!tuple_type || tuple_type->getElements().empty())
            return;

        /// NULL elements would change the result of the whole disjunction from false to NULL.
        /// Dynamic/Variant/JSON elements are excluded because we cannot tell in advance whether a needle can only
        /// match their quoted-and-escaped rendering inside the tuple and not their runtime value (see
        /// `hasUnknownQuotingSemantics`). DateTime/DateTime64 elements are excluded because the rewritten predicate
        /// wraps them in a scalar `toString`, which renders differently from their rendering inside the tuple
        /// whenever `date_time_output_format` is not `simple` (see `hasSettingDependentCompositeRendering`).
        for (const auto & element_type : tuple_type->getElements())
            if (removeLowCardinality(element_type)->isNullable() || hasUnknownQuotingSemantics(element_type)
                || hasSettingDependentCompositeRendering(element_type))
                return;

        auto needle = tryExtractEnclosedNeedle(pattern);
        if (!needle)
            return;

        auto unescaped_needle_opt = extractLikeRequiredChars(*needle);
        if (!unescaped_needle_opt)
            return;
        const String & unescaped_needle = *unescaped_needle_opt;
        for (char c : unescaped_needle)
        {
            if (c == '(' || c == ')' || c == ',' || c == '\'' || c == '\\' || static_cast<UInt8>(c) < 0x20)
                return;
        }

        bool has_string_elements = false;
        for (const auto & element_type : tuple_type->getElements())
            has_string_elements |= isStringOrFixedString(removeLowCardinality(element_type));

        if (has_string_elements)
        {
            /// A needle starting with one of the escape sequence letters could match the escaped rendering of
            /// a String element without matching its raw value (e.g. `%nb%` matches the rendering `'a\nb'` of the
            /// value `a<newline>b`). See the function comment above for why only the first character has to be checked.
            char first_char = unescaped_needle.front();
            if (is_case_insensitive)
            {
                /// Unicode case folding of non-ASCII characters is not modeled.
                if (static_cast<UInt8>(first_char) >= 0x80)
                    return;
                if (first_char >= 'A' && first_char <= 'Z')
                    first_char = first_char - 'A' + 'a';
            }
            if (std::string_view("bfnrt0").contains(first_char))
                return;
        }

        const String like_function_name = is_case_insensitive ? "ilike" : "like";
        auto pattern_constant = std::make_shared<ConstantNode>("%" + *needle + "%");

        /// If the argument is a `tuple` function, take its arguments directly: this keeps plain columns
        /// as plain columns in the rewritten conditions, which is what allows using a text index.
        QueryTreeNodes element_nodes;
        const auto * inner_function_node = inner_argument->as<FunctionNode>();
        if (inner_function_node && inner_function_node->getFunctionName() == "tuple"
            && inner_function_node->getArguments().getNodes().size() == tuple_type->getElements().size())
        {
            element_nodes = inner_function_node->getArguments().getNodes();
        }
        else
        {
            for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
            {
                auto index_constant = std::make_shared<ConstantNode>(static_cast<UInt64>(i + 1));
                element_nodes.push_back(makeResolvedFunction("tupleElement", {inner_argument, std::move(index_constant)}));
            }
        }

        QueryTreeNodes disjuncts;
        disjuncts.reserve(element_nodes.size());

        for (auto & element_node : element_nodes)
        {
            QueryTreeNodePtr operand = element_node;
            if (!isStringOrFixedString(removeLowCardinality(operand->getResultType())))
                operand = makeResolvedFunction("toString", {std::move(operand)});

            disjuncts.push_back(makeResolvedFunction(like_function_name, {std::move(operand), pattern_constant}, /*mark_as_operator=*/ true));
        }

        QueryTreeNodePtr result;
        if (disjuncts.size() == 1)
            result = std::move(disjuncts[0]);
        else
            result = makeResolvedFunction("or", std::move(disjuncts), /*mark_as_operator=*/ true);

        /// The rewrite must preserve the result type (e.g. it could be wrapped in Nullable due to group_by_use_nulls).
        if (!result->getResultType()->equals(*function_node.getResultType()))
            return;

        node = std::move(result);
    }

    QueryTreeNodePtr makeResolvedFunction(const String & name, QueryTreeNodes arguments, bool mark_as_operator = false) const
    {
        auto result_function = std::make_shared<FunctionNode>(name);
        if (mark_as_operator)
            result_function->markAsOperator();
        result_function->getArguments().getNodes() = std::move(arguments);
        resolveOrdinaryFunctionNodeByName(*result_function, name, getContext());
        return result_function;
    }
};

}

void OptimizeStringConversionComparisonPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeStringConversionComparisonVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
