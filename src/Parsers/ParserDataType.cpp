#include <Parsers/ParserDataType.h>

#include <string_view>
#include <unordered_set>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTUDTReference.h>
#include <Parsers/ASTUDTTemplate.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/StringUtils.h>


namespace DB
{

namespace
{

bool isEnumType(const String & type_name_upper)
{
    return type_name_upper == "ENUM" || type_name_upper == "ENUM8" || type_name_upper == "ENUM16";
}

/// Integer type names (and MySQL aliases) that accept the MySQL display-width modifier `(N)` and the
/// SIGNED/UNSIGNED suffix. Matched by exact (uppercased) name: a loose substring test on "INT" also
/// matched unrelated names like `quantileInterpolatedWeighted`, silently eating their first `(...)`
/// group as a display width and breaking the parse round-trip.
bool isIntegerTypeName(const String & type_name_upper)
{
    static const std::unordered_set<std::string_view> integer_type_names
    {
        "INT8", "INT16", "INT32", "INT64", "INT128", "INT256",
        "UINT8", "UINT16", "UINT32", "UINT64", "UINT128", "UINT256",
        "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "INT1",
    };
    return integer_type_names.contains(type_name_upper);
}

bool tupleElementIsDefinitelyUnnamed(IParser::Pos pos)
{
    /// Restrict this lookahead to a BareWord: it is already a valid ParserIdentifier without
    /// decoding, while a quoted identifier can still be empty or malformed. A named Tuple
    /// element is `identifier data_type`, and every data type currently starts with another
    /// ParserIdentifier. None of these four tokens can start that second identifier, so the
    /// named parse is guaranteed to fail and the element can go directly through the type path.
    if (pos->type != TokenType::BareWord)
        return false;

    const auto next_type = (++pos)->type;
    if (next_type == TokenType::BareWord || next_type == TokenType::QuotedIdentifier)
        return false;

    return next_type == TokenType::Comma || next_type == TokenType::ClosingRoundBracket || next_type == TokenType::OpeningRoundBracket
        || next_type == TokenType::Dot;
}

/// Parse enum values directly into the vector without creating ASTLiteral nodes.
/// Format: 'name1' = value1, 'name2' = value2, ...
/// Only handles fully explicit enums (all values must have = number).
/// Returns false to fall back to generic parser for auto-assigned values or special literals.
bool parseEnumValues(
    IParser::Pos & pos,
    std::vector<std::pair<String, Int64>> & values,
    Expected & expected)
{
    bool first_element = true;

    while (true)
    {
        if (!first_element)
        {
            if (pos->type != TokenType::Comma)
                break;
            ++pos;
        }
        first_element = false;

        if (pos->type != TokenType::StringLiteral)
        {
            expected.add(pos, "string literal for enum element name");
            return false;
        }

        String elem_name;
        char first_char = *pos->begin;
        if (first_char == 'b' || first_char == 'B' || first_char == 'x' || first_char == 'X')
        {
            /// Binary/hex string literals (b'...', x'...') - use full parser to decode
            ASTPtr literal_ast;
            ParserStringLiteral string_literal_parser;
            if (!string_literal_parser.parse(pos, literal_ast, expected))
                return false;
            elem_name = literal_ast->as<ASTLiteral &>().value.safeGet<String>();
        }
        else
        {
            ReadBufferFromMemory in(pos->begin, pos->size());
            if (!tryReadQuotedStringWithSQLStyle(elem_name, in) || in.count() != pos->size())
                return false;
            ++pos;
        }

        /// Must have explicit value - if not, fall back to generic parser for auto-assignment
        if (pos->type != TokenType::Equals)
            return false;
        ++pos;

        bool negative = false;
        if (pos->type == TokenType::Minus)
        {
            negative = true;
            ++pos;
        }

        if (pos->type != TokenType::Number)
        {
            expected.add(pos, "number for enum element value");
            return false;
        }

        UInt64 abs_value = 0;
        ReadBufferFromMemory num_in(pos->begin, pos->size());
        if (!tryReadIntText(abs_value, num_in) || num_in.count() != pos->size())
            return false;
        ++pos;

        /// Values are stored as Int64. A magnitude above Int64 cannot be stored faithfully:
        /// a plain cast would silently wrap (e.g. UInt64 18446744073709551615 to -1) and pass
        /// the downstream range check. Such a value is out of range for any Enum anyway, so
        /// fall back to the generic parser, which rejects it.
        if (abs_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            return false;

        Int64 elem_value = negative ? -static_cast<Int64>(abs_value) : static_cast<Int64>(abs_value);
        values.emplace_back(elem_name, elem_value);
    }

    return !values.empty();
}

struct EmptyDataTypeFamilyClassificationSummary
{
};

struct UnclassifiedDataTypeParserState
{
    using Summary = EmptyDataTypeFamilyClassificationSummary;
    static constexpr bool enabled = false;
    static constexpr bool template_parameters_enabled = false;
    static constexpr bool qualified_references_enabled = false;

    void add(Summary &, std::string_view, DataTypeFamilySyntaxKind) const noexcept { }
    void merge(Summary &, const Summary &) const noexcept { }
};

struct QualifiedReferenceDataTypeParserState
{
    using Summary = EmptyDataTypeFamilyClassificationSummary;
    static constexpr bool enabled = false;
    static constexpr bool template_parameters_enabled = false;
    static constexpr bool qualified_references_enabled = true;

    void add(Summary &, std::string_view, DataTypeFamilySyntaxKind) const noexcept { }
    void merge(Summary &, const Summary &) const noexcept { }
};

struct ClassifiedDataTypeParserState
{
    using Summary = DataTypeFamilyClassificationSummary;
    static constexpr bool enabled = true;
    static constexpr bool template_parameters_enabled = false;
    static constexpr bool qualified_references_enabled = true;

    DataTypeFamilyClassifier classifier;

    void add(Summary & summary, std::string_view family_name, DataTypeFamilySyntaxKind syntax_kind) const noexcept
    {
        summary.add(classifier.classify(family_name, syntax_kind));
    }

    void merge(Summary & summary, const Summary & child) const noexcept { summary.merge(child); }
};

struct TemplateDataTypeParserState
{
    using Summary = EmptyDataTypeFamilyClassificationSummary;
    static constexpr bool enabled = false;
    static constexpr bool template_parameters_enabled = true;
    static constexpr bool qualified_references_enabled = true;

    const UDTExpressionParserContext & context;
    const std::unordered_map<String, size_t, UDTParameterNameHash, std::equal_to<>> & parameter_indexes;

    void add(Summary &, std::string_view, DataTypeFamilySyntaxKind) const noexcept { }
    void merge(Summary &, const Summary &) const noexcept { }

    const UDTTemplateParameterDescriptor * findParameter(std::string_view name) const
    {
        const auto it = parameter_indexes.find(name);
        if (it == parameter_indexes.end())
            return nullptr;
        return &context.parameters[it->second];
    }

    bool isSelfReference(std::string_view database_name, std::string_view type_name) const noexcept
    {
        return database_name == context.definition_database && type_name == context.definition_name;
    }
};

template <typename State>
bool parseDataTypeImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, const State & state, typename State::Summary & summary);

template <typename State>
class ParserDataTypeWithState final : public IParserBase
{
public:
    ParserDataTypeWithState(const State & state_, typename State::Summary & summary_)
        : state(state_)
        , summary(summary_)
    {
    }

private:
    const char * getName() const override { return "user-defined type expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        typename State::Summary parsed_summary;
        if (!parseDataTypeImpl(pos, node, expected, state, parsed_summary))
            return false;
        summary = parsed_summary;
        return true;
    }

    const State & state;
    typename State::Summary & summary;
};

template <typename State>
bool parseNestedDataType(IParser::Pos & pos, ASTPtr & node, Expected & expected, const State & state, typename State::Summary & summary)
{
    if constexpr (State::template_parameters_enabled)
    {
        ParserDataTypeWithState<State> parser(state, summary);
        return parser.parse(pos, node, expected);
    }
    else if constexpr (State::enabled)
    {
        ParserDataTypeWithFamilyClassification parser(state.classifier, summary);
        return parser.parse(pos, node, expected);
    }
    else if constexpr (State::qualified_references_enabled)
    {
        ParserDataTypeWithState<State> parser(state, summary);
        return parser.parse(pos, node, expected);
    }
    else
    {
        ParserDataType parser;
        return parser.parse(pos, node, expected);
    }
}

bool getOrdinaryIdentifierName(const ASTPtr & ast, String & name)
{
    const auto * identifier = ast ? ast->as<ASTIdentifier>() : nullptr;
    if (!identifier || identifier->isParam() || !identifier->isShort())
        return false;

    name = identifier->shortName();
    return !name.empty();
}

ASTPtr makeTemplateParameterReference(const UDTTemplateParameterDescriptor & parameter)
{
    if (parameter.kind == UDTParameterKind::Type)
    {
        auto reference = make_intrusive<ASTUDTTypeParameterReference>();
        reference->name = parameter.name;
        reference->ordinal = parameter.ordinal;
        return reference;
    }

    auto reference = make_intrusive<ASTUDTValueParameterReference>();
    reference->name = parameter.name;
    reference->ordinal = parameter.ordinal;
    reference->kind = parameter.kind;
    return reference;
}

bool parseTemplateValueParameter(
    IParser::Pos & pos, ASTPtr & node, Expected & expected, const TemplateDataTypeParserState & state, bool allow_decrement)
{
    const auto begin = pos;
    ParserIdentifier identifier_parser;
    ASTPtr identifier;
    String name;
    if (!identifier_parser.parse(pos, identifier, expected) || !getOrdinaryIdentifierName(identifier, name))
        return false;

    const auto * parameter = state.findParameter(name);
    if (!parameter || !isUDTValueParameterKind(parameter->kind))
    {
        pos = begin;
        return false;
    }

    ASTPtr parameter_reference = makeTemplateParameterReference(*parameter);
    if (pos->type != TokenType::Minus)
    {
        node = std::move(parameter_reference);
        return true;
    }

    if (!allow_decrement || !state.context.decreasing_parameter || parameter->ordinal != *state.context.decreasing_parameter
        || !isUDTUnsignedParameterKind(parameter->kind))
    {
        pos = begin;
        return false;
    }

    ++pos;
    ASTPtr amount_ast;
    ParserUnsignedInteger amount_parser;
    if (!amount_parser.parse(pos, amount_ast, expected) || amount_ast->as<ASTLiteral &>().value.safeGet<UInt64>() != 1)
    {
        pos = begin;
        return false;
    }

    auto decrement = make_intrusive<ASTUDTDecrement>(std::move(parameter_reference), 1);
    node = std::move(decrement);
    return true;
}

bool validateSelfReferenceArguments(
    const ASTUDTReference & reference, const TemplateDataTypeParserState & state, IParser::Pos error_pos, Expected & expected)
{
    const auto * arguments = reference.getArguments() ? reference.getArguments()->as<ASTExpressionList>() : nullptr;
    const size_t argument_count = arguments ? arguments->children.size() : 0;
    if (argument_count != state.context.parameters.size())
    {
        expected.add(error_pos, "one self-call argument for every declared parameter");
        return false;
    }

    for (size_t index = 0; index < argument_count; ++index)
    {
        const auto & parameter = state.context.parameters[index];
        const auto & argument = arguments->children[index];

        if (parameter.kind == UDTParameterKind::Type)
        {
            const auto * reference_argument = argument->as<ASTUDTTypeParameterReference>();
            if (!reference_argument || reference_argument->ordinal != parameter.ordinal)
            {
                expected.add(error_pos, "a self-call TYPE argument forwarding the same formal");
                return false;
            }
            continue;
        }

        if (state.context.decreasing_parameter && parameter.ordinal == *state.context.decreasing_parameter)
        {
            const auto * decrement = argument->as<ASTUDTDecrement>();
            const auto * value_reference = decrement && decrement->parameter_reference
                ? decrement->parameter_reference->as<ASTUDTValueParameterReference>()
                : nullptr;
            if (!decrement || decrement->amount != 1 || !value_reference || value_reference->ordinal != parameter.ordinal)
            {
                expected.add(error_pos, "the declared decreasing self-call argument minus one");
                return false;
            }
        }
        else
        {
            const auto * value_reference = argument->as<ASTUDTValueParameterReference>();
            if (!value_reference || value_reference->ordinal != parameter.ordinal)
            {
                expected.add(error_pos, "a self-call value argument forwarding the same formal");
                return false;
            }
        }
    }

    return true;
}

bool parseTemplateTypeIf(
    IParser::Pos & pos,
    ASTPtr & node,
    Expected & expected,
    const TemplateDataTypeParserState & state,
    TemplateDataTypeParserState::Summary & summary)
{
    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    ASTPtr parameter;
    if (!parseTemplateValueParameter(pos, parameter, expected, state, /*allow_decrement=*/false))
        return false;

    const auto * value_reference = parameter->as<ASTUDTValueParameterReference>();
    if (!value_reference || !isUDTIntegerParameterKind(value_reference->kind))
    {
        expected.add(pos, "an integer value parameter in TYPE_IF");
        return false;
    }

    if (pos->type != TokenType::Equals)
    {
        expected.add(pos, "equals operator in TYPE_IF");
        return false;
    }
    ++pos;

    ASTPtr zero;
    ParserUnsignedInteger integer_parser;
    if (!integer_parser.parse(pos, zero, expected) || zero->as<ASTLiteral &>().value.safeGet<UInt64>() != 0)
    {
        expected.add(pos, "zero in TYPE_IF predicate");
        return false;
    }

    if (pos->type != TokenType::Comma)
        return false;
    ++pos;

    ASTPtr then_type;
    TemplateDataTypeParserState::Summary then_summary;
    if (!parseNestedDataType(pos, then_type, expected, state, then_summary) || pos->type != TokenType::Comma)
        return false;
    ++pos;

    ASTPtr else_type;
    TemplateDataTypeParserState::Summary else_summary;
    if (!parseNestedDataType(pos, else_type, expected, state, else_summary) || pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    auto predicate = make_intrusive<ASTUDTIsZero>(std::move(parameter));
    auto type_if = make_intrusive<ASTUDTTypeIf>(std::move(predicate), std::move(then_type), std::move(else_type));
    state.merge(summary, then_summary);
    state.merge(summary, else_summary);
    node = std::move(type_if);
    return true;
}

template <typename State>
bool parseQualifiedReferenceArguments(
    IParser::Pos & pos,
    ASTUDTReference & reference,
    Expected & expected,
    const State & state,
    typename State::Summary & summary)
{
    const bool is_self_reference = [&]
    {
        if constexpr (State::template_parameters_enabled)
            return state.isSelfReference(reference.database_name, reference.type_name);
        return false;
    }();

    if (pos->type != TokenType::OpeningRoundBracket)
    {
        if constexpr (State::template_parameters_enabled)
            if (is_self_reference)
                return validateSelfReferenceArguments(reference, state, pos, expected);
        return true;
    }
    ++pos;

    auto arguments = make_intrusive<ASTExpressionList>();
    size_t argument_index = 0;
    while (true)
    {
        if (argument_index > 0)
        {
            if (pos->type != TokenType::Comma)
                break;
            ++pos;
            if (pos->type == TokenType::ClosingRoundBracket)
                return false;
        }

        ASTPtr argument;
        typename State::Summary argument_summary;
        ParserLiteral literal_parser;
        bool parsed_as_nested_type = false;
        if constexpr (State::template_parameters_enabled)
            parseTemplateValueParameter(pos, argument, expected, state, is_self_reference);
        if (!argument)
        {
            if (!literal_parser.parse(pos, argument, expected))
                parsed_as_nested_type = parseNestedDataType(pos, argument, expected, state, argument_summary);
        }

        if (!argument)
            break;

        if (parsed_as_nested_type && pos->type == TokenType::ClosingRoundBracket)
        {
            auto previous = pos;
            --previous;
            if (previous->type == TokenType::Comma)
                return false;
        }

        state.merge(summary, argument_summary);
        arguments->children.emplace_back(std::move(argument));
        ++argument_index;
    }

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    if (!arguments->children.empty())
        reference.children.push_back(arguments);

    if constexpr (State::template_parameters_enabled)
        if (is_self_reference && !validateSelfReferenceArguments(reference, state, pos, expected))
            return false;
    return true;
}

template <typename State>
class ClassificationSummaryDestination
{
public:
    using Summary = typename State::Summary;

    explicit ClassificationSummaryDestination(Summary & summary_)
        : summary(summary_)
    {
    }

    void commit(const Summary & parsed_summary) const noexcept { summary = parsed_summary; }

private:
    Summary & summary;
};

template <>
class ClassificationSummaryDestination<UnclassifiedDataTypeParserState>
{
public:
    explicit ClassificationSummaryDestination(EmptyDataTypeFamilyClassificationSummary &) { }
    void commit(const EmptyDataTypeFamilyClassificationSummary &) const noexcept { }
};

/// Parser of Dynamic type argument: Dynamic(max_types=N)
template <typename State>
class DynamicArgumentParser : public IParserBase
{
public:
    explicit DynamicArgumentParser(const State & state_)
        : state(state_)
    {
    }

private:
    const char * getName() const override { return "Dynamic data type optional argument"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ASTPtr identifier;
        ParserIdentifier identifier_parser;
        if (!identifier_parser.parse(pos, identifier, expected))
            return false;

        if (pos->type != TokenType::Equals)
        {
            expected.add(pos, "equals operator");
            return false;
        }

        ++pos;

        ASTPtr number;
        if constexpr (State::template_parameters_enabled)
            parseTemplateValueParameter(pos, number, expected, state, /*allow_decrement=*/false);
        if (!number)
        {
            ParserNumber number_parser;
            if (!number_parser.parse(pos, number, expected))
                return false;
        }

        node = makeASTOperator("equals", identifier, number);
        return true;
    }

    [[no_unique_address]] State state;
};

/// Parser of Object type argument. For example: JSON(some_parameter=N, some.path SomeType, SKIP skip.path, ...)
template <typename State>
class ObjectArgumentParser : public IParserBase
{
public:
    ObjectArgumentParser(const State & state_, typename State::Summary & summary_)
        : state(state_)
        , destination(summary_)
    {
    }

private:
    const char * getName() const override { return "JSON data type optional argument"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        typename State::Summary parsed_summary;
        auto argument = make_intrusive<ASTObjectTypeArgument>();

        /// SKIP arguments
        if (ParserKeyword(Keyword::SKIP).ignore(pos))
        {
            /// SKIP REGEXP '<some_regexp>'
            if (ParserKeyword(Keyword::REGEXP).ignore(pos))
            {
                ParserStringLiteral literal_parser;
                ASTPtr literal;
                if (!literal_parser.parse(pos, literal, expected))
                    return false;
                argument->skip_path_regexp = literal;
                argument->children.push_back(argument->skip_path_regexp);
            }
            /// SKIP some.path
            else
            {
                ParserCompoundIdentifier compound_identifier_parser;
                ASTPtr compound_identifier;
                if (!compound_identifier_parser.parse(pos, compound_identifier, expected))
                    return false;

                argument->skip_path = compound_identifier;
                argument->children.push_back(argument->skip_path);
            }

            node = argument;
            destination.commit(parsed_summary);
            return true;
        }

        ParserCompoundIdentifier compound_identifier_parser;
        ASTPtr identifier;
        if (!compound_identifier_parser.parse(pos, identifier, expected))
            return false;

        /// some_parameter=N
        if (pos->type == TokenType::Equals)
        {
            ++pos;
            ASTPtr number;
            if constexpr (State::template_parameters_enabled)
                parseTemplateValueParameter(pos, number, expected, state, /*allow_decrement=*/false);
            if (!number)
            {
                ParserNumber number_parser;
                if (!number_parser.parse(pos, number, expected))
                    return false;
            }

            argument->parameter = makeASTOperator("equals", identifier, number);
            argument->children.push_back(argument->parameter);
            node = argument;
            destination.commit(parsed_summary);
            return true;
        }

        ASTPtr type;
        if (!parseNestedDataType(pos, type, expected, state, parsed_summary))
            return false;

        auto name_and_type = make_intrusive<ASTObjectTypedPathArgument>();
        name_and_type->path = getIdentifierName(identifier);
        name_and_type->type = type;
        name_and_type->children.push_back(name_and_type->type);
        argument->path_with_type = name_and_type;
        argument->children.push_back(argument->path_with_type);
        node = argument;
        destination.commit(parsed_summary);
        return true;
    }

    [[no_unique_address]] State state;
    [[no_unique_address]] ClassificationSummaryDestination<State> destination;
};

template <typename State>
class NameTypePairParser : public IParserBase
{
public:
    NameTypePairParser(const State & state_, typename State::Summary & summary_)
        : state(state_)
        , destination(summary_)
    {
    }

private:
    const char * getName() const override { return "name and type pair"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserIdentifier name_parser;
        ASTPtr name;
        ASTPtr type;
        typename State::Summary parsed_summary;
        if (!name_parser.parse(pos, name, expected) || !parseNestedDataType(pos, type, expected, state, parsed_summary))
            return false;

        auto name_type_pair = make_intrusive<ASTNameTypePair>();
        tryGetIdentifierNameInto(name, name_type_pair->name);
        name_type_pair->type = type;
        name_type_pair->children.push_back(type);
        node = name_type_pair;
        destination.commit(parsed_summary);
        return true;
    }

    [[no_unique_address]] State state;
    [[no_unique_address]] ClassificationSummaryDestination<State> destination;
};

template <typename State>
bool parseDataTypeImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, const State & state, typename State::Summary & summary)
{
    typename State::Summary parsed_summary;
    String type_name;

    const bool identifier_was_quoted = pos->type == TokenType::QuotedIdentifier;
    ParserIdentifier name_parser;
    ASTPtr identifier;
    if (!name_parser.parse(pos, identifier, expected))
        return false;
    if (!getOrdinaryIdentifierName(identifier, type_name))
        return false;

    if (pos->type == TokenType::Dot)
    {
        if constexpr (!State::qualified_references_enabled)
            return false;

        ++pos;
        ASTPtr type_identifier;
        String qualified_type_name;
        if (!name_parser.parse(pos, type_identifier, expected) || !getOrdinaryIdentifierName(type_identifier, qualified_type_name))
            return false;

        /// UDT references have exactly two identifier components. Reject a third
        /// component here instead of emitting a valid prefix and relying on a
        /// distant caller to notice the unconsumed suffix.
        if (pos->type == TokenType::Dot)
        {
            expected.add(pos, "exactly two components in a qualified type reference");
            return false;
        }

        auto reference = make_intrusive<ASTUDTReference>();
        reference->database_name = type_name;
        reference->type_name = qualified_type_name;
        if (!parseQualifiedReferenceArguments(pos, *reference, expected, state, parsed_summary))
            return false;

        state.add(parsed_summary, qualified_type_name, DataTypeFamilySyntaxKind::QualifiedReference);
        summary = parsed_summary;
        node = reference;
        return true;
    }

    if constexpr (State::template_parameters_enabled)
    {
        if (Poco::toUpper(type_name) == "TYPE_IF" && pos->type == TokenType::OpeningRoundBracket)
        {
            if (identifier_was_quoted)
                return false;
            if (!parseTemplateTypeIf(pos, node, expected, state, parsed_summary))
                return false;
            summary = parsed_summary;
            return true;
        }

        const auto * parameter = state.findParameter(type_name);
        if (parameter && pos->type != TokenType::OpeningRoundBracket)
        {
            if (parameter->kind != UDTParameterKind::Type)
            {
                expected.add(pos, "a TYPE parameter in a type-producing position");
                return false;
            }

            node = makeTemplateParameterReference(*parameter);
            summary = parsed_summary;
            return true;
        }

        if (type_name == state.context.definition_name)
        {
            auto reference = make_intrusive<ASTUDTReference>();
            reference->database_name = state.context.definition_database;
            reference->type_name = type_name;
            if (!parseQualifiedReferenceArguments(pos, *reference, expected, state, parsed_summary))
                return false;

            summary = parsed_summary;
            node = std::move(reference);
            return true;
        }

        if (parameter)
        {
            expected.add(pos, "a non-applied TYPE parameter");
            return false;
        }
    }

    /// When parsing we accept quoted type names (e.g. `UInt64`), but when formatting we print them
    /// unquoted (e.g. UInt64). This introduces problems when the string in the quotes is garbage:
    ///  * Array(`x.y`) -> Array(x.y) -> fails to parse
    ///  * `Null` -> Null -> parses as keyword instead of type name
    ///  * `8` -> 8 -> parses as a numeric literal instead of a type name
    /// Here we check for these cases and reject.
    if (type_name.empty()
        || isNumericASCII(type_name[0])
        || !std::all_of(type_name.begin(), type_name.end(), [](char c) { return isWordCharASCII(c) || c == '$'; }))
    {
        expected.add(pos, "type name");
        return false;
    }
    /// Keywords that IParserColumnDeclaration recognizes before the type name.
    /// E.g. reject CREATE TABLE a (x `Null`) because in "x Null" the Null would be parsed as
    /// column attribute rather than type name.
    {
        String n = type_name;
        toUpperASCII(n);
        if (n == "NOT" || n == "NULL" || n == "DEFAULT" || n == "MATERIALIZED" || n == "EPHEMERAL" || n == "ALIAS" || n == "AUTO" || n == "PRIMARY" || n == "TTL" || n == "COMMENT" || n == "CODEC"
            || n == "SETTINGS" || n == "STATISTICS")
        {
            expected.add(pos, "type name");
            return false;
        }
    }

    String type_name_upper = Poco::toUpper(type_name);
    String type_name_suffix;

    /// Special cases for compatibility with SQL standard. We can parse several words as type name
    /// only for certain first words, otherwise we don't know how many words to parse
    if (type_name_upper == "NATIONAL")
    {
        if (ParserKeyword(Keyword::CHARACTER_LARGE_OBJECT).ignore(pos))
            type_name_suffix = toStringView(Keyword::CHARACTER_LARGE_OBJECT);
        else if (ParserKeyword(Keyword::CHARACTER_VARYING).ignore(pos))
            type_name_suffix = toStringView(Keyword::CHARACTER_VARYING);
        else if (ParserKeyword(Keyword::CHAR_VARYING).ignore(pos))
            type_name_suffix = toStringView(Keyword::CHAR_VARYING);
        else if (ParserKeyword(Keyword::CHARACTER).ignore(pos))
            type_name_suffix = toStringView(Keyword::CHARACTER);
        else if (ParserKeyword(Keyword::CHAR).ignore(pos))
            type_name_suffix = toStringView(Keyword::CHAR);
    }
    else if (type_name_upper == "BINARY" ||
             type_name_upper == "CHARACTER" ||
             type_name_upper == "CHAR" ||
             type_name_upper == "NCHAR")
    {
        if (ParserKeyword(Keyword::LARGE_OBJECT).ignore(pos))
            type_name_suffix = toStringView(Keyword::LARGE_OBJECT);
        else if (ParserKeyword(Keyword::VARYING).ignore(pos))
            type_name_suffix = toStringView(Keyword::VARYING);
    }
    else if (type_name_upper == "DOUBLE")
    {
        if (ParserKeyword(Keyword::PRECISION).ignore(pos))
            type_name_suffix = toStringView(Keyword::PRECISION);
    }
    else if (isIntegerTypeName(type_name_upper))
    {
        /// Support SIGNED and UNSIGNED integer type modifiers for compatibility with MySQL
        if (ParserKeyword(Keyword::SIGNED).ignore(pos, expected))
            type_name_suffix = toStringView(Keyword::SIGNED);
        else if (ParserKeyword(Keyword::UNSIGNED).ignore(pos, expected))
            type_name_suffix = toStringView(Keyword::UNSIGNED);
        else if (pos->type == TokenType::OpeningRoundBracket)
        {
            ++pos;
            if (pos->type == TokenType::Number)
                ++pos;
            if (pos->type != TokenType::ClosingRoundBracket)
               return false;
            ++pos;
            if (ParserKeyword(Keyword::SIGNED).ignore(pos, expected))
                type_name_suffix = toStringView(Keyword::SIGNED);
            else if (ParserKeyword(Keyword::UNSIGNED).ignore(pos, expected))
                type_name_suffix = toStringView(Keyword::UNSIGNED);
        }

    }

    if (!type_name_suffix.empty())
        type_name = type_name_upper + " " + type_name_suffix;

    /// skip trailing comma in types, e.g. Tuple(Int, String,)
    if (pos->type == TokenType::Comma)
    {
        Expected test_expected;
        auto test_pos = pos;
        ++test_pos;
        if (ParserToken(TokenType::ClosingRoundBracket).ignore(test_pos, test_expected))
        { // the end of the type definition was reached and there was a trailing comma
            ++pos;
        }
    }

    /// Handle Enum types specially - parse directly into ASTEnumDataType
    /// to avoid creating hundreds of ASTLiteral nodes for large enums.
    /// Only handles fully explicit enums; falls back to generic parser for auto-assigned values.
    if (isEnumType(type_name_upper) && pos->type == TokenType::OpeningRoundBracket)
    {
        auto saved_pos = pos;
        ++pos;

        auto enum_node = make_intrusive<ASTEnumDataType>();
        enum_node->name = type_name;

        if (parseEnumValues(pos, enum_node->values, expected) && pos->type == TokenType::ClosingRoundBracket)
        {
            ++pos;
            enum_node->values.shrink_to_fit();
            state.add(parsed_summary, type_name, DataTypeFamilySyntaxKind::SpecializedEnum);
            summary = parsed_summary;
            node = enum_node;
            return true;
        }
        pos = saved_pos;
    }

    /// Handle Tuple types specially - parse directly into ASTTupleDataType
    /// to avoid creating ASTNameTypePair nodes for each named element.
    ///
    /// `Tuple()` is the one form this fast path rejects and the generic argument parser below
    /// accepts, so detect an empty argument list up front and leave it to the generic parser. For
    /// every other argument list this fast path is authoritative: the generic parser applies exactly
    /// the same element parsers (`ParserNameTypePair` is `ParserIdentifier` followed by
    /// `ParserDataType`), so it stops at the same token and fails the same way. Falling through on
    /// failure used to parse the argument list a second time, which doubled the work at every
    /// nesting level - a malformed `Tuple(Tuple(...))` of depth N cost 2^N and exhausted
    /// `max_parser_backtracks` instead of reporting a syntax error.
    bool use_tuple_fast_path = type_name == "Tuple" && pos->type == TokenType::OpeningRoundBracket;
    if (use_tuple_fast_path)
    {
        auto after_bracket = pos;
        ++after_bracket;
        use_tuple_fast_path = after_bracket->type != TokenType::ClosingRoundBracket;
    }

    if (use_tuple_fast_path)
    {
        ++pos;

        auto tuple_node = make_intrusive<ASTTupleDataType>();
        tuple_node->name = type_name;
        auto arguments = make_intrusive<ASTExpressionList>();
        tuple_node->children.push_back(arguments);

        bool has_named_elements = false;
        Strings element_names_tmp;
        bool first_element = true;
        typename State::Summary tuple_summary;

        while (true)
        {
            if (!first_element)
            {
                if (pos->type == TokenType::Comma)
                    ++pos;
                else
                    break;
            }
            first_element = false;

            /// Try to parse: identifier Type (named element)
            /// or just: Type (unnamed element)
            ParserIdentifier identifier_parser;
            ASTPtr identifier_node;
            ASTPtr type_node;
            typename State::Summary element_summary;

            auto element_pos = pos;
            bool try_named_element = !tupleElementIsDefinitelyUnnamed(pos);
            if (!try_named_element)
            {
                /// Preserve the failed nested-type parser's Expected entries without constructing
                /// the first identifier AST. If that parser ever accepts one of these delimiters,
                /// automatically retain the old named-first path instead of relying on stale
                /// lookahead assumptions.
                auto named_type_pos = pos;
                ++named_type_pos;
                ASTPtr ignored_type_node;
                typename State::Summary ignored_summary;
                try_named_element = parseNestedDataType(named_type_pos, ignored_type_node, expected, state, ignored_summary);
            }
            if (try_named_element && identifier_parser.parse(pos, identifier_node, expected)
                && parseNestedDataType(pos, type_node, expected, state, element_summary))
            {
                /// Named element: name Type
                String elem_name;
                tryGetIdentifierNameInto(identifier_node, elem_name);
                element_names_tmp.push_back(elem_name);
                arguments->children.push_back(type_node);
                has_named_elements = true;
                state.merge(tuple_summary, element_summary);
            }
            else
            {
                /// Try just Type (unnamed element)
                if (try_named_element)
                    pos = element_pos;
                element_summary = {};
                if (parseNestedDataType(pos, type_node, expected, state, element_summary))
                {
                    /// Empty placeholder needed to detect mixed named/unnamed tuples.
                    /// The factory validates that all names are non-empty when element_names is set.
                    element_names_tmp.push_back("");
                    arguments->children.push_back(type_node);
                    state.merge(tuple_summary, element_summary);
                }
                else
                {
                    /// Could not parse element
                    break;
                }
            }
        }

        if (pos->type == TokenType::ClosingRoundBracket && !arguments->children.empty())
        {
            ++pos;
            /// Only store element_names if tuple has any named elements
            if (has_named_elements)
            {
                element_names_tmp.shrink_to_fit();
                tuple_node->element_names = std::move(element_names_tmp);
            }
            arguments->children.shrink_to_fit();
            state.add(tuple_summary, type_name, DataTypeFamilySyntaxKind::SpecializedTuple);
            summary = tuple_summary;
            node = tuple_node;
            return true;
        }

        return false;
    }

    auto data_type_node = make_intrusive<ASTDataType>();
    data_type_node->name = type_name;

    if (pos->type != TokenType::OpeningRoundBracket)
    {
        state.add(parsed_summary, type_name, DataTypeFamilySyntaxKind::Generic);
        summary = parsed_summary;
        node = data_type_node;
        return true;
    }
    ++pos;

    /// Parse optional parameters
    ASTPtr expr_list_args = make_intrusive<ASTExpressionList>();

    /// Allow mixed lists of nested and normal types.
    /// Parameters are either:
    /// - Nested table element;
    /// - Tuple element
    /// - Enum element in form of 'a' = 1;
    /// - literal;
    /// - Dynamic type argument;
    /// - JSON type argument;
    /// - another data type (or identifier);

    size_t arg_num = 0;
    bool have_version_of_aggregate_function = false;
    while (true)
    {
        if (arg_num > 0)
        {
            if (pos->type == TokenType::Comma)
                ++pos;
            else
                break;
        }

        ASTPtr arg;
        typename State::Summary arg_summary;
        if (type_name == "Dynamic")
        {
            DynamicArgumentParser<State> parser(state);
            parser.parse(pos, arg, expected);
        }
        else if (equalsCaseInsensitive(type_name, "json"))
        {
            ObjectArgumentParser<State> parser(state, arg_summary);
            parser.parse(pos, arg, expected);
        }
        else if (type_name == "Nested")
        {
            if constexpr (State::enabled || State::template_parameters_enabled || State::qualified_references_enabled)
            {
                NameTypePairParser<State> name_and_type_parser(state, arg_summary);
                name_and_type_parser.parse(pos, arg, expected);
            }
            else
            {
                ParserNameTypePair name_and_type_parser;
                name_and_type_parser.parse(pos, arg, expected);
            }
        }
        else if (type_name == "Tuple")
        {
            if constexpr (State::enabled || State::template_parameters_enabled || State::qualified_references_enabled)
            {
                NameTypePairParser<State> name_and_type_parser(state, arg_summary);
                if (!name_and_type_parser.parse(pos, arg, expected))
                {
                    arg_summary = {};
                    parseNestedDataType(pos, arg, expected, state, arg_summary);
                }
            }
            else
            {
                ParserNameTypePair name_and_type_parser;
                ParserDataType only_type_parser;
                name_and_type_parser.parse(pos, arg, expected) || only_type_parser.parse(pos, arg, expected);
            }
        }
        else if (type_name == "AggregateFunction" || type_name == "SimpleAggregateFunction")
        {
            /// This is less trivial.
            /// The first optional argument for AggregateFunction is a numeric literal, defining the version.
            /// The next argument is the function name, optionally with parameters.
            /// Subsequent arguments are data types.

            if (arg_num == 0 && type_name == "AggregateFunction")
            {
                ParserUnsignedInteger version_parser;
                if (version_parser.parse(pos, arg, expected))
                {
                    have_version_of_aggregate_function = true;
                    expr_list_args->children.emplace_back(std::move(arg));
                    ++arg_num;
                    continue;
                }
            }

            if (arg_num == (have_version_of_aggregate_function ? 1 : 0))
            {
                ParserFunction function_parser;
                ParserIdentifier identifier_parser;
                function_parser.parse(pos, arg, expected)
                    || identifier_parser.parse(pos, arg, expected);
            }
            else
            {
                parseNestedDataType(pos, arg, expected, state, arg_summary);
            }
        }
        else
        {
            /// Only accept simple literals (numbers, strings, NULL, ...) as
            /// data-type arguments. We deliberately do NOT accept collection
            /// literals like `(1)`, `[1, 2]` or `{a: 1}` here: no real data
            /// type takes a tuple/array/map literal as an argument, and
            /// accepting them produces an `ASTLiteral` with `Field::Tuple`
            /// (or `Field::Array`) inside the type's argument list. The
            /// formatter then prints such a Tuple literal as `tuple(...)`
            /// (the explicit function form -- see
            /// `FieldVisitorToString::operator()(const Tuple &)`), and
            /// re-parsing `tuple(...)` in this context yields an
            /// `ASTDataType("tuple")` instead of an `ASTLiteral`, breaking
            /// the AST round-trip check in `executeQuery` (DEBUG/sanitizer
            /// builds). See STID 1941-1bfa.
            ParserLiteral literal_parser;

            const char * operators[] = {"=", "equals", nullptr};
            ParserLeftAssociativeBinaryOperatorList enum_parser(operators, std::make_unique<ParserLiteral>());

            if constexpr (State::template_parameters_enabled)
                parseTemplateValueParameter(pos, arg, expected, state, /*allow_decrement=*/false);
            if (!arg)
                enum_parser.parse(pos, arg, expected) || literal_parser.parse(pos, arg, expected)
                    || parseNestedDataType(pos, arg, expected, state, arg_summary);
        }

        if (!arg)
            break;

        state.merge(parsed_summary, arg_summary);
        expr_list_args->children.emplace_back(std::move(arg));
        ++arg_num;
    }

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    /// Only attach arguments if non-empty, so that e.g. `Tuple()` produces the same
    /// AST as `Tuple` (no children). This keeps the formatting roundtrip consistent:
    /// formatImpl omits parentheses for empty arguments, and reparsing without
    /// parentheses produces a node with no children.
    if (!expr_list_args->children.empty())
        data_type_node->children.push_back(expr_list_args);

    state.add(parsed_summary, type_name, DataTypeFamilySyntaxKind::Generic);
    summary = parsed_summary;
    node = data_type_node;
    return true;
}

}

bool ParserDataType::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    UnclassifiedDataTypeParserState state;
    EmptyDataTypeFamilyClassificationSummary summary;
    return parseDataTypeImpl(pos, node, expected, state, summary);
}

bool ParserDataTypeWithQualifiedReferences::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    QualifiedReferenceDataTypeParserState state;
    EmptyDataTypeFamilyClassificationSummary summary;
    return parseDataTypeImpl(pos, node, expected, state, summary);
}

bool ParserDataTypeWithFamilyClassification::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    DataTypeFamilyClassificationSummary parsed_summary;
    ClassifiedDataTypeParserState state{classifier};
    if (!parseDataTypeImpl(pos, node, expected, state, parsed_summary))
        return false;

    summary = parsed_summary;
    return true;
}

ParserUDTExpression::ParserUDTExpression(UDTExpressionParserContext context_)
    : context(std::move(context_))
{
    parameter_indexes.reserve(context.parameters.size());
    for (size_t index = 0; index < context.parameters.size(); ++index)
    {
        const auto & parameter = context.parameters[index];
        if (parameter.name.empty() || static_cast<size_t>(parameter.ordinal) != index || parameter.kind < UDTParameterKind::Type
            || parameter.kind > UDTParameterKind::String || !parameter_indexes.emplace(parameter.name, index).second)
        {
            context_is_valid = false;
            return;
        }
    }

    if (context.decreasing_parameter
        && (static_cast<size_t>(*context.decreasing_parameter) >= context.parameters.size()
            || !isUDTUnsignedParameterKind(context.parameters[*context.decreasing_parameter].kind)))
        context_is_valid = false;
}

bool ParserUDTExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!context_is_valid)
    {
        expected.add(pos, "a valid ordered user-defined type parameter context");
        return false;
    }

    TemplateDataTypeParserState state{context, parameter_indexes};
    EmptyDataTypeFamilyClassificationSummary summary;
    return parseDataTypeImpl(pos, node, expected, state, summary);
}
}
