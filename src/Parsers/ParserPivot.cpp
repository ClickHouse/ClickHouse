#include <algorithm>
#include <string_view>
#include <utility>
#include <unordered_set>
#include <vector>

#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
#include <Common/StringUtils.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserStreamSettings.h>
#include <Parsers/ParserTablesInSelectQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

class ParserTableExpressionWithoutImplicitAlias : public ParserTableExpression
{
public:
    ParserTableExpressionWithoutImplicitAlias() : ParserTableExpression(false) {}
    using ParserTableExpression::parseImpl;
};

struct PivotSpec
{
    ASTs aggregates;
    ASTPtr pivot_column;
    ASTs values;
};

ASTPtr getTableExpressionSource(const ASTTableExpression & table_expression)
{
    if (table_expression.database_and_table_name)
        return table_expression.database_and_table_name;
    if (table_expression.table_function)
        return table_expression.table_function;
    return table_expression.subquery;
}

String getTableExpressionAlias(const ASTTableExpression & table_expression)
{
    auto source = getTableExpressionSource(table_expression);
    return source ? source->tryGetAlias() : String{};
}

void setTableExpressionAlias(ASTTableExpression & table_expression, const String & alias)
{
    auto source = getTableExpressionSource(table_expression);
    if (source)
        source->setAlias(alias);
}

String getPivotWrittenQualifier(const ASTTableExpression & table_expression)
{
    if (auto alias = getTableExpressionAlias(table_expression); !alias.empty())
        return alias;

    if (const auto * identifier = table_expression.database_and_table_name
            ? table_expression.database_and_table_name->as<ASTIdentifier>()
            : nullptr)
        return identifier->shortName();

    return {};
}

constexpr std::string_view PIVOT_SOURCE_ALIAS = "__pivot_source";

bool hasTopLevelPivot(IParser::Pos pos)
{
    int round_depth = 0;
    int square_depth = 0;

    while (pos->type != TokenType::EndOfStream && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++round_depth;
        else if (pos->type == TokenType::ClosingRoundBracket)
        {
            if (round_depth == 0)
                return false;
            --round_depth;
        }
        else if (pos->type == TokenType::OpeningSquareBracket)
            ++square_depth;
        else if (pos->type == TokenType::ClosingSquareBracket)
        {
            if (square_depth == 0)
                return false;
            --square_depth;
        }
        else if (round_depth == 0 && square_depth == 0)
        {
            if (pos->type == TokenType::Comma)
                return false;

            if (pos->type == TokenType::BareWord)
            {
                std::string_view word{pos->begin, pos->size()};
                if (equalsCaseInsensitive(word, "PIVOT"))
                    return true;
                if (equalsCaseInsensitive(word, "JOIN")
                    || equalsCaseInsensitive(word, "SELECT")
                    || equalsCaseInsensitive(word, "PREWHERE")
                    || equalsCaseInsensitive(word, "WHERE")
                    || equalsCaseInsensitive(word, "GROUP")
                    || equalsCaseInsensitive(word, "HAVING")
                    || equalsCaseInsensitive(word, "WINDOW")
                    || equalsCaseInsensitive(word, "QUALIFY")
                    || equalsCaseInsensitive(word, "ORDER")
                    || equalsCaseInsensitive(word, "LIMIT")
                    || equalsCaseInsensitive(word, "SETTINGS")
                    || equalsCaseInsensitive(word, "FORMAT")
                    || equalsCaseInsensitive(word, "INTO")
                    || equalsCaseInsensitive(word, "PARALLEL")
                    || equalsCaseInsensitive(word, "UNION")
                    || equalsCaseInsensitive(word, "EXCEPT")
                    || equalsCaseInsensitive(word, "INTERSECT"))
                    return false;
            }
        }

        ++pos;
    }

    return false;
}

String qualifyPivotIdentifier(ASTPtr & node, const String & written_qualifier, const String & target_qualifier)
{
    auto & identifier = node->as<ASTIdentifier &>();
    String short_name = identifier.shortName();

    if (identifier.compound())
    {
        if (identifier.name_parts.size() != 2
            || written_qualifier.empty()
            || identifier.name_parts.front() != written_qualifier)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "PIVOT currently supports simple source columns or columns qualified by the source name or alias; unsupported identifier {}",
                identifier.name());
    }

    String alias = identifier.tryGetAlias();
    node = make_intrusive<ASTIdentifier>(std::vector<String>{target_qualifier, short_name});
    if (!alias.empty())
        node->setAlias(alias);
    return short_name;
}

bool parsePivotBody(IParser::Pos & pos, PivotSpec & spec, Expected & expected)
{
    ParserToken open(TokenType::OpeningRoundBracket);
    ParserToken close(TokenType::ClosingRoundBracket);

    if (!open.ignore(pos, expected))
        return false;

    ASTPtr aggregates;
    if (!ParserNotEmptyExpressionList(false).parse(pos, aggregates, expected))
        return false;

    if (!ParserKeyword(Keyword::FOR).ignore(pos, expected))
        return false;

    ASTPtr pivot_column;
    if (!ParserCompoundIdentifier(false, false).parse(pos, pivot_column, expected))
        return false;

    if (!ParserKeyword(Keyword::IN).ignore(pos, expected))
        return false;

    if (!open.ignore(pos, expected))
        return false;

    ASTPtr values;
    if (!ParserNotEmptyExpressionList(false).parse(pos, values, expected))
        return false;

    if (!close.ignore(pos, expected) || !close.ignore(pos, expected))
        return false;

    spec.aggregates = std::move(aggregates->children);
    spec.pivot_column = std::move(pivot_column);
    spec.values = std::move(values->children);
    return true;
}

bool collectPivotSourceColumns(
    ASTPtr & node,
    const String & written_qualifier,
    std::vector<String> & columns,
    std::unordered_set<String> & seen)
{
    if (!node)
        return true;

    if (node->as<ASTSubquery>() || node->as<ASTSelectQuery>() || node->as<ASTSelectWithUnionQuery>())
        return false;

    if (node->as<ASTAsterisk>() || node->as<ASTQualifiedAsterisk>())
        return true;

    if (const auto * function = node->as<ASTFunction>())
    {
        if (function->isLambdaFunction() || equalsCaseInsensitive(function->name, "lambda"))
            return false;

        /// Parametric-function parameters are constants, not source-column expressions. Traverse only
        /// the argument list so a query-scoped constant used as a parameter is not rewritten as a column.
        if (!function->arguments)
            return true;
        for (auto & argument : function->arguments->children)
            if (!collectPivotSourceColumns(argument, written_qualifier, columns, seen))
                return false;
        return true;
    }

    if (const auto * identifier = node->as<ASTIdentifier>())
    {
        if (identifier->compound()
            && (identifier->name_parts.size() != 2
                || written_qualifier.empty()
                || identifier->name_parts.front() != written_qualifier))
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "PIVOT currently supports simple source columns or columns qualified by the source name or alias; unsupported identifier {}",
                identifier->name());

        String name = identifier->shortName();
        if (seen.emplace(name).second)
            columns.push_back(std::move(name));
        return true;
    }

    for (auto & child : node->children)
        if (!collectPivotSourceColumns(child, written_qualifier, columns, seen))
            return false;

    return true;
}

ASTPtr makePivotAsterisk(const std::vector<String> & excluded_columns)
{
    auto asterisk = make_intrusive<ASTAsterisk>();
    if (excluded_columns.empty())
        return asterisk;

    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    for (const auto & column : excluded_columns)
        except->children.push_back(make_intrusive<ASTIdentifier>(column));

    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(except);
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);
    return asterisk;
}

String getPivotValueName(const ASTPtr & value)
{
    if (auto alias = value->tryGetAlias(); !alias.empty())
        return alias;

    const auto * literal = value->as<ASTLiteral>();
    if (!literal)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "PIVOT IN values must be literals or have explicit aliases");

    if (literal->value.getType() == Field::Types::String)
        return literal->value.safeGet<String>();

    return applyVisitor(FieldVisitorToString{}, literal->value);
}

ASTPtr makePivotCondition(const ASTPtr & pivot_column, const ASTPtr & value)
{
    auto value_without_alias = value->clone();
    value_without_alias->setAlias({});

    const auto & literal = value->as<ASTLiteral &>();
    if (literal.value.isNull())
        return makeASTFunction("isNull", pivot_column->clone());

    return makeASTFunction("equals", pivot_column->clone(), std::move(value_without_alias));
}

ASTPtr makePivotAggregate(const ASTPtr & aggregate, const ASTPtr & pivot_column, const ASTPtr & value, const String & output_name)
{
    auto result = aggregate->clone();
    auto * function = result->as<ASTFunction>();
    if (!function)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "PIVOT aggregate expression must be a function call");

    result->setAlias({});
    auto condition = makePivotCondition(pivot_column, value);

    if (!function->arguments)
    {
        function->arguments = make_intrusive<ASTExpressionList>();
        function->children.push_back(function->arguments);
    }

    if (equalsCaseInsensitive(function->name, "count"))
    {
        auto & children = function->arguments->children;
        children.erase(std::remove_if(children.begin(), children.end(), [](const ASTPtr & child)
        {
            return child->as<ASTAsterisk>() || child->as<ASTQualifiedAsterisk>();
        }), children.end());
    }

    /// Match aggregate FILTER: append the -If combinator and let semantic aggregate resolution
    /// validate the root function. Existing -If aggregates become *IfIf and are rejected cleanly
    /// by AggregateFunctionFactory rather than being mistaken for ordinary scalar functions.
    function->name += "If";
    function->arguments->children.push_back(std::move(condition));

    result->setAlias(output_name);
    return result;
}

ASTPtr rewritePivot(ASTPtr source, const PivotSpec & spec, const String & result_alias)
{
    if (spec.aggregates.empty() || spec.values.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "PIVOT requires at least one aggregate and one IN value");

    auto & table_expression = source->as<ASTTableExpression &>();
    const String written_qualifier = getPivotWrittenQualifier(table_expression);
    String target_qualifier = written_qualifier;
    if (target_qualifier.empty())
    {
        target_qualifier = String(PIVOT_SOURCE_ALIAS);
        setTableExpressionAlias(table_expression, target_qualifier);
    }

    ASTPtr pivot_column = spec.pivot_column->clone();
    if (!pivot_column->as<ASTIdentifier>())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "PIVOT FOR expression must be a column identifier");

    std::vector<String> excluded_columns;
    std::unordered_set<String> excluded_seen;
    auto add_excluded = [&](String name)
    {
        if (excluded_seen.emplace(name).second)
            excluded_columns.push_back(std::move(name));
    };

    add_excluded(qualifyPivotIdentifier(pivot_column, written_qualifier, target_qualifier));

    ASTs aggregates;
    aggregates.reserve(spec.aggregates.size());
    const bool multiple_aggregates = spec.aggregates.size() > 1;
    for (const auto & original_aggregate : spec.aggregates)
    {
        ASTPtr aggregate = original_aggregate->clone();
        const auto * function = aggregate->as<ASTFunction>();
        if (!function)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "PIVOT aggregate expression must be a function call");
        if (function->isWindowFunction())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "PIVOT aggregate expression cannot be a window function");

        if (multiple_aggregates && aggregate->tryGetAlias().empty())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Each PIVOT aggregate requires an alias when more than one aggregate is specified");

        std::vector<String> referenced_columns;
        std::unordered_set<String> referenced_seen;
        if (!collectPivotSourceColumns(aggregate, written_qualifier, referenced_columns, referenced_seen))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "PIVOT aggregate expressions cannot contain subqueries or lambdas");

        for (auto & column : referenced_columns)
            add_excluded(std::move(column));
        aggregates.push_back(std::move(aggregate));
    }

    auto select_list = make_intrusive<ASTExpressionList>();
    select_list->children.push_back(makePivotAsterisk(excluded_columns));

    std::unordered_set<String> output_names;
    for (const auto & value : spec.values)
    {
        if (!value->as<ASTLiteral>())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "PIVOT IN values must be literals");

        String value_name = getPivotValueName(value);
        if (value_name.empty())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "An empty PIVOT value requires an explicit alias");

        for (const auto & aggregate : aggregates)
        {
            String aggregate_alias = aggregate->tryGetAlias();
            String output_name = value_name;
            if (multiple_aggregates || !aggregate_alias.empty())
                output_name += "_" + aggregate_alias;

            if (!output_names.emplace(output_name).second)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "PIVOT produces duplicate output column '{}'", output_name);

            select_list->children.push_back(makePivotAggregate(aggregate, pivot_column, value, output_name));
        }
    }

    auto source_element = make_intrusive<ASTTablesInSelectQueryElement>();
    source_element->table_expression = source;
    source_element->children.push_back(source);

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    tables->children.push_back(source_element);

    auto select = make_intrusive<ASTSelectQuery>();
    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));
    select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    select->group_by_all = true;

    /// Keep source columns ahead of propagated WITH aliases in the generated query. Aggregate
    /// identifiers stay unqualified so names that are not source columns can resolve normally.
    auto settings = make_intrusive<ASTSetQuery>();
    settings->is_standalone = false;
    settings->changes.emplace_back("prefer_column_name_to_alias", Field{true});
    select->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings));

    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(select);

    auto select_with_union = make_intrusive<ASTSelectWithUnionQuery>();
    select_with_union->children.push_back(list_of_selects);
    select_with_union->list_of_selects = list_of_selects;

    auto result = make_intrusive<ASTTableExpression>();
    result->subquery = make_intrusive<ASTSubquery>(std::move(select_with_union));
    result->children.push_back(result->subquery);
    if (!result_alias.empty())
        result->subquery->setAlias(result_alias);

    return result;
}

bool parseImplicitAliasTail(IParser::Pos & pos, ASTTableExpression & table_expression, Expected & expected)
{
    if (pos->type == TokenType::OpeningRoundBracket)
    {
        ++pos;
        ParserAliasesExpressionList column_aliases_parser;
        if (!column_aliases_parser.parse(pos, table_expression.column_aliases, expected))
            return false;
        if (pos->type != TokenType::ClosingRoundBracket)
            return false;
        ++pos;
    }

    if (ParserKeyword(Keyword::FINAL).ignore(pos, expected))
        table_expression.final = true;

    if (ParserKeyword(Keyword::SAMPLE).ignore(pos, expected))
    {
        ParserSampleRatio ratio;
        if (!ratio.parse(pos, table_expression.sample_size, expected))
            return false;
        if (ParserKeyword(Keyword::OFFSET).ignore(pos, expected))
            if (!ratio.parse(pos, table_expression.sample_offset, expected))
                return false;
    }

    if (ParserKeyword(Keyword::STREAM).ignore(pos, expected))
    {
        ParserStreamSettings stream_settings_parser;
        if (!stream_settings_parser.parse(pos, table_expression.stream_settings, expected))
            return false;
    }

    /// ParserTableExpression::parseImpl normally registers these typed fields in `children` after
    /// parsing the whole table expression. This wrapper parses them after an implicit alias, so mirror
    /// the same canonical child order here.
    if (table_expression.sample_size)
        table_expression.children.emplace_back(table_expression.sample_size);
    if (table_expression.sample_offset)
        table_expression.children.emplace_back(table_expression.sample_offset);
    if (table_expression.stream_settings)
        table_expression.children.emplace_back(table_expression.stream_settings);
    if (table_expression.column_aliases)
        table_expression.children.emplace_back(table_expression.column_aliases);

    return true;
}

String parseOptionalAlias(IParser::Pos & pos, Expected & expected, bool allow_alias_without_as_keyword)
{
    ParserAlias alias_parser(allow_alias_without_as_keyword);
    ASTPtr alias_node;
    if (!alias_parser.parse(pos, alias_node, expected))
        return {};
    return getIdentifierName(alias_node);
}

bool parsePivotTableExpression(
    IParser::Pos & pos,
    ASTPtr & node,
    Expected & expected,
    bool allow_alias_without_as_keyword)
{
    /// Parse the ordinary source with implicit aliases disabled. This prevents an unaliased source
    /// from consuming PIVOT itself as an alias. Explicit AS aliases and ordinary table modifiers are
    /// still handled by the existing ParserTableExpression::parseImpl implementation.
    ParserTableExpressionWithoutImplicitAlias source_parser;
    ASTPtr source;
    if (!source_parser.parseImpl(pos, source, expected))
        return false;

    auto parse_pivot = [&]() -> bool
    {
        if (!ParserKeyword::createDeprecated("PIVOT").ignore(pos, expected))
            return false;

        PivotSpec spec;
        if (!parsePivotBody(pos, spec, expected))
            return false;

        String result_alias = parseOptionalAlias(pos, expected, allow_alias_without_as_keyword);
        node = rewritePivot(std::move(source), spec, result_alias);
        return true;
    };

    /// Common case: unaliased source, or an explicit `AS alias` already parsed by the ordinary path.
    if (IParserBase::wrapParseImpl(pos, parse_pivot))
        return true;

    if (!allow_alias_without_as_keyword || !getTableExpressionAlias(source->as<ASTTableExpression &>()).empty())
        return false;

    /// Support `FROM source s PIVOT (...)`: parse the implicit source alias here, then the modifiers
    /// that the ordinary table-expression parser would have parsed after an alias.
    String source_alias = parseOptionalAlias(pos, expected, true);
    if (source_alias.empty())
        return false;

    auto & source_expression = source->as<ASTTableExpression &>();
    setTableExpressionAlias(source_expression, source_alias);
    if (!parseImplicitAliasTail(pos, source_expression, expected))
        return false;

    return parse_pivot();
}

}


bool ParserTableExpression::parse(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!hasTopLevelPivot(pos))
        return IParserBase::parse(pos, node, expected);

    Expected pivot_expected = expected;
    pivot_expected.add(pos, getName());

    ASTPtr pivot_node;
    if (IParserBase::wrapParseImpl(pos, IParserBase::IncreaseDepthTag{}, [&]
        {
            return parsePivotTableExpression(pos, pivot_node, pivot_expected, allow_alias_without_as_keyword);
        }))
    {
        expected = std::move(pivot_expected);
        node = std::move(pivot_node);
        return true;
    }

    return IParserBase::parse(pos, node, expected);
}

}
