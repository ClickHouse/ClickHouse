#include <Parsers/Kusto/KQLParser.h>

#include <Parsers/Kusto/KQLFunctions.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <base/hex.h>

#include <Poco/String.h>


namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int TOO_DEEP_RECURSION;
}

namespace
{

ASTPtr makeLiteral(Field value)
{
    return make_intrusive<ASTLiteral>(std::move(value));
}

ASTPtr makeIdentifier(const String & name)
{
    return make_intrusive<ASTIdentifier>(name);
}

/// The operators that may follow a `|`, mapped to the parse routine that handles them.
/// Anything not in this set is rejected by name, so an unsupported operator says so
/// instead of being silently reinterpreted (the old parser fell through to "table name",
/// which is why `search 'x'` used to fail with "Unknown table expression 'search'").
const std::map<String, KQLOperatorKind> & pipelineOperatorNames()
{
    static const std::map<String, KQLOperatorKind> names{
        {"where", KQLOperatorKind::Where},
        {"filter", KQLOperatorKind::Where},
        {"extend", KQLOperatorKind::Extend},
        {"project", KQLOperatorKind::Project},
        {"project-away", KQLOperatorKind::ProjectAway},
        {"project-keep", KQLOperatorKind::ProjectKeep},
        {"project-rename", KQLOperatorKind::ProjectRename},
        {"summarize", KQLOperatorKind::Summarize},
        {"sort", KQLOperatorKind::Sort},
        {"order", KQLOperatorKind::Sort},
        {"take", KQLOperatorKind::Take},
        {"limit", KQLOperatorKind::Take},
        {"top", KQLOperatorKind::Top},
        {"distinct", KQLOperatorKind::Distinct},
        {"count", KQLOperatorKind::Count},
        {"mv-expand", KQLOperatorKind::MvExpand},
        {"join", KQLOperatorKind::Join},
        {"union", KQLOperatorKind::Union},
        {"as", KQLOperatorKind::As},
        {"render", KQLOperatorKind::Render},
    };
    return names;
}

/// KQL operators this dialect does not implement. Listing them by name is what lets a
/// query that uses one fail with "not supported" instead of being reinterpreted - a bare
/// `search 'x'` would otherwise parse as a table called `search`.
const std::set<String> & unsupportedOperatorNames()
{
    static const std::set<String> names{
        "consume",       "evaluate",     "externaldata", "facet",   "find",     "fork",
        "getschema",     "invoke",       "lookup",       "make-series", "materialize",
        "mv-apply",      "parse",        "parse-kv",     "parse-where", "partition",
        "reduce",        "sample",       "sample-distinct",           "scan",   "search",
        "serialize",     "top-hitters",  "top-nested",
    };
    return names;
}

const std::map<String, KQLJoinKind> & joinKindNames()
{
    static const std::map<String, KQLJoinKind> names{
        {"innerunique", KQLJoinKind::InnerUnique},
        {"inner", KQLJoinKind::Inner},
        {"leftouter", KQLJoinKind::LeftOuter},
        {"rightouter", KQLJoinKind::RightOuter},
        {"fullouter", KQLJoinKind::FullOuter},
        {"leftsemi", KQLJoinKind::LeftSemi},
        {"rightsemi", KQLJoinKind::RightSemi},
        {"leftanti", KQLJoinKind::LeftAnti},
        {"leftantisemi", KQLJoinKind::LeftAnti},
        {"rightanti", KQLJoinKind::RightAnti},
        {"rightantisemi", KQLJoinKind::RightAnti},
    };
    return names;
}

/// KQL scalar type names, as they appear in `datatable` schemas and `typeof(...)`.
const std::map<String, String> & kqlTypeToClickHouseType()
{
    static const std::map<String, String> types{
        {"bool", "Bool"},
        {"boolean", "Bool"},
        {"datetime", "DateTime64(7, 'UTC')"},
        {"date", "DateTime64(7, 'UTC')"},
        {"decimal", "Decimal128(20)"},
        {"dynamic", "String"},
        {"guid", "UUID"},
        {"uuid", "UUID"},
        {"int", "Int32"},
        {"long", "Int64"},
        {"real", "Float64"},
        {"double", "Float64"},
        {"string", "String"},
        {"timespan", "Int64"},
        {"time", "Int64"},
    };
    return types;
}

}

KQLParser::DepthGuard::DepthGuard(KQLParser & parser_, const char * what) : parser(parser_)
{
    ++parser.depth;
    if (parser.depth > parser.max_depth)
    {
        --parser.depth;
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Maximum parse depth ({}) exceeded while parsing a KQL {}. Consider raising the max_parser_depth setting",
            parser.max_depth,
            what);
    }
}

KQLParser::KQLParser(const char * query_begin_, std::vector<KQLToken> tokens_, size_t max_depth_)
    : query_begin(query_begin_), tokens(std::move(tokens_)), max_depth(max_depth_)
{
}

const KQLToken & KQLParser::lookahead(size_t offset) const
{
    const size_t target = index + offset;
    return target < tokens.size() ? tokens[target] : tokens.back();
}

const char * KQLParser::getEndPosition() const
{
    return current().begin;
}

bool KQLParser::atKeyword(std::string_view keyword) const
{
    if (current().type != KQLTokenType::BareWord)
        return false;
    const std::string_view text = current().text();
    if (text.size() != keyword.size())
        return false;
    for (size_t i = 0; i < text.size(); ++i)
        if (toLowerIfAlphaASCII(text[i]) != keyword[i])
            return false;
    return true;
}

bool KQLParser::consumeKeyword(std::string_view keyword)
{
    if (!atKeyword(keyword))
        return false;
    ++index;
    return true;
}

bool KQLParser::consume(KQLTokenType type)
{
    if (current().type != type)
        return false;
    ++index;
    return true;
}

void KQLParser::expect(KQLTokenType type)
{
    if (!consume(type))
        fail(fmt::format("expected {}", getKQLTokenName(type)));
}

void KQLParser::expectKeyword(std::string_view keyword)
{
    if (!consumeKeyword(keyword))
        fail(fmt::format("expected '{}'", keyword));
}

String KQLParser::expectIdentifierName()
{
    /// `['column name']` and `["column name"]` are how KQL quotes an identifier.
    if (at(KQLTokenType::OpeningSquareBracket) && lookahead().type == KQLTokenType::StringLiteral
        && lookahead(2).type == KQLTokenType::ClosingSquareBracket)
    {
        String name = lookahead().inner;
        index += 3;
        return name;
    }

    if (!at(KQLTokenType::BareWord))
        fail("expected a column or table name");

    String name(current().text());
    ++index;
    return name;
}

void KQLParser::fail(const String & message) const
{
    failAt(current(), message);
}

void KQLParser::failAt(const KQLToken & token, const String & message) const
{
    const size_t offset = token.begin >= query_begin ? static_cast<size_t>(token.begin - query_begin) : 0;

    /// Point at what is actually there, so "expected ')'" is followed by what was found.
    String found;
    if (token.isError())
        found = token.inner;
    else if (token.isEnd())
        found = "end of query";
    else
        found = fmt::format("'{}'", token.text());

    throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error in KQL query at position {}: {}, found {}", offset + 1, message, found);
}

KQLTabularExpressionPtr KQLParser::parseQuery()
{
    /// `let` bindings accumulate until the tabular expression that uses them. They live in
    /// `scope`, so two queries parsed on the same thread cannot see each other's bindings.
    bool saw_let = false;
    while (atKeyword("let"))
    {
        saw_let = true;
        parseLetStatement();
        if (!consume(KQLTokenType::Semicolon))
        {
            if (at(KQLTokenType::EndOfStream))
                fail("a query must end with a tabular expression, not a 'let' statement");
            fail("expected ';' after a 'let' statement");
        }
    }

    if (at(KQLTokenType::EndOfStream))
        fail(saw_let ? "a query must end with a tabular expression, not a 'let' statement" : "empty query");

    KQLTabularExpressionPtr result = parseTabularExpression();

    consume(KQLTokenType::Semicolon);
    return result;
}

void KQLParser::parseLetStatement()
{
    expectKeyword("let");
    const KQLToken & name_token = current();
    const String name = expectIdentifierName();
    expect(KQLTokenType::Equals);

    if (scope.scalars.contains(name) || scope.tabulars.contains(name))
        failAt(name_token, fmt::format("'{}' is already defined in this query", name));

    /// A `let` may bind either a scalar or a whole tabular expression. Only a parenthesized
    /// form or something that starts a pipeline can be tabular; everything else is scalar.
    const bool looks_tabular = at(KQLTokenType::OpeningRoundBracket) || atKeyword("datatable") || atKeyword("range")
        || atKeyword("print")
        || (at(KQLTokenType::BareWord) && lookahead().type == KQLTokenType::Pipe);

    if (looks_tabular)
    {
        const size_t saved_index = index;
        if (at(KQLTokenType::OpeningRoundBracket))
        {
            /// `let x = (1 + 2);` is a scalar; `let T = (Events | take 1);` is tabular.
            /// Only the pipeline form is treated as tabular.
            size_t probe = index + 1;
            int nesting = 1;
            bool has_top_level_pipe = false;
            while (probe < tokens.size() && nesting > 0)
            {
                const KQLTokenType type = tokens[probe].type;
                if (type == KQLTokenType::OpeningRoundBracket)
                    ++nesting;
                else if (type == KQLTokenType::ClosingRoundBracket)
                    --nesting;
                else if (type == KQLTokenType::Pipe && nesting == 1)
                    has_top_level_pipe = true;
                else if (type == KQLTokenType::EndOfStream || type == KQLTokenType::Error)
                    break;
                ++probe;
            }
            if (!has_top_level_pipe)
            {
                scope.scalars[name] = parseExpression();
                return;
            }
            expect(KQLTokenType::OpeningRoundBracket);
            scope.tabulars[name] = parseTabularExpression();
            expect(KQLTokenType::ClosingRoundBracket);
            return;
        }

        index = saved_index;
        scope.tabulars[name] = parseTabularExpression();
        return;
    }

    scope.scalars[name] = parseExpression();
}

KQLTabularExpressionPtr KQLParser::parseTabularExpression()
{
    DepthGuard guard(*this, "tabular expression");

    auto result = std::make_shared<KQLTabularExpression>();
    result->source = parseSource();

    while (consume(KQLTokenType::Pipe))
        result->operators.push_back(parsePipelineOperator());

    return result;
}

KQLSourcePtr KQLParser::parseSource()
{
    if (atKeyword("print"))
        return parsePrintSource();
    if (atKeyword("datatable"))
        return parseDataTableSource();
    if (atKeyword("range"))
        return parseRangeSource();

    if (at(KQLTokenType::OpeningRoundBracket))
    {
        ++index;
        auto source = std::make_shared<KQLSource>();
        source->kind = KQLSourceKind::Subquery;
        source->inputs.push_back(parseTabularExpression());
        expect(KQLTokenType::ClosingRoundBracket);
        return source;
    }

    if (atKeyword("union"))
    {
        ++index;
        auto source = std::make_shared<KQLSource>();
        source->kind = KQLSourceKind::Union;
        do
        {
            if (at(KQLTokenType::OpeningRoundBracket))
            {
                source->inputs.push_back(parseParenthesizedTabularExpression());
            }
            else
            {
                auto operand = std::make_shared<KQLTabularExpression>();
                operand->source = parseSource();
                source->inputs.push_back(operand);
            }
        } while (consume(KQLTokenType::Comma));

        if (source->inputs.size() < 2)
            fail("'union' needs at least two inputs");
        return source;
    }

    /// A bare name: either a `let`-bound tabular expression or a table.
    const KQLToken & name_token = current();
    const String name = expectIdentifierName();

    if (auto it = scope.tabulars.find(name); it != scope.tabulars.end())
    {
        auto source = std::make_shared<KQLSource>();
        source->kind = KQLSourceKind::Subquery;
        source->inputs.push_back(it->second);
        return source;
    }

    /// An operator keyword here is not a table name. The hyphenated operators arrive as
    /// three tokens, so the check has to consider `<word>-<word>` too.
    String lowered_name = Poco::toLower(name);
    if (at(KQLTokenType::Minus) && lookahead().type == KQLTokenType::BareWord)
        lowered_name += "-" + Poco::toLower(String(lookahead().text()));

    if (pipelineOperatorNames().contains(lowered_name))
        failAt(name_token, fmt::format("'{}' is a pipeline operator, so it must follow a '|'", lowered_name));
    if (unsupportedOperatorNames().contains(lowered_name))
        failAt(name_token, fmt::format("'{}' is not a supported KQL operator", lowered_name));

    auto source = std::make_shared<KQLSource>();
    source->kind = KQLSourceKind::Table;
    source->table = name;

    /// `database.table`.
    if (consume(KQLTokenType::Dot))
    {
        source->database = source->table;
        source->table = expectIdentifierName();
    }

    return source;
}

KQLSourcePtr KQLParser::parsePrintSource()
{
    expectKeyword("print");
    auto source = std::make_shared<KQLSource>();
    source->kind = KQLSourceKind::Print;
    source->projections = parseNamedExpressionList();
    return source;
}

KQLSourcePtr KQLParser::parseDataTableSource()
{
    expectKeyword("datatable");
    auto source = std::make_shared<KQLSource>();
    source->kind = KQLSourceKind::DataTable;

    expect(KQLTokenType::OpeningRoundBracket);
    do
    {
        source->column_names.push_back(expectIdentifierName());
        expect(KQLTokenType::Colon);

        const KQLToken & type_token = current();
        const String kql_type = Poco::toLower(String(expectIdentifierName()));
        auto it = kqlTypeToClickHouseType().find(kql_type);
        if (it == kqlTypeToClickHouseType().end())
            failAt(type_token, fmt::format("'{}' is not a KQL scalar type", kql_type));
        source->column_types.push_back(it->second);
    } while (consume(KQLTokenType::Comma));
    expect(KQLTokenType::ClosingRoundBracket);

    expect(KQLTokenType::OpeningSquareBracket);
    if (!at(KQLTokenType::ClosingSquareBracket))
    {
        do
        {
            /// A trailing comma before `]` is allowed, as in Kusto.
            if (at(KQLTokenType::ClosingSquareBracket))
                break;
            source->values.push_back(parseExpression());
        } while (consume(KQLTokenType::Comma));
    }
    expect(KQLTokenType::ClosingSquareBracket);

    const size_t width = source->column_names.size();
    if (source->values.size() % width != 0)
        fail(fmt::format(
            "datatable has {} columns but {} values, which is not a whole number of rows", width, source->values.size()));

    return source;
}

KQLSourcePtr KQLParser::parseRangeSource()
{
    expectKeyword("range");
    auto source = std::make_shared<KQLSource>();
    source->kind = KQLSourceKind::Range;
    source->range_column = expectIdentifierName();
    expectKeyword("from");
    source->range_from = parseExpression();
    expectKeyword("to");
    source->range_to = parseExpression();
    expectKeyword("step");
    source->range_step = parseExpression();
    return source;
}

KQLOperatorPtr KQLParser::parsePipelineOperator()
{
    DepthGuard guard(*this, "pipeline operator");

    if (!at(KQLTokenType::BareWord))
        fail("expected a pipeline operator after '|'");

    String name = Poco::toLower(String(current().text()));
    const KQLToken & name_token = current();

    /// `sort by` / `order by`, and the hyphenated operators the lexer splits on '-'.
    if ((name == "sort" || name == "order") && lookahead().type == KQLTokenType::BareWord)
    {
        /// consumed by parseSort, which expects the `by`
    }
    else if (lookahead().type == KQLTokenType::Minus && lookahead(2).type == KQLTokenType::BareWord)
    {
        name += "-";
        name += Poco::toLower(String(lookahead(2).text()));
    }

    auto it = pipelineOperatorNames().find(name);
    if (it == pipelineOperatorNames().end())
        failAt(name_token, fmt::format("'{}' is not a supported KQL operator", name));

    /// Consume the operator name (one token, or three for the hyphenated forms).
    if (name.find('-') != String::npos)
        index += 3;
    else
        ++index;

    KQLOperatorPtr result;
    switch (it->second)
    {
        case KQLOperatorKind::Where: result = parseWhere(); break;
        case KQLOperatorKind::Extend: result = parseExtend(); break;
        case KQLOperatorKind::Project: result = parseProject(); break;
        case KQLOperatorKind::ProjectAway: result = parseProjectAwayOrKeep(KQLOperatorKind::ProjectAway); break;
        case KQLOperatorKind::ProjectKeep: result = parseProjectAwayOrKeep(KQLOperatorKind::ProjectKeep); break;
        case KQLOperatorKind::ProjectRename: result = parseProjectRename(); break;
        case KQLOperatorKind::Summarize: result = parseSummarize(); break;
        case KQLOperatorKind::Sort: result = parseSort(); break;
        case KQLOperatorKind::Take: result = parseTake(); break;
        case KQLOperatorKind::Top: result = parseTop(); break;
        case KQLOperatorKind::Distinct: result = parseDistinct(); break;
        case KQLOperatorKind::Count: result = parseCount(); break;
        case KQLOperatorKind::MvExpand: result = parseMvExpand(); break;
        case KQLOperatorKind::Join: result = parseJoin(); break;
        case KQLOperatorKind::Union: result = parseUnion(); break;
        case KQLOperatorKind::As: result = parseAs(); break;
        case KQLOperatorKind::Render: result = parseRender(); break;
    }

    result->name = name;
    return result;
}

KQLOperatorPtr KQLParser::parseWhere()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Where;
    op->predicate = parseExpression();
    return op;
}

KQLOperatorPtr KQLParser::parseExtend()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Extend;
    op->expressions = parseNamedExpressionList();
    return op;
}

KQLOperatorPtr KQLParser::parseProject()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Project;
    op->expressions = parseNamedExpressionList();
    return op;
}

KQLOperatorPtr KQLParser::parseProjectAwayOrKeep(KQLOperatorKind kind)
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = kind;
    do
    {
        /// The argument is a column name possibly containing `*`, not an expression.
        if (at(KQLTokenType::Asterisk))
        {
            op->column_patterns.emplace_back("*");
            ++index;
            continue;
        }

        String pattern = expectIdentifierName();
        /// `Col*` lexes as an identifier followed by `*`.
        while (at(KQLTokenType::Asterisk))
        {
            pattern += "*";
            ++index;
            if (at(KQLTokenType::BareWord))
            {
                pattern += current().text();
                ++index;
            }
        }
        op->column_patterns.push_back(std::move(pattern));
    } while (consume(KQLTokenType::Comma));
    return op;
}

KQLOperatorPtr KQLParser::parseProjectRename()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::ProjectRename;
    do
    {
        String new_name = expectIdentifierName();
        expect(KQLTokenType::Equals);
        String old_name = expectIdentifierName();
        op->renames.emplace_back(std::move(new_name), std::move(old_name));
    } while (consume(KQLTokenType::Comma));
    return op;
}

KQLOperatorPtr KQLParser::parseSummarize()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Summarize;

    /// `summarize by X` aggregates nothing and just groups - the aggregation list is optional.
    if (!atKeyword("by"))
    {
        do
        {
            op->expressions.push_back(parseNamedExpression());
        } while (consume(KQLTokenType::Comma));
    }

    if (consumeKeyword("by"))
    {
        do
        {
            op->by_expressions.push_back(parseNamedExpression());
        } while (consume(KQLTokenType::Comma));
    }

    if (op->expressions.empty() && op->by_expressions.empty())
        fail("'summarize' needs an aggregation or a 'by' clause");

    return op;
}

std::vector<KQLSortItem> KQLParser::parseSortItems()
{
    std::vector<KQLSortItem> items;
    do
    {
        KQLSortItem item;
        item.expression = parseExpression();

        if (consumeKeyword("asc"))
            item.descending = false;
        else if (consumeKeyword("desc"))
            item.descending = true;

        /// Kusto puts nulls at the "small" end: first when ascending, last when descending.
        item.nulls_first = !item.descending;

        if (consumeKeyword("nulls"))
        {
            if (consumeKeyword("first"))
                item.nulls_first = true;
            else if (consumeKeyword("last"))
                item.nulls_first = false;
            else
                fail("expected 'first' or 'last' after 'nulls'");
        }

        items.push_back(std::move(item));
    } while (consume(KQLTokenType::Comma));
    return items;
}

KQLOperatorPtr KQLParser::parseSort()
{
    expectKeyword("by");
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Sort;
    op->sort_items = parseSortItems();
    return op;
}

KQLOperatorPtr KQLParser::parseTake()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Take;
    op->limit = parseExpression();
    return op;
}

KQLOperatorPtr KQLParser::parseTop()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Top;
    op->limit = parseExpression();
    expectKeyword("by");
    op->sort_items = parseSortItems();
    return op;
}

KQLOperatorPtr KQLParser::parseDistinct()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Distinct;

    if (at(KQLTokenType::Asterisk))
    {
        ++index;
        return op; /// Empty expression list means "all columns".
    }

    do
    {
        KQLNamedExpression named;
        named.expression = parseExpression();
        op->expressions.push_back(std::move(named));
    } while (consume(KQLTokenType::Comma));
    return op;
}

KQLOperatorPtr KQLParser::parseCount()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Count;
    op->alias = "Count";
    if (consumeKeyword("as"))
        op->alias = expectIdentifierName();
    return op;
}

KQLOperatorPtr KQLParser::parseMvExpand()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::MvExpand;
    do
    {
        op->expressions.push_back(parseNamedExpression());
    } while (consume(KQLTokenType::Comma));
    return op;
}

KQLTabularExpressionPtr KQLParser::parseParenthesizedTabularExpression()
{
    expect(KQLTokenType::OpeningRoundBracket);
    KQLTabularExpressionPtr result = parseTabularExpression();
    expect(KQLTokenType::ClosingRoundBracket);
    return result;
}

KQLOperatorPtr KQLParser::parseJoin()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Join;

    if (consumeKeyword("kind"))
    {
        expect(KQLTokenType::Equals);
        const KQLToken & kind_token = current();
        const String kind_name = Poco::toLower(String(expectIdentifierName()));
        auto it = joinKindNames().find(kind_name);
        if (it == joinKindNames().end())
            failAt(kind_token, fmt::format("'{}' is not a supported join kind", kind_name));
        op->join_kind = it->second;
    }

    /// `hint.*` options tune Kusto's distribution strategy and have no ClickHouse equivalent.
    /// Rejecting is better than accepting a hint we would silently ignore.
    if (atKeyword("hint"))
        fail("join hints are not supported");

    op->inputs.push_back(parseParenthesizedTabularExpression());

    expectKeyword("on");
    do
    {
        /// `on Key` or `on $left.A == $right.B`.
        if (at(KQLTokenType::BareWord) && current().text() == "$left")
        {
            index += 1;
            expect(KQLTokenType::Dot);
            String left = expectIdentifierName();
            expect(KQLTokenType::DoubleEquals);
            if (!(at(KQLTokenType::BareWord) && current().text() == "$right"))
                fail("expected '$right' on the right-hand side of a join condition");
            ++index;
            expect(KQLTokenType::Dot);
            String right = expectIdentifierName();
            op->join_keys.emplace_back(std::move(left), std::move(right));
        }
        else
        {
            String key = expectIdentifierName();
            op->join_keys.emplace_back(key, key);
        }
    } while (consume(KQLTokenType::Comma));

    return op;
}

KQLOperatorPtr KQLParser::parseUnion()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Union;

    if (consumeKeyword("kind"))
    {
        expect(KQLTokenType::Equals);
        const KQLToken & kind_token = current();
        const String kind_name = Poco::toLower(String(expectIdentifierName()));
        if (kind_name == "outer")
            op->union_kind_outer = true;
        else if (kind_name != "inner")
            failAt(kind_token, fmt::format("'{}' is not a supported union kind", kind_name));
        else
            fail("'union kind=inner' is not supported");
    }

    do
    {
        if (at(KQLTokenType::OpeningRoundBracket))
        {
            op->inputs.push_back(parseParenthesizedTabularExpression());
        }
        else
        {
            auto operand = std::make_shared<KQLTabularExpression>();
            operand->source = parseSource();
            op->inputs.push_back(operand);
        }
    } while (consume(KQLTokenType::Comma));

    return op;
}

KQLOperatorPtr KQLParser::parseAs()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::As;
    op->alias = expectIdentifierName();
    return op;
}

KQLOperatorPtr KQLParser::parseRender()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Render;
    /// `render` is a client-side hint even in Kusto; the server returns the same rows.
    /// Consume the visualization name and any `with (...)` options, then do nothing.
    if (at(KQLTokenType::BareWord))
        ++index;
    if (consumeKeyword("with"))
    {
        expect(KQLTokenType::OpeningRoundBracket);
        int nesting = 1;
        while (nesting > 0)
        {
            if (at(KQLTokenType::EndOfStream) || at(KQLTokenType::Error))
                fail("unterminated 'with (' in 'render'");
            if (at(KQLTokenType::OpeningRoundBracket))
                ++nesting;
            else if (at(KQLTokenType::ClosingRoundBracket))
                --nesting;
            ++index;
        }
    }
    return op;
}

std::vector<KQLNamedExpression> KQLParser::parseNamedExpressionList()
{
    std::vector<KQLNamedExpression> result;
    do
    {
        result.push_back(parseNamedExpression());
    } while (consume(KQLTokenType::Comma));
    return result;
}

KQLNamedExpression KQLParser::parseNamedExpression()
{
    KQLNamedExpression named;

    /// `Name = expression`, distinguished from the comparison `a == b` by the single '='.
    const bool has_alias = (at(KQLTokenType::BareWord) && lookahead().type == KQLTokenType::Equals)
        || (at(KQLTokenType::OpeningSquareBracket) && lookahead().type == KQLTokenType::StringLiteral
            && lookahead(2).type == KQLTokenType::ClosingSquareBracket && lookahead(3).type == KQLTokenType::Equals);

    if (has_alias)
    {
        named.alias = expectIdentifierName();
        expect(KQLTokenType::Equals);
    }

    named.expression = parseExpression();
    return named;
}

/// Comparison and the logical connectives return `UInt8` in ClickHouse, which prints as
/// 0 and 1. KQL has a real `bool`, so a predicate is presented as one - otherwise
/// `print a > b` answers `1` where Kusto answers `true`.
namespace
{
bool producesBoolean(const ASTPtr & node)
{
    const auto * function = node->as<ASTFunction>();
    if (!function)
        return false;

    static const std::set<String> boolean_functions{
        "equals", "notEquals", "less", "lessOrEquals", "greater", "greaterOrEquals",
        "and", "or", "not", "in", "notIn", "match", "startsWith", "endsWith"};
    return boolean_functions.contains(function->name);
}
}

ASTPtr KQLParser::parseExpression()
{
    DepthGuard guard(*this, "expression");

    ASTPtr result = parseOr();
    if (producesBoolean(result))
        return makeASTFunction("toBool", result);
    return result;
}

ASTPtr KQLParser::parseOr()
{
    ASTPtr left = parseAnd();
    while (atKeyword("or"))
    {
        ++index;
        left = makeASTFunction("or", left, parseAnd());
    }
    return left;
}

ASTPtr KQLParser::parseAnd()
{
    ASTPtr left = parseComparison();
    while (atKeyword("and"))
    {
        ++index;
        left = makeASTFunction("and", left, parseComparison());
    }
    return left;
}

ASTPtr KQLParser::parseComparison()
{
    ASTPtr left = parseAdditive();

    /// Comparison does not chain in KQL: `a < b < c` is an error, not `(a < b) < c`.
    static const std::map<KQLTokenType, const char *> comparisons{
        {KQLTokenType::DoubleEquals, "equals"},
        {KQLTokenType::NotEquals, "notEquals"},
        {KQLTokenType::Less, "less"},
        {KQLTokenType::LessOrEquals, "lessOrEquals"},
        {KQLTokenType::Greater, "greater"},
        {KQLTokenType::GreaterOrEquals, "greaterOrEquals"},
    };

    if (auto it = comparisons.find(current().type); it != comparisons.end())
    {
        ++index;
        return makeASTFunction(it->second, left, parseAdditive());
    }

    /// `=~` and `!~` are case-insensitive string equality.
    if (at(KQLTokenType::TildeEquals) || at(KQLTokenType::NotTildeEquals))
    {
        const bool negated = at(KQLTokenType::NotTildeEquals);
        ++index;
        ASTPtr right = parseAdditive();
        ASTPtr equal = makeASTFunction("equals", makeASTFunction("lowerUTF8", left), makeASTFunction("lowerUTF8", right));
        return negated ? ASTPtr(makeASTFunction("not", equal)) : equal;
    }

    if (at(KQLTokenType::Equals))
        fail("'=' is assignment in KQL; use '==' to compare");

    if (ASTPtr word_operator = tryParseWordOperator(left))
        return word_operator;

    return left;
}

ASTPtr KQLParser::parseAdditive()
{
    ASTPtr left = parseMultiplicative();
    while (at(KQLTokenType::Plus) || at(KQLTokenType::Minus))
    {
        const bool is_plus = at(KQLTokenType::Plus);
        ++index;
        left = makeASTFunction(is_plus ? "plus" : "minus", left, parseMultiplicative());
    }
    return left;
}

ASTPtr KQLParser::parseMultiplicative()
{
    ASTPtr left = parseUnary();
    while (at(KQLTokenType::Asterisk) || at(KQLTokenType::Slash) || at(KQLTokenType::Percent))
    {
        const KQLTokenType type = current().type;
        ++index;
        ASTPtr right = parseUnary();
        if (type == KQLTokenType::Asterisk)
            left = makeASTFunction("multiply", left, right);
        else if (type == KQLTokenType::Slash)
            /// KQL divides integers as integers: `7 / 2` is 3, not 3.5. Only the runtime
            /// knows the operand types, so the choice is made there.
            left = makeASTFunction("kqlDivide", left, right);
        else
            left = makeASTFunction("modulo", left, right);
    }
    return left;
}

ASTPtr KQLParser::parseUnary()
{
    if (at(KQLTokenType::Minus))
    {
        ++index;
        return makeASTFunction("negate", parseUnary());
    }
    if (at(KQLTokenType::Plus))
    {
        ++index;
        return parseUnary();
    }
    if (atKeyword("not"))
    {
        ++index;
        return makeASTFunction("not", parseUnary());
    }
    return parsePostfix(parsePrimary());
}

ASTPtr KQLParser::parsePostfix(ASTPtr operand)
{
    while (true)
    {
        if (at(KQLTokenType::OpeningSquareBracket))
        {
            const KQLToken & bracket = current();
            ++index;
            ASTPtr subscript = parseExpression();
            expect(KQLTokenType::ClosingSquareBracket);

            /// KQL's `dynamic` maps to a ClickHouse `Array`, so a subscript is `arrayElement`.
            /// KQL indexes from 0 and ClickHouse from 1. Object member lookup is a different
            /// thing that this mapping does not cover - see `parseDynamicLiteral`.
            if (const auto * literal = subscript->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
                failAt(bracket, "indexing a dynamic object by key is not supported");

            operand = makeASTFunction("arrayElement", operand, makeASTFunction("plus", subscript, makeLiteral(static_cast<Int64>(1))));
            continue;
        }

        if (at(KQLTokenType::Dot) && lookahead().type == KQLTokenType::BareWord)
            fail("member access on a dynamic value is not supported");

        return operand;
    }
}

ASTPtr KQLParser::parseParenthesizedOrInList()
{
    expect(KQLTokenType::OpeningRoundBracket);
    ASTPtr inner = parseExpression();
    expect(KQLTokenType::ClosingRoundBracket);
    return inner;
}

ASTPtr KQLParser::parsePrimary()
{
    DepthGuard guard(*this, "expression");

    const KQLToken & token = current();

    switch (token.type)
    {
        case KQLTokenType::Error:
            fail("invalid token");

        case KQLTokenType::Number:
        {
            const std::string_view text = token.text();
            ++index;
            if (text.size() > 2 && text[0] == '0' && (text[1] == 'x' || text[1] == 'X'))
            {
                UInt64 value = 0;
                for (size_t i = 2; i < text.size(); ++i)
                    value = value * 16 + unhex(text[i]);
                return makeLiteral(value);
            }
            if (text.find('.') != std::string_view::npos || text.find('e') != std::string_view::npos
                || text.find('E') != std::string_view::npos)
                return makeLiteral(std::stod(String(text)));

            /// KQL integer literals are 64-bit signed (`long`).
            const String digits(text);
            errno = 0;
            char * parse_end = nullptr;
            const long long value = std::strtoll(digits.c_str(), &parse_end, 10);
            if (errno == ERANGE || parse_end != digits.c_str() + digits.size())
                failAt(token, "integer literal is out of range");
            return makeLiteral(static_cast<Int64>(value));
        }

        case KQLTokenType::StringLiteral:
        {
            /// The whole point of the rewrite: user text becomes a literal *node*. There is
            /// no representation in which it could be read back as SQL syntax.
            String value = token.inner;
            ++index;
            return makeLiteral(std::move(value));
        }

        case KQLTokenType::Timespan:
        {
            const Int64 ticks = token.timespan_ticks;
            ++index;
            /// A KQL timespan becomes a ClickHouse `Interval` in nanoseconds: the tick is
            /// 100 ns, which no coarser interval kind can represent exactly. As a real
            /// interval it both adds to a datetime and renders as `1.00:00:00` under
            /// `interval_output_format = 'kusto'`.
            if (ticks > std::numeric_limits<Int64>::max() / 100 || ticks < std::numeric_limits<Int64>::min() / 100)
                failAt(token, "timespan literal is too large to represent in nanoseconds");
            return makeASTFunction("toIntervalNanosecond", makeLiteral(ticks * 100));
        }

        case KQLTokenType::DateTimeLiteral:
        {
            String text = token.inner;
            ++index;
            if (Poco::toLower(text) == "null")
                return makeASTFunction("CAST", makeLiteral(Field()), makeLiteral(String("Nullable(DateTime64(7, 'UTC'))")));
            return makeASTFunction("parseDateTime64BestEffortOrNull", makeLiteral(std::move(text)), makeLiteral(static_cast<UInt64>(7)), makeLiteral(String("UTC")));
        }

        case KQLTokenType::GuidLiteral:
        {
            String text = token.inner;
            ++index;
            return makeASTFunction("toUUIDOrNull", makeLiteral(std::move(text)));
        }

        case KQLTokenType::OpeningRoundBracket:
            return parseParenthesizedOrInList();

        case KQLTokenType::OpeningSquareBracket:
        {
            /// `['column name']` - a quoted column reference.
            if (lookahead().type == KQLTokenType::StringLiteral && lookahead(2).type == KQLTokenType::ClosingSquareBracket)
                return makeIdentifier(expectIdentifierName());
            fail("expected a quoted column name");
        }

        case KQLTokenType::Asterisk:
            ++index;
            return make_intrusive<ASTAsterisk>();

        case KQLTokenType::BareWord:
            break;

        default:
            fail("expected an expression");
    }

    const String word(token.text());
    const String lowered = Poco::toLower(word);

    if (lowered == "true" || lowered == "false")
    {
        ++index;
        return makeLiteral(lowered == "true");
    }

    if (lowered == "null")
    {
        ++index;
        return makeLiteral(Field());
    }

    /// `dynamic([1, 2, 3])` / `dynamic({"a": 1})` - a literal of KQL's JSON-ish type.
    if (lowered == "dynamic" && lookahead().type == KQLTokenType::OpeningRoundBracket)
    {
        index += 2;
        ASTPtr value = parseDynamicLiteral();
        expect(KQLTokenType::ClosingRoundBracket);
        return value;
    }

    if (lookahead().type == KQLTokenType::OpeningRoundBracket)
    {
        ++index;
        return parseFunctionCall(lowered);
    }

    /// A plain name: a `let`-bound scalar, or a column reference.
    ++index;
    if (auto it = scope.scalars.find(word); it != scope.scalars.end())
        return it->second->clone();

    return makeIdentifier(word);
}

ASTPtr KQLParser::parseDynamicLiteral()
{
    DepthGuard guard(*this, "dynamic literal");

    /// KQL `dynamic` holds JSON. We build the JSON text here, at parse time, from literal
    /// tokens only - so the result is a single `ASTLiteral(String)` that the JSON functions
    /// read at runtime. Nothing the user typed becomes SQL syntax.
    if (at(KQLTokenType::OpeningSquareBracket))
    {
        ++index;
        Array elements;
        if (!at(KQLTokenType::ClosingSquareBracket))
        {
            do
            {
                ASTPtr element = parseDynamicLiteral();
                const auto * literal = element->as<ASTLiteral>();
                if (!literal)
                    fail("a dynamic literal may only contain constants");
                elements.push_back(literal->value);
            } while (consume(KQLTokenType::Comma));
        }
        expect(KQLTokenType::ClosingSquareBracket);
        return makeLiteral(std::move(elements));
    }

    /// A dynamic *object* has no faithful ClickHouse counterpart under the array mapping
    /// used here, so it is rejected rather than approximated.
    if (at(KQLTokenType::OpeningCurlyBrace))
        fail("dynamic object literals are not supported");

    return parseExpression();
}

ASTPtr KQLParser::parseFunctionCall(const String & name)
{
    DepthGuard guard(*this, "function call");

    const KQLToken & name_token = tokens[index - 1];
    expect(KQLTokenType::OpeningRoundBracket);

    ASTs arguments;
    if (!at(KQLTokenType::ClosingRoundBracket))
    {
        do
        {
            arguments.push_back(parseExpression());
        } while (consume(KQLTokenType::Comma));
    }
    expect(KQLTokenType::ClosingRoundBracket);

    String error;
    ASTPtr result = translateKQLFunction(name, arguments, error);
    if (!result)
        failAt(name_token, error);

    return result;
}

ASTPtr KQLParser::tryParseWordOperator(const ASTPtr & left)
{
    if (!at(KQLTokenType::BareWord))
        return nullptr;

    String op = Poco::toLower(String(current().text()));

    bool negated = false;
    if (op.starts_with('!'))
    {
        negated = true;
        op = op.substr(1);
    }

    const auto finish = [&](ASTPtr predicate) -> ASTPtr
    { return negated ? ASTPtr(makeASTFunction("not", predicate)) : predicate; };

    /// `between (low .. high)`
    if (op == "between")
    {
        ++index;
        expect(KQLTokenType::OpeningRoundBracket);
        ASTPtr low = parseAdditive();
        expect(KQLTokenType::DotDot);
        ASTPtr high = parseAdditive();
        expect(KQLTokenType::ClosingRoundBracket);
        return finish(makeASTFunction(
            "and", makeASTFunction("greaterOrEquals", left, low), makeASTFunction("lessOrEquals", left->clone(), high)));
    }

    /// `in (a, b, c)` and its case-insensitive form `in~`.
    if (op == "in" || op == "in~")
    {
        const bool case_insensitive = op == "in~";
        ++index;
        expect(KQLTokenType::OpeningRoundBracket);
        ASTs elements;
        if (!at(KQLTokenType::ClosingRoundBracket))
        {
            do
            {
                elements.push_back(parseExpression());
            } while (consume(KQLTokenType::Comma));
        }
        expect(KQLTokenType::ClosingRoundBracket);

        auto tuple = makeASTFunction("tuple");
        tuple->arguments->children = elements;

        ASTPtr haystack = left;
        if (case_insensitive)
        {
            haystack = makeASTFunction("lowerUTF8", left);
            for (auto & element : tuple->arguments->children)
                element = makeASTFunction("lowerUTF8", element);
        }
        return finish(makeASTFunction("in", haystack, tuple));
    }

    /// `has_any (a, b)` / `has_all (a, b)`
    if (op == "has_any" || op == "has_all")
    {
        const bool all = op == "has_all";
        const KQLToken & op_token = current();
        ++index;
        expect(KQLTokenType::OpeningRoundBracket);
        ASTs needles;
        do
        {
            needles.push_back(parseExpression());
        } while (consume(KQLTokenType::Comma));
        expect(KQLTokenType::ClosingRoundBracket);

        ASTPtr combined;
        for (const auto & needle : needles)
        {
            String error;
            ASTPtr term = buildKQLStringOperator("has", left->clone(), needle, error);
            if (!term)
                failAt(op_token, error);
            combined = combined ? ASTPtr(makeASTFunction(all ? "and" : "or", combined, term)) : term;
        }
        return finish(combined);
    }

    /// The substring and term operators. Each becomes a matching function whose semantics
    /// match Kusto's, never a LIKE pattern - `contains '50%'` must not match '50x'.
    {
        static const std::set<String> string_operators{
            "contains", "contains_cs", "startswith", "startswith_cs", "endswith", "endswith_cs",
            "has", "has_cs", "hasprefix", "hasprefix_cs", "hassuffix", "hassuffix_cs"};

        if (string_operators.contains(op))
        {
            const KQLToken & op_token = current();
            ++index;
            ASTPtr needle = parseAdditive();
            String error;
            ASTPtr predicate = buildKQLStringOperator(op, left, needle, error);
            if (!predicate)
                failAt(op_token, error);
            return finish(predicate);
        }
    }

    /// `matches regex "..."`
    if (op == "matches")
    {
        if (!(lookahead().type == KQLTokenType::BareWord && Poco::toLower(String(lookahead().text())) == "regex"))
            return nullptr;
        index += 2;
        ASTPtr pattern = parseAdditive();
        return finish(makeASTFunction("match", left, pattern));
    }

    return nullptr;
}

}
