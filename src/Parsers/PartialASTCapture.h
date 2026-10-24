#pragma once

/// Capture of the statement tree built so far when a statement parser fails, for the standalone
/// WebAssembly parser (`utils/wasm-parser`), which reports it as `partial_ast`. Only that build
/// defines `CLICKHOUSE_PARSER_PARTIAL_AST`; everywhere else the hook is a no-op that evaluates
/// none of its arguments, so the parser and its `Expected` stay exactly as they are.
///
/// A call goes right before an existing `return false` of a statement parser, after the statement
/// keyword has committed, or where the parser recovers from a failed optional part and goes on
/// (such a capture is reported only if the parse gets no further). It never assigns to `node` and
/// never moves `pos`. For `SELECT` the last argument is the clause, an `ASTSelectQuery::Expression`;
/// for `INSERT`, `CREATE` and `ALTER` it is the JSON key of the slot that failed. A parser of a part
/// of a statement - a storage definition, an `ALTER` command - captures a fragment the same way,
/// which the statement around it then reports in the slot of that part.

#if defined(CLICKHOUSE_PARSER_PARTIAL_AST)

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParser.h>

namespace DB
{

/// Clones `node` into `expected.partial_ast`, with an `Error` node in the slot named by the JSON key
/// `expected_what`. Defined in `utils/wasm-parser`.
void snapshotPartialAST(Expected & expected, const ASTPtr & node, IParser::Pos pos, const char * expected_what);

/// The same for a `SELECT`, with the `Error` in the clause `slot` of the `ASTSelectQuery` `node`.
void snapshotPartialAST(Expected & expected, const ASTPtr & node, IParser::Pos pos, ASTSelectQuery::Expression slot);

/// Clones a part of a statement - a storage definition, an `ALTER` command - into
/// `expected.partial_ast_fragment`, with the `Error` in the slot named by the JSON key
/// `expected_what`, or, for a null key, as the last element of the `ASTExpressionList` `node`. The
/// capture of the node around it at the same position takes the fragment in place of an `Error`.
void snapshotPartialFragment(Expected & expected, const ASTPtr & node, IParser::Pos pos, const char * expected_what);

/// Held by `ParserSelectQuery::parseImpl`: counts the `SELECT`s being parsed.
class PartialASTSelectScope
{
public:
    explicit PartialASTSelectScope(Expected & expected_)
        : expected(expected_)
    {
        ++expected.partial_ast_selects;
    }

    ~PartialASTSelectScope()
    {
        --expected.partial_ast_selects;
    }

    PartialASTSelectScope(const PartialASTSelectScope &) = delete;
    PartialASTSelectScope & operator=(const PartialASTSelectScope &) = delete;

private:
    Expected & expected;
};

/// Held by `ParserInsertQuery` while it parses its `SELECT`: a capture of a `SELECT` of its union gets
/// the `WITH` written before `INSERT`, which `ast` puts there; one nested in another `SELECT` does not.
class PartialASTOuterWith
{
public:
    PartialASTOuterWith(Expected & expected_, const ASTPtr & with)
        : expected(expected_)
        , previous_with(expected.partial_ast_outer_with)
        , previous_selects(expected.partial_ast_outer_with_selects)
    {
        expected.partial_ast_outer_with = with;
        expected.partial_ast_outer_with_selects = expected.partial_ast_selects;
    }

    ~PartialASTOuterWith()
    {
        expected.partial_ast_outer_with = std::move(previous_with);
        expected.partial_ast_outer_with_selects = previous_selects;
    }

    PartialASTOuterWith(const PartialASTOuterWith &) = delete;
    PartialASTOuterWith & operator=(const PartialASTOuterWith &) = delete;

private:
    Expected & expected;
    ASTPtr previous_with;
    size_t previous_selects;
};

}

#define PARTIAL_AST_SNAPSHOT(expected, node, pos, what) snapshotPartialAST(expected, node, pos, what)
#define PARTIAL_AST_FRAGMENT(expected, node, pos, what) snapshotPartialFragment(expected, node, pos, what)

#else

#define PARTIAL_AST_SNAPSHOT(expected, node, pos, what) ((void)0)
#define PARTIAL_AST_FRAGMENT(expected, node, pos, what) ((void)0)

#endif
