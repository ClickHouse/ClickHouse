#pragma once

/// Capture of the statement tree built so far when a statement parser fails, which the standalone
/// WebAssembly parser (`utils/wasm-parser`) reports as `partial_ast`. Capture is off unless the caller
/// points `Expected::partial_ast` at a `PartialASTState`. With the pointer null - every parse in the
/// server - each hook is one branch that evaluates none of its other arguments, so a failure path
/// neither allocates nor copies anything for it.
///
/// A call goes right before an existing `return false` of a statement parser, after the statement
/// keyword has committed, or where the parser recovers from a failed optional part and goes on
/// (such a capture is reported only if the parse gets no further). It never assigns to `node` and
/// never moves `pos`. For `SELECT` the last argument is the clause, an `ASTSelectQuery::Expression`;
/// for `INSERT`, `CREATE` and `ALTER` it is the JSON key of the slot that failed. A parser of a part
/// of a statement - a storage definition, an `ALTER` command - captures a fragment the same way,
/// which the statement around it then reports in the slot of that part.

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParser.h>
#include <base/defines.h>

#include <optional>
#include <utility>

namespace DB
{

/// A partial tree past these is not captured. They are the limits `ch_parse` holds an `ast` to, as
/// `MAX_PARSER_DEPTH` and `MAX_AST_ELEMENTS` in `wasm_parser.cpp`, which checks that they agree.
constexpr size_t PARTIAL_AST_MAX_DEPTH = 1000;
constexpr size_t PARTIAL_AST_MAX_ELEMENTS = 50000;

/// What a parse with capture on fills in. The caller owns it and points `Expected::partial_ast` at it;
/// `tryParseQuery` clears it at the start of each parse.
struct PartialASTState
{
    /// The captured tree, with an `Error` node in the slot that failed, and the `max_parsed_pos` it was
    /// captured at.
    ASTPtr tree;
    const char * pos = nullptr;

    /// The number of `SELECT`s being parsed, and inside an `INSERT`, the `WITH` written before it and
    /// that number when the `INSERT` starts its own `SELECT`: a capture one `SELECT` deeper is a
    /// `SELECT` of the union of the `INSERT`, which `ast` gives that `WITH`.
    size_t selects = 0;
    ASTPtr outer_with;
    size_t outer_with_selects = 0;

    /// A part of a statement that failed - a storage definition, a dictionary, an `ALTER` command, the
    /// list of commands - captured the same way, with the `max_parsed_pos` it was captured at. The
    /// capture of the node around it at that position takes it in place of an `Error`.
    ASTPtr fragment;
    const char * fragment_pos = nullptr;

    /// The last element of the last list of columns and table properties that parsed, in the order
    /// written, which `ASTColumns` does not keep: an `Error` after the list follows it.
    ASTPtr last_column_element;
};

/// Clones `node` into `expected.partial_ast->tree`, with an `Error` node in the slot named by the JSON
/// key `expected_what`. Called only with capture on.
void snapshotPartialAST(Expected & expected, const ASTPtr & node, IParser::Pos pos, const char * expected_what);

/// The same for a `SELECT`, with the `Error` in the clause `slot` of the `ASTSelectQuery` `node`.
void snapshotPartialAST(Expected & expected, const ASTPtr & node, IParser::Pos pos, ASTSelectQuery::Expression slot);

/// Clones a part of a statement - a storage definition, an `ALTER` command - into
/// `expected.partial_ast->fragment`, with the `Error` in the slot named by the JSON key
/// `expected_what`, or, for a null key, as the last element of the `ASTExpressionList` `node`. The
/// capture of the node around it at the same position takes the fragment in place of an `Error`.
void snapshotPartialFragment(Expected & expected, const ASTPtr & node, IParser::Pos pos, const char * expected_what);

/// Held by `ParserSelectQuery::parseImpl`: counts the `SELECT`s being parsed, with capture on.
class PartialASTSelectScope
{
public:
    explicit PartialASTSelectScope(const Expected & expected)
        : state(expected.partial_ast)
    {
        if (unlikely(state))
            ++state->selects;
    }

    ~PartialASTSelectScope()
    {
        if (unlikely(state))
            --state->selects;
    }

    PartialASTSelectScope(const PartialASTSelectScope &) = delete;
    PartialASTSelectScope & operator=(const PartialASTSelectScope &) = delete;

private:
    PartialASTState * const state;
};

/// Held by `ParserInsertQuery` while it parses its `SELECT`, with capture on: a capture of a `SELECT` of
/// its union gets the `WITH` written before `INSERT`, which `ast` puts there; one nested in another
/// `SELECT` does not.
class PartialASTOuterWith
{
public:
    PartialASTOuterWith(const Expected & expected, const ASTPtr & with)
        : state(expected.partial_ast)
    {
        if (likely(!state))
            return;
        previous_with = std::exchange(state->outer_with, with);
        previous_selects = std::exchange(state->outer_with_selects, state->selects);
    }

    ~PartialASTOuterWith()
    {
        if (likely(!state))
            return;
        state->outer_with = std::move(previous_with);
        state->outer_with_selects = previous_selects;
    }

    PartialASTOuterWith(const PartialASTOuterWith &) = delete;
    PartialASTOuterWith & operator=(const PartialASTOuterWith &) = delete;

private:
    PartialASTState * const state;
    ASTPtr previous_with;
    size_t previous_selects = 0;
};

/// Held where a parser tries an alternative on a copy of `Expected` and drops the copy if the
/// alternative fails. The copy points at the same state, so with capture on, what the alternative
/// captured is dropped by `rollback`, as the rest of the copy is.
class PartialASTSavepoint
{
public:
    explicit PartialASTSavepoint(const Expected & expected)
        : state(expected.partial_ast)
    {
        if (unlikely(state))
            saved = *state;
    }

    void rollback()
    {
        if (unlikely(state))
            *state = std::move(*saved);
    }

    PartialASTSavepoint(const PartialASTSavepoint &) = delete;
    PartialASTSavepoint & operator=(const PartialASTSavepoint &) = delete;

private:
    PartialASTState * const state;
    std::optional<PartialASTState> saved;
};

}

/// The pointer is tested before any other argument is evaluated: `node` is often a call that assembles
/// the tree so far, which must not run with capture off.
#define PARTIAL_AST_SNAPSHOT(expected, node, pos, what) \
    do \
    { \
        if (unlikely((expected).partial_ast)) \
            ::DB::snapshotPartialAST(expected, node, pos, what); \
    } while (false)

#define PARTIAL_AST_FRAGMENT(expected, node, pos, what) \
    do \
    { \
        if (unlikely((expected).partial_ast)) \
            ::DB::snapshotPartialFragment(expected, node, pos, what); \
    } while (false)
