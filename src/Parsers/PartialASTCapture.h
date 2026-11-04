#pragma once

/// Capture of the statement tree built so far when a statement parser fails. The standalone
/// WebAssembly parser (`utils/wasm-parser`) reports it as `partial_ast`.
///
/// Capture is off unless the caller points `Expected::partial_ast` at a `PartialASTState`. In the server
/// the pointer is always null, and in a build without formatting it is null at compile time. Each hook then costs one branch on the pointer, taken before any other
/// argument is evaluated, so a failure path neither allocates nor copies anything for capture. A parser
/// that assembles the tree so far also builds a closure of references to its locals on each call, which is
/// a few stores and no allocation, and `ParserSelectQuery` holds a `PartialASTSelectScope`, which is one
/// more branch on entry and one on exit.
///
/// A hook goes right before an existing `return false` of a statement parser, after the statement
/// keyword has committed. It also goes where the parser recovers from a failed optional part and goes
/// on. Such a capture is reported only if the parse gets no further. A hook never assigns to `node` and
/// never moves `pos`.
///
/// The last argument names the slot that failed. For `SELECT` it is the clause, an
/// `ASTSelectQuery::Expression`. For `INSERT`, `CREATE` and `ALTER` it is the JSON key of the slot. A
/// parser of a part of a statement, such as a storage definition or an `ALTER` command, captures a
/// fragment the same way. The statement around it then reports the fragment in the slot of that part.

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParser.h>
#include <base/defines.h>

#include <algorithm>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace DB
{

/// What a parse with capture on fills in. The caller owns it and points `Expected::partial_ast` at it.
/// `tryParseQuery` clears it at the start of each parse.
struct PartialASTState
{
    /// Set by the caller: a partial tree past these limits is not captured. They are the limits that the
    /// caller holds an `ast` to.
    size_t max_depth = 0;
    size_t max_elements = 0;

    /// The captured tree, with an `Error` node in the slot that failed, and the `max_parsed_pos` it was
    /// captured at.
    ASTPtr tree;
    const char * tree_pos = nullptr;

    /// The number of `SELECT`s being parsed. Inside an `INSERT`, `outer_with` is the `WITH` written before
    /// it, and `outer_with_select_depth` is the number of `SELECT`s when the `INSERT` starts its own
    /// `SELECT`. A capture one `SELECT` deeper is a `SELECT` of the union of the `INSERT`, which `ast`
    /// gives that `WITH`.
    size_t select_depth = 0;
    ASTPtr outer_with;
    size_t outer_with_select_depth = 0;

    /// For each union being parsed, the number of its branches that have started. A parenthesized union
    /// with several branches is a node of its own in `ast`, which the `WITH` of the `INSERT` does not
    /// reach; see `capturePartialAST`. `outer_with_union_level` is the number of unions being parsed when
    /// the `INSERT` starts its own.
    std::vector<size_t> union_branches;
    size_t outer_with_union_level = 0;

    /// A part of a statement that failed: a storage definition, a dictionary, an `ALTER` command, the list
    /// of commands. It is captured like the tree, with the `max_parsed_pos` it was captured at. The capture
    /// of the node around it at that position takes it in place of an `Error`.
    ASTPtr fragment;
    const char * fragment_pos = nullptr;

    /// The last element of the last list of columns and table properties that parsed, in the order
    /// written. `ASTColumns` does not keep that order. An `Error` after the list follows this element.
    ASTPtr last_column_element;

    /// Forgets the outputs of a parse and keeps the limits.
    void clear()
    {
        tree = nullptr;
        tree_pos = nullptr;
        select_depth = 0;
        outer_with = nullptr;
        outer_with_select_depth = 0;
        union_branches.clear();
        outer_with_union_level = 0;
        fragment = nullptr;
        fragment_pos = nullptr;
        last_column_element = nullptr;
    }
};

/// The state that capture writes to, or null with capture off. A build that cannot report a partial tree,
/// the WebAssembly parser without formatting, has it null at compile time, so the compiler drops every
/// capture of the parsers, and the macros and scopes below get it through this function only.
ALWAYS_INLINE inline PartialASTState * partialASTState(const Expected & expected)
{
#if defined(CLICKHOUSE_PARSER_NO_FORMATTING)
    (void)expected;
    return nullptr;
#else
    return expected.partial_ast;
#endif
}

/// Whether a capture can be taken now, with capture on. It is false at a position that an earlier capture
/// went past, and when too little stack is left to clone a tree, which does not throw. In that case no
/// capture is taken at this position, and the parse goes on as it does with capture off. The macros call
/// it before they evaluate the node.
bool canCapturePartialAST(const Expected & expected);

/// Clones `node` into `expected.partial_ast->tree`, with an `Error` node in the slot named by the JSON
/// key `slot_key`. A null `node` captures nothing. Called only with capture on.
void capturePartialAST(Expected & expected, const ASTPtr & node, IParser::Pos pos, const char * slot_key);

/// The same for a `SELECT`, with the `Error` in the clause `slot` of the `ASTSelectQuery` `node`. For
/// `TABLES`, a list that has elements gets the `Error` as one more element.
void capturePartialAST(Expected & expected, const ASTPtr & node, IParser::Pos pos, ASTSelectQuery::Expression slot);

/// Clones a part of a statement into `expected.partial_ast->fragment`, with the `Error` in the slot named
/// by the JSON key `slot_key`. The capture of the node around it at the same position takes the fragment
/// in place of an `Error`.
void capturePartialFragment(Expected & expected, const ASTPtr & node, IParser::Pos pos, const char * slot_key);

/// The same, with the `Error` as the last element of the `ASTExpressionList` `node`.
void capturePartialListElement(Expected & expected, const ASTPtr & node, IParser::Pos pos);

/// Whether `key` is a slot of `node` that a capture may name: `ASTPartialNode` writes it only when the
/// slot is set, and a slot that failed is empty.
bool isPartialASTSlotKey(const IAST & node, std::string_view key);

/// Whether an optional part, whose parser started when `max_parsed_pos` was `max_before`, failed inside
/// itself rather than at its first token.
inline bool optionalPartFailed(const Expected & expected, const char * max_before, IParser::Pos pos)
{
    return expected.max_parsed_pos > std::max(max_before, pos->begin);
}

/// Held by `ParserSelectQuery::parseImpl`: counts the `SELECT`s being parsed, with capture on.
class PartialASTSelectScope
{
public:
    explicit PartialASTSelectScope(const Expected & expected)
        : state(partialASTState(expected))
    {
        if (unlikely(state))
            ++state->select_depth;
    }

    ~PartialASTSelectScope()
    {
        if (unlikely(state))
            --state->select_depth;
    }

    PartialASTSelectScope(const PartialASTSelectScope &) = delete;
    PartialASTSelectScope & operator=(const PartialASTSelectScope &) = delete;

private:
    PartialASTState * const state;
};

/// Held by `ParserSelectWithUnionQuery::parseImpl`: one union being parsed, with capture on.
class PartialASTUnionScope
{
public:
    explicit PartialASTUnionScope(const Expected & expected)
        : state(partialASTState(expected))
    {
        if (unlikely(state))
            state->union_branches.push_back(0);
    }

    ~PartialASTUnionScope()
    {
        if (unlikely(state))
            state->union_branches.pop_back();
    }

    PartialASTUnionScope(const PartialASTUnionScope &) = delete;
    PartialASTUnionScope & operator=(const PartialASTUnionScope &) = delete;

private:
    PartialASTState * const state;
};

/// Called where a union starts one of its branches, with capture on.
inline void countPartialASTUnionBranch(const Expected & expected)
{
    if (PartialASTState * state = partialASTState(expected); unlikely(state) && !state->union_branches.empty())
        ++state->union_branches.back();
}

/// Called where a pipe set operation takes the query before it as the first branch of the union that it
/// builds, and replaces the union being parsed by it, with capture on.
inline void setPartialASTUnionBranches(const Expected & expected, size_t count)
{
    if (PartialASTState * state = partialASTState(expected); unlikely(state) && !state->union_branches.empty())
        state->union_branches.back() = count;
}

/// Held by `ParserInsertQuery` while it parses its `SELECT`, with capture on. A capture of a `SELECT` of
/// its union gets the `WITH` written before `INSERT`, which `ast` puts there. A `SELECT` nested in
/// another `SELECT` does not.
class PartialASTOuterWith
{
public:
    PartialASTOuterWith(const Expected & expected, const ASTPtr & with)
        : state(partialASTState(expected))
    {
        if (unlikely(state))
        {
            previous_with = std::exchange(state->outer_with, with);
            previous_select_depth = std::exchange(state->outer_with_select_depth, state->select_depth);
            previous_union_level = std::exchange(state->outer_with_union_level, state->union_branches.size());
        }
    }

    ~PartialASTOuterWith()
    {
        if (unlikely(state))
        {
            state->outer_with = std::move(previous_with);
            state->outer_with_select_depth = previous_select_depth;
            state->outer_with_union_level = previous_union_level;
        }
    }

    PartialASTOuterWith(const PartialASTOuterWith &) = delete;
    PartialASTOuterWith & operator=(const PartialASTOuterWith &) = delete;

private:
    PartialASTState * const state;
    ASTPtr previous_with;
    size_t previous_select_depth = 0;
    size_t previous_union_level = 0;
};

/// Held where a parser tries an alternative on a copy of `Expected` and drops the copy if the alternative
/// fails. The copy points at the same state, so with capture on, the destructor drops what the
/// alternative captured, as it drops the rest of the copy, unless `commit` was called.
class PartialASTSavepoint
{
public:
    explicit PartialASTSavepoint(const Expected & expected)
        : state(partialASTState(expected))
    {
        if (unlikely(state))
            saved = *state;
    }

    void commit() { committed = true; }

    ~PartialASTSavepoint()
    {
        if (unlikely(state) && !committed)
            *state = std::move(*saved);
    }

    PartialASTSavepoint(const PartialASTSavepoint &) = delete;
    PartialASTSavepoint & operator=(const PartialASTSavepoint &) = delete;

private:
    PartialASTState * const state;
    std::optional<PartialASTState> saved;
    bool committed = false;
};

}

/// Each macro tests the pointer first, and then whether a capture can be taken, before it evaluates
/// `node`. `node` is often a call that assembles the tree so far, which must not run with capture off, or
/// with too little stack to clone it.
#define PARTIAL_AST_CAPTURE(expected, node, pos, slot) \
    do \
    { \
        if (unlikely(::DB::partialASTState(expected)) && ::DB::canCapturePartialAST(expected)) \
            ::DB::capturePartialAST(expected, node, pos, slot); \
    } while (false)

/// The same where the part failed only if `condition`, which is evaluated only with capture on.
#define PARTIAL_AST_CAPTURE_IF(expected, condition, node, pos, slot) \
    do \
    { \
        if (unlikely(::DB::partialASTState(expected)) && (condition) && ::DB::canCapturePartialAST(expected)) \
            ::DB::capturePartialAST(expected, node, pos, slot); \
    } while (false)

#define PARTIAL_AST_CAPTURE_FRAGMENT(expected, node, pos, slot_key) \
    do \
    { \
        if (unlikely(::DB::partialASTState(expected)) && ::DB::canCapturePartialAST(expected)) \
            ::DB::capturePartialFragment(expected, node, pos, slot_key); \
    } while (false)

#define PARTIAL_AST_CAPTURE_FRAGMENT_IF(expected, condition, node, pos, slot_key) \
    do \
    { \
        if (unlikely(::DB::partialASTState(expected)) && (condition) && ::DB::canCapturePartialAST(expected)) \
            ::DB::capturePartialFragment(expected, node, pos, slot_key); \
    } while (false)

#define PARTIAL_AST_CAPTURE_LIST_ELEMENT(expected, node, pos) \
    do \
    { \
        if (unlikely(::DB::partialASTState(expected)) && ::DB::canCapturePartialAST(expected)) \
            ::DB::capturePartialListElement(expected, node, pos); \
    } while (false)
