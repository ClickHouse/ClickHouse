#include <Parsers/PartialASTCapture.h>

#include <Parsers/ASTError.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Common/checkStackSize.h>

#include <algorithm>
#include <array>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>


namespace DB
{

namespace
{

/// The stack that cloning one level of a tree takes, with some to spare. A clone checks the stack at
/// each level and throws when it is more than half used, so a capture that would run into that is
/// not taken.
constexpr size_t CLONE_STACK_BYTES_PER_LEVEL = 512;

/// The stack that a capture needs at least, to assemble a tree and clone the top levels of it.
constexpr size_t MIN_CAPTURE_STACK_HEADROOM = 16 * 1024;

/// Whether `root` is within the limits of the caller, counted the way `IAST::checkDepth` and
/// `IAST::checkSize` count, and can be cloned with the stack that is left. Iterative, and it stops at
/// the first node past a limit, so measuring a tree of any shape is cheap and cannot throw. Cloning it
/// is recursive, and for the left-deep tree of a long operator chain, or for deeply nested subqueries,
/// it would run out of the stack. `extra`, if any, is measured as one more child of `root`.
bool fitsPartialASTLimits(const PartialASTState & state, const IAST & root, const IAST * extra)
{
    std::vector<std::pair<const IAST *, size_t>> nodes{{&root, 0}};
    if (extra)
        nodes.emplace_back(extra, 1);
    size_t size = 0;
    size_t deepest = 0;
    while (!nodes.empty())
    {
        auto [node, depth] = nodes.back();
        nodes.pop_back();
        if (depth >= state.max_depth || ++size > state.max_elements)
            return false;
        deepest = std::max(deepest, depth);
        for (const auto & child : node->children)
            nodes.emplace_back(child.get(), depth + 1);
    }
    return (deepest + 1) * CLONE_STACK_BYTES_PER_LEVEL <= getStackSizeHeadroom();
}

/// A tree that could never be an `ast` is not reported. Nothing captured before it is: that was a
/// shallower failure, and the parse got further. Nor anything around it at this position.
void blockCaptures(PartialASTState & state, const Expected & expected)
{
    state.tree = nullptr;
    state.tree_pos = expected.max_parsed_pos;
}

/// The checks every capture makes before it clones anything: false if it should not be taken. `extra`
/// is a tree that the capture adds to `node` as one more child.
bool shouldCapture(PartialASTState & state, const Expected & expected, const ASTPtr & node, bool is_select, const IAST * extra = nullptr)
{
    if (!node)
        return false;

    /// A capture that did not get as far as the one already taken is a backtracked alternative.
    if (state.tree_pos && expected.max_parsed_pos < state.tree_pos)
        return false;

    /// At the same position, the statement that captured first is the innermost one, and it is kept.
    /// For `SELECT * FROM (SELECT a FROM`, that is the subquery, not the query around it. The one
    /// exception is a `SELECT` over a capture of another statement at that position: the column list of
    /// an `INSERT`, which then retries the `(` as a subquery. A position with `tree_pos` and no `tree`
    /// is one where the innermost statement was too big, and nothing around it is taken.
    if (state.tree_pos && expected.max_parsed_pos == state.tree_pos && (!state.tree || !is_select || state.tree->as<ASTSelectQuery>()))
        return false;

    if (!fitsPartialASTLimits(state, *node, extra))
    {
        blockCaptures(state, expected);
        return false;
    }

    return true;
}

boost::intrusive_ptr<ASTError> makeError(const Expected & expected, IParser::Pos pos)
{
    auto error = make_intrusive<ASTError>();
    error->begin_pos = expected.max_parsed_pos;

    /// The token the parser stopped at, for its end. `pos` is a copy, and only tokens the parser has
    /// already looked at are visited, so `Tokens::max`, which the error message is built from, stays.
    const char * last_seen = pos.max().begin;
    while (pos->begin < error->begin_pos && pos->begin < last_seen)
        ++pos;
    error->end_pos = pos->begin == error->begin_pos ? pos->end : error->begin_pos;

    /// What is expected so far. The parse can add to it at this position later - `end of query`, for
    /// one - so `ch_parse` takes the final list instead when the error is still here.
    for (const char * variant : expected.variants)
        error->expected.emplace_back(variant);

    return error;
}

void storeCapture(PartialASTState & state, const Expected & expected, ASTPtr partial)
{
    state.tree = std::move(partial);
    state.tree_pos = expected.max_parsed_pos;
}

/// The node a fragment is a part of: the node of an `ASTPartialNode`, or the list itself.
const IAST * fragmentNode(const IAST & fragment)
{
    if (const auto * wrapper = fragment.as<ASTPartialNode>())
        return wrapper->node.get();
    return &fragment;
}

/// Whether a fragment fills the slot `slot_key` of `parent`, or, for a null key, is an element of the
/// list `parent`.
bool fillsSlot(const IAST & parent, const char * slot_key, const IAST & fragment)
{
    const IAST & node = *fragmentNode(fragment);
    const std::string_view slot = slot_key ? slot_key : "";
    if (parent.as<ASTCreateQuery>())
        return (slot == "refresh_strategy" && node.as<ASTRefreshStrategy>()) || (slot == "dictionary" && node.as<ASTDictionary>())
            || ((slot == "storage" || slot == "inner_engine") && node.as<ASTStorage>());
    if (parent.as<ASTAlterQuery>())
        return slot == "command_list" && node.as<ASTExpressionList>();
    if (parent.as<ASTExpressionList>())
        return !slot_key && node.as<ASTAlterCommand>();
    if (parent.as<ASTAlterCommand>())
        return slot == "refresh" && node.as<ASTRefreshStrategy>();
    return false;
}

/// The slots of a node that a capture may name, and how to empty each one. A slot is written only when it
/// is set, and a failed slot is empty, so the `Error` is the only value that its key gets. The
/// slot is emptied for a failure after the slot parsed, a missing `)`, which leaves it set: the `Error` takes
/// the place of what parsed there, as it does of a broken expression. A slot with no `reset` has nothing to empty.
template <typename Node>
struct Slot
{
    std::string_view key;
    void (*reset)(Node &);
};

/// `field` names a member, so it cannot be put in parentheses.
// NOLINTBEGIN(bugprone-macro-parentheses)
#define RESET_CHILD(Node, field) \
    Slot<Node> \
    { \
        #field, [](Node & node) { node.reset(node.field); } \
    }
#define RESET_STRING(Node, field) \
    Slot<Node> \
    { \
        #field, [](Node & node) { node.field.clear(); } \
    }
// NOLINTEND(bugprone-macro-parentheses)
#define KEEP(Node, key) \
    Slot<Node> { key, nullptr }

const Slot<ASTInsertQuery> INSERT_SLOTS[]
    = {KEEP(ASTInsertQuery, "table"),
       KEEP(ASTInsertQuery, "table_function"),
       KEEP(ASTInsertQuery, "partition_by"),
       KEEP(ASTInsertQuery, "infile"),
       KEEP(ASTInsertQuery, "compression"),
       RESET_CHILD(ASTInsertQuery, settings_ast),
       KEEP(ASTInsertQuery, "format"),
       KEEP(ASTInsertQuery, "columns"),
       KEEP(ASTInsertQuery, "select")};

const Slot<ASTCreateQuery> CREATE_SLOTS[]
    = {KEEP(ASTCreateQuery, "table_ast"),
       KEEP(ASTCreateQuery, "cluster"),
       KEEP(ASTCreateQuery, "columns_list"),
       KEEP(ASTCreateQuery, "storage"),
       KEEP(ASTCreateQuery, "select"),
       /// `AS remote(` parses `remote` as a table name after the table function failed.
       Slot<ASTCreateQuery>{
           "as_table_function",
           [](ASTCreateQuery & node)
           {
               node.as_database.clear();
               node.as_table.clear();
           }},
       KEEP(ASTCreateQuery, "aliases_list"),
       KEEP(ASTCreateQuery, "refresh_strategy"),
       KEEP(ASTCreateQuery, "targets"),
       KEEP(ASTCreateQuery, "sql_security"),
       KEEP(ASTCreateQuery, "comment"),
       KEEP(ASTCreateQuery, "dictionary_attributes_list"),
       KEEP(ASTCreateQuery, "dictionary"),
       KEEP(ASTCreateQuery, "as_table"),
       KEEP(ASTCreateQuery, "attach_from_path"),
       Slot<ASTCreateQuery>{"attach_as_replicated", [](ASTCreateQuery & node) { node.attach_as_replicated.reset(); }}};

const Slot<ASTAlterQuery> ALTER_SLOTS[]
    = {KEEP(ASTAlterQuery, "database_ast"),
       KEEP(ASTAlterQuery, "table_ast"),
       KEEP(ASTAlterQuery, "cluster"),
       KEEP(ASTAlterQuery, "command_list")};

const Slot<ASTAlterCommand> ALTER_COMMAND_SLOTS[]
    = {RESET_CHILD(ASTAlterCommand, col_decl),
       RESET_CHILD(ASTAlterCommand, column),
       RESET_CHILD(ASTAlterCommand, order_by),
       RESET_CHILD(ASTAlterCommand, sample_by),
       RESET_CHILD(ASTAlterCommand, index_decl),
       RESET_CHILD(ASTAlterCommand, index),
       RESET_CHILD(ASTAlterCommand, constraint_decl),
       RESET_CHILD(ASTAlterCommand, constraint),
       RESET_CHILD(ASTAlterCommand, projection_decl),
       RESET_CHILD(ASTAlterCommand, projection),
       RESET_CHILD(ASTAlterCommand, statistics_decl),
       RESET_CHILD(ASTAlterCommand, partition),
       RESET_CHILD(ASTAlterCommand, partitions),
       RESET_CHILD(ASTAlterCommand, predicate),
       RESET_CHILD(ASTAlterCommand, update_assignments),
       RESET_CHILD(ASTAlterCommand, comment),
       RESET_CHILD(ASTAlterCommand, ttl),
       RESET_CHILD(ASTAlterCommand, settings_changes),
       RESET_CHILD(ASTAlterCommand, settings_resets),
       RESET_CHILD(ASTAlterCommand, select),
       RESET_CHILD(ASTAlterCommand, sql_security),
       RESET_CHILD(ASTAlterCommand, rename_to),
       RESET_CHILD(ASTAlterCommand, refresh),
       RESET_CHILD(ASTAlterCommand, snapshot_desc),
       RESET_CHILD(ASTAlterCommand, execute_args),
       RESET_CHILD(ASTAlterCommand, add_enum_values),
       RESET_STRING(ASTAlterCommand, move_destination_name),
       RESET_STRING(ASTAlterCommand, from),
       RESET_STRING(ASTAlterCommand, with_name),
       RESET_STRING(ASTAlterCommand, from_table),
       RESET_STRING(ASTAlterCommand, to_table),
       RESET_STRING(ASTAlterCommand, snapshot_name),
       RESET_STRING(ASTAlterCommand, execute_command_name),
       RESET_STRING(ASTAlterCommand, remove_property)};

const Slot<ASTStorage> STORAGE_SLOTS[]
    = {RESET_CHILD(ASTStorage, engine),
       RESET_CHILD(ASTStorage, partition_by),
       RESET_CHILD(ASTStorage, primary_key),
       RESET_CHILD(ASTStorage, order_by),
       RESET_CHILD(ASTStorage, unique_key),
       RESET_CHILD(ASTStorage, sample_by),
       RESET_CHILD(ASTStorage, ttl_table),
       RESET_CHILD(ASTStorage, settings)};

const Slot<ASTDictionary> DICTIONARY_SLOTS[]
    = {RESET_CHILD(ASTDictionary, primary_key),
       RESET_CHILD(ASTDictionary, source),
       RESET_CHILD(ASTDictionary, lifetime),
       RESET_CHILD(ASTDictionary, layout),
       RESET_CHILD(ASTDictionary, range),
       RESET_CHILD(ASTDictionary, dict_settings)};

const Slot<ASTRefreshStrategy> REFRESH_STRATEGY_SLOTS[]
    = {KEEP(ASTRefreshStrategy, "period"),
       KEEP(ASTRefreshStrategy, "offset"),
       KEEP(ASTRefreshStrategy, "spread"),
       KEEP(ASTRefreshStrategy, "dependencies"),
       KEEP(ASTRefreshStrategy, "settings")};

#undef RESET_CHILD
#undef RESET_STRING
#undef KEEP

/// Calls `f(typed_node, slots)` with the slots of the type of `node`; false if the type has none.
template <typename Ast, typename F>
bool withSlots(Ast & node, F && f)
{
    if (auto * insert = node.template as<ASTInsertQuery>())
        f(*insert, INSERT_SLOTS);
    else if (auto * create = node.template as<ASTCreateQuery>())
        f(*create, CREATE_SLOTS);
    else if (auto * alter = node.template as<ASTAlterQuery>())
        f(*alter, ALTER_SLOTS);
    else if (auto * command = node.template as<ASTAlterCommand>())
        f(*command, ALTER_COMMAND_SLOTS);
    else if (auto * storage = node.template as<ASTStorage>())
        f(*storage, STORAGE_SLOTS);
    else if (auto * dictionary = node.template as<ASTDictionary>())
        f(*dictionary, DICTIONARY_SLOTS);
    else if (auto * refresh = node.template as<ASTRefreshStrategy>())
        f(*refresh, REFRESH_STRATEGY_SLOTS);
    else
        return false;
    return true;
}

void resetSlot(IAST & node, std::string_view key)
{
    withSlots(
        node,
        [&](auto & typed, const auto & slots)
        {
            for (const auto & slot : slots)
                if (slot.key == key && slot.reset)
                    slot.reset(typed);
        });
}

/// What goes into the slot that failed: the fragment captured inside it at this position, if any, and
/// otherwise null. A fragment is taken once.
ASTPtr takeFragment(PartialASTState & state, const Expected & expected, const IAST & parent, const char * slot_key)
{
    if (state.fragment && state.fragment_pos == expected.max_parsed_pos && fillsSlot(parent, slot_key, *state.fragment))
        return std::exchange(state.fragment, nullptr);
    return nullptr;
}

ASTPtr wrapInPartialNode(ASTPtr node, ASTPtr failed_value, const char * failed_key)
{
    auto wrapper = make_intrusive<ASTPartialNode>();
    wrapper->node = std::move(node);
    wrapper->failed_value = std::move(failed_value);
    wrapper->failed_key = failed_key;
    wrapper->children = {wrapper->node, wrapper->failed_value};
    return wrapper;
}

enum class ColumnListKind : size_t
{
    Columns,
    Indices,
    Constraints,
    Projections,
    Codecs,
};

std::array<ASTExpressionList *, 5> columnLists(const ASTColumns & list)
{
    return {list.columns, list.indices, list.constraints, list.projections, list.codec_declarations};
}

/// Whether the list of a `CREATE TABLE` holds columns only, so that what failed after it is the
/// element after the last column. With codec declarations, indices, constraints, projections or a primary key in it, the
/// order of the elements is not in the tree.
bool hasOnlyColumns(const ASTColumns & list)
{
    return list.columns && !list.codec_declarations && !list.indices && !list.constraints && !list.projections && !list.primary_key
        && !list.primary_key_from_columns;
}

/// A `PRIMARY KEY` declared in the column list of a `CREATE` goes into the storage definition, where
/// the success path puts it: in a materialized view, the inner engine of its `To` target. Not where
/// the success path rejects the query instead (a plain view, `TO [db.]table`, a table function, two
/// primary keys), nor where the storage definition is the `Error` itself or failed in its own
/// `PRIMARY KEY`.
void movePrimaryKeyToStorage(ASTCreateQuery & create, const IAST & inner, std::string_view key)
{
    if (!create.columns_list)
        return;
    auto & columns = *create.columns_list;
    if (!columns.primary_key == !columns.primary_key_from_columns)
        return;
    if (create.is_ordinary_view || create.as_table_function)
        return;

    ASTStorage * storage = nullptr;
    if (key == "storage" || key == "inner_engine")
    {
        const auto * wrapper = inner.as<ASTPartialNode>();
        if (!wrapper || std::string_view(wrapper->failed_key) == "primary_key")
            return;
        storage = wrapper->node->as<ASTStorage>();
    }
    else if (create.is_materialized_view)
    {
        if (create.targets && create.targets->tryGetTarget(ViewTarget::To) && !create.targets->getTableID(ViewTarget::To).empty())
            return;
        if (create.targets && create.targets->getTableASTWithQueryParams(ViewTarget::To))
            return;
        if (!create.targets)
            create.set(create.targets, make_intrusive<ASTViewTargets>());
        if (!create.targets->getInnerEngine(ViewTarget::To))
            create.targets->setInnerEngine(ViewTarget::To, make_intrusive<ASTStorage>());
        storage = create.targets->getInnerEngine(ViewTarget::To)->as<ASTStorage>();
    }
    else
    {
        if (!create.storage)
            create.set(create.storage, make_intrusive<ASTStorage>());
        storage = create.storage;
    }

    if (!storage || storage->primary_key)
        return;
    if (columns.primary_key)
    {
        storage->set(storage->primary_key, columns.primary_key->ptr());
        columns.reset(columns.primary_key);
    }
    else
    {
        storage->set(storage->primary_key, columns.primary_key_from_columns->ptr());
        columns.reset(columns.primary_key_from_columns);
    }
    storage->normalizeChildrenOrder();
}

}

bool canCapturePartialAST(const Expected & expected)
{
    chassert(expected.partial_ast);
    PartialASTState & state = *expected.partial_ast;

    if (state.tree_pos && expected.max_parsed_pos < state.tree_pos)
        return false;

    if (unlikely(getStackSizeHeadroom() < MIN_CAPTURE_STACK_HEADROOM))
    {
        /// Nothing is cloned here, and nothing around it at this position is taken. A capture already
        /// taken at this position is kept.
        if (!state.tree_pos || expected.max_parsed_pos > state.tree_pos)
            blockCaptures(state, expected);
        return false;
    }

    return true;
}

bool isPartialASTSlotKey(const IAST & node, std::string_view key)
{
    bool found = false;
    withSlots(
        node,
        [&](const auto &, const auto & slots)
        { found = std::any_of(std::begin(slots), std::end(slots), [&](const auto & slot) { return slot.key == key; }); });
    return found;
}

void capturePartialAST(Expected & expected, const ASTPtr & node, IParser::Pos pos, ASTSelectQuery::Expression slot)
{
    chassert(expected.partial_ast);
    PartialASTState & state = *expected.partial_ast;

    /// A `SELECT` of the union of an `INSERT` - not nested in another `SELECT` of it - gets the `WITH`
    /// written before the `INSERT`, where `ast` has it, unless it has a `WITH` of its own.
    const IAST * outer_with = nullptr;
    if (node && state.outer_with && state.select_depth == state.outer_with_select_depth + 1 && !node->as<ASTSelectQuery &>().with())
        outer_with = state.outer_with.get();

    if (!shouldCapture(state, expected, node, /*is_select=*/true, outer_with))
        return;

    auto error = makeError(expected, pos);
    ASTPtr partial = node->clone();
    auto & select = partial->as<ASTSelectQuery &>();

    /// A list of tables that has elements, and failed after them, gets the `Error` as one more element.
    if (slot == ASTSelectQuery::Expression::TABLES && select.tables())
        select.tables()->children.push_back(error);
    else
        select.setExpression(slot, ASTPtr(error));

    if (outer_with)
    {
        select.setExpression(ASTSelectQuery::Expression::WITH, outer_with->clone());
        select.normalizeChildrenOrder();
    }
    storeCapture(state, expected, std::move(partial));
}

void capturePartialAST(Expected & expected, const ASTPtr & node, IParser::Pos pos, const char * slot_key)
{
    chassert(expected.partial_ast);
    PartialASTState & state = *expected.partial_ast;

    if (!node)
        return;

    ASTPtr fragment = takeFragment(state, expected, *node, slot_key);
    if (!shouldCapture(state, expected, node, /*is_select=*/false, fragment.get()))
        return;

    const std::string_view key = slot_key;

    /// Where in the list of a `CREATE` an `Error` after it goes: after the element written last, whose
    /// list is found in the tree the parser built, as the order of the lists is not in it.
    std::optional<ColumnListKind> column_list_kind;
    if (const auto * create = node->as<ASTCreateQuery>(); create && key == "columns_list" && create->columns_list)
    {
        const auto & list = *create->columns_list;
        if (hasOnlyColumns(list))
        {
            column_list_kind = ColumnListKind::Columns;
        }
        else
        {
            const auto lists = columnLists(list);
            for (size_t kind = 0; kind < lists.size(); ++kind)
                if (lists[kind] && !lists[kind]->children.empty() && lists[kind]->children.back().get() == state.last_column_element.get())
                    column_list_kind = static_cast<ColumnListKind>(kind);
        }
    }

    ASTPtr failed_value = fragment ? fragment : makeError(expected, pos);
    ASTPtr partial = node->clone();
    auto * insert = partial->as<ASTInsertQuery>();
    auto * create = partial->as<ASTCreateQuery>();
    bool placed = false;
    if (insert && key == "columns" && insert->columns && insert->columns->as<ASTExpressionList>())
    {
        /// The list parsed and its `)` is missing: what failed is the element after the last one.
        insert->columns->children.push_back(failed_value);
        placed = true;
    }
    else if (create && key == "aliases_list" && create->aliases_list)
    {
        create->aliases_list->children.push_back(failed_value);
        placed = true;
    }
    else if (create && key == "dictionary_attributes_list" && create->dictionary_attributes_list)
    {
        create->dictionary_attributes_list->children.push_back(failed_value);
        placed = true;
    }
    else if (create && key == "columns_list" && create->columns_list)
    {
        /// Likewise for `CREATE TABLE t (a UInt8,`. Without the element written last - a `PRIMARY KEY`
        /// or a foreign key, which the list does not keep - the whole list is what failed.
        if (column_list_kind)
        {
            columnLists(*create->columns_list)[static_cast<size_t>(*column_list_kind)]->children.push_back(failed_value);
            placed = true;
        }
        else
        {
            create->reset(create->columns_list);
        }
    }
    else if (create && key == "inner_engine")
    {
        /// The storage of a materialized view, which `ast` has as the inner engine of its `To` target.
        if (!create->targets)
            create->set(create->targets, make_intrusive<ASTViewTargets>());
        create->targets->setInnerEngine(ViewTarget::To, failed_value);
        placed = true;
    }

    if (create)
    {
        movePrimaryKeyToStorage(*create, *failed_value, key);

        /// The success path marks a `TimeSeries` table as soon as its storage parses; a storage that
        /// failed after its engine is that table as well.
        if (const auto * wrapper = failed_value->as<ASTPartialNode>(); wrapper && key == "storage")
            if (const auto * storage = wrapper->node->as<ASTStorage>(); storage && storage->engine && storage->engine->name == "TimeSeries")
                create->is_time_series_table = true;
    }

    if (placed)
    {
        storeCapture(state, expected, std::move(partial));
        return;
    }

    chassert(isPartialASTSlotKey(*partial, key));
    resetSlot(*partial, key);
    storeCapture(state, expected, wrapInPartialNode(std::move(partial), std::move(failed_value), slot_key));
}

namespace
{

/// `slot_key` is null for an element of the list `node`.
void capturePartialFragmentImpl(Expected & expected, const ASTPtr & node, IParser::Pos pos, const char * slot_key)
{
    chassert(expected.partial_ast);
    PartialASTState & state = *expected.partial_ast;

    if (!node)
        return;

    /// Nothing around a statement captured at this position or further is taken, so no part of it is.
    if (state.tree_pos && expected.max_parsed_pos <= state.tree_pos)
        return;

    ASTPtr inner = takeFragment(state, expected, *node, slot_key);

    /// An empty list with nothing in it that captured is not a part: the slot of the list is what failed.
    if (!slot_key && !inner && node->children.empty())
        return;

    if (!fitsPartialASTLimits(state, *node, inner.get()))
    {
        blockCaptures(state, expected);
        return;
    }

    if (!inner)
        inner = makeError(expected, pos);
    ASTPtr partial = node->clone();
    auto * dictionary = partial->as<ASTDictionary>();
    if (!slot_key)
    {
        partial->children.push_back(std::move(inner));
    }
    else if (dictionary && std::string_view(slot_key) == "primary_key" && dictionary->primary_key)
    {
        /// `PRIMARY KEY (a, b` lacks its `)`: what failed is the element after the last one.
        dictionary->primary_key->children.push_back(std::move(inner));
    }
    else
    {
        chassert(isPartialASTSlotKey(*partial, slot_key));
        resetSlot(*partial, slot_key);
        partial = wrapInPartialNode(std::move(partial), std::move(inner), slot_key);
    }

    state.fragment = std::move(partial);
    state.fragment_pos = expected.max_parsed_pos;
}

}

void capturePartialFragment(Expected & expected, const ASTPtr & node, IParser::Pos pos, const char * slot_key)
{
    chassert(slot_key);
    capturePartialFragmentImpl(expected, node, pos, slot_key);
}

void capturePartialListElement(Expected & expected, const ASTPtr & node, IParser::Pos pos)
{
    capturePartialFragmentImpl(expected, node, pos, nullptr);
}

}
