#include <Interpreters/MutationPredicateColumnsAccess.h>

#include <Access/Common/AccessRightsElement.h>
#include <Core/Names.h>
#include <Dictionaries/IDictionary.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageJoin.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace
{

/// The identifiers on the right of an `IN` in a mutation expression, split by whether they are
/// compound. `RequiredSourceColumnsVisitor` reports such a name as a column, but a one-part name
/// there is always a table: the mutation expression is qualified with a database before it is
/// stored, and `WHERE 1 IN arr` becomes `1 IN (db.arr)`, which reads a table and never the array
/// column `arr` - only a qualified `WHERE 1 IN t.arr` reads the column.
/// Subqueries are not descended into, matching the visitor, whose required columns are the ones
/// being resolved here.
struct InRightHandSideIdentifiers
{
    NameSet bare;
    NameSet compound;
};

void collectInRightHandSideIdentifiers(const IAST & ast, InRightHandSideIdentifiers & names)
{
    if (ast.as<ASTSubquery>() || ast.as<ASTSelectQuery>() || ast.as<ASTSelectWithUnionQuery>())
        return;

    if (const auto * function = ast.as<ASTFunction>();
        function && function->arguments && functionIsInOrGlobalInOperator(function->name)
        && function->arguments->children.size() == 2)
    {
        if (const auto * identifier = function->arguments->children[1]->as<ASTIdentifier>())
            (identifier->compound() ? names.compound : names.bare).insert(identifier->name());
    }

    for (const auto & child : ast.children)
        if (child)
            collectInRightHandSideIdentifiers(*child, names);
}

InRightHandSideIdentifiers collectInRightHandSideIdentifiers(const ASTs & expressions)
{
    InRightHandSideIdentifiers names;
    for (const auto & expression : expressions)
        if (expression)
            collectInRightHandSideIdentifiers(*expression, names);
    return names;
}

}

void addExpressionColumnsSelectAccess(
    AccessRightsElements & required_access,
    const IAST * expression,
    const String & database,
    const String & table,
    const StorageInMemoryMetadata & metadata)
{
    if (!expression)
        return;

    RequiredSourceColumnsVisitor::Data columns_context;
    auto expression_clone = expression->clone();
    RequiredSourceColumnsVisitor(columns_context).visit(expression_clone);

    InRightHandSideIdentifiers in_right_hand_side_names;
    collectInRightHandSideIdentifiers(*expression, in_right_hand_side_names);

    Strings columns;
    const String db_table_prefix = database.empty() ? String{} : database + "." + table + ".";
    const String table_prefix = table + ".";
    for (const auto & name : columns_context.requiredColumns())
    {
        /// A one-part name on the right of `IN` is a table, even when the mutated table has a column
        /// of that name; `addExpressionIndirectReadsAccess` requires `SELECT` on that table instead.
        if (in_right_hand_side_names.bare.contains(name))
            continue;

        /// A real column (including a real dotted/quoted name like `t.id`) requires SELECT as-is.
        if (metadata.columns.has(name))
        {
            columns.emplace_back(name);
            continue;
        }

        /// A virtual column not shadowed by a real one needs no SELECT grant, as in a plain SELECT.
        if (metadata.isVirtualColumn(name))
            continue;

        /// Otherwise strip a `table.` / `db.table.` qualifier and resolve the bare name the same way.
        std::string_view bare = name;
        if (!db_table_prefix.empty() && bare.starts_with(db_table_prefix))
            bare.remove_prefix(db_table_prefix.size());
        else if (bare.starts_with(table_prefix))
            bare.remove_prefix(table_prefix.size());

        if (metadata.isVirtualColumn(String(bare)))
            continue;

        /// A qualified name on the right of `IN` that is not a column of this table names a table
        /// or a set, and `addExpressionIndirectReadsAccess` requires `SELECT` on it instead.
        if (!metadata.columns.has(String(bare)) && in_right_hand_side_names.compound.contains(name))
            continue;

        columns.emplace_back(bare);
    }

    if (!columns.empty())
        required_access.emplace_back(AccessType::SELECT, database, table, columns);
}

namespace
{

bool isAsterisk(const IAST & ast)
{
    return ast.as<ASTAsterisk>() || ast.as<ASTQualifiedAsterisk>()
        || ast.as<ASTColumnsListMatcher>() || ast.as<ASTColumnsRegexpMatcher>();
}

/// An asterisk selects every column, so the columns read cannot be enumerated from the AST. A
/// nested subquery carries its own select list, whose asterisks say nothing about this level.
bool selectsEverything(const IAST & ast)
{
    if (isAsterisk(ast))
        return true;
    if (ast.as<ASTSubquery>())
        return false;

    for (const auto & child : ast.children)
        if (child && selectsEverything(*child))
            return true;

    return false;
}

/// A scalar `WITH <expression> AS name` of a mutation subquery, with its value when that value is a
/// string literal - which is what `dictGet` and `joinGet` may name their object by.
struct WithScalar
{
    String name;
    std::optional<String> string_value;
};

/// Walks a mutation expression and collects, by name, the reads it performs through a subquery, a
/// table on the right of `IN`, `dictGet` or `joinGet`. See `addExpressionIndirectReadsAccess`.
class IndirectReadsCollector
{
public:
    IndirectReadsCollector(
        AccessRightsElements & required_access_,
        ContextPtr context_,
        const String & mutated_database_,
        const String & mutated_table_,
        const StorageInMemoryMetadata * mutated_metadata_)
        : required_access(required_access_)
        , context(std::move(context_))
        , mutated_database(mutated_database_)
        , mutated_table(mutated_table_)
        , mutated_metadata(mutated_metadata_)
    {
    }

    void visitExpression(const IAST * ast)
    {
        if (!ast)
            return;

        if (const auto * subquery = ast->as<ASTSubquery>())
        {
            visitSelectOrUnion(*subquery);
            return;
        }

        if (const auto * function = ast->as<ASTFunction>())
            visitNamedReads(*function);

        for (const auto & child : ast->children)
            visitExpression(child.get());
    }

private:
    /// The table an `IN`, `dictGet` or `joinGet` names in its arguments instead of reading it as a
    /// column. The column set of such a read is not enumerable from the AST except for `joinGet`,
    /// which names the one column it reads.
    void visitNamedReads(const ASTFunction & function)
    {
        if (!function.arguments)
            return;
        const auto & arguments = function.arguments->children;

        if (functionIsInOrGlobalInOperator(function.name) && arguments.size() == 2)
        {
            /// `x IN other` reads `other`; `x IN (SELECT ...)` and `x IN (1, 2)` do not name a table.
            if (const auto * identifier = arguments[1]->as<ASTIdentifier>(); identifier && namesATable(*identifier))
            {
                if (auto table_id = tryGetNamedTable(*identifier); table_id && !needsNoGrant(*table_id))
                    required_access.emplace_back(
                        AccessType::SELECT, databaseOfTable(*table_id), table_id->table_name);
            }
        }
        else if (functionIsJoinGet(function.name) && arguments.size() >= 2)
        {
            /// `joinGet('db.join_tbl', 'column', ...)` reads that column and the key columns it
            /// probes, exactly as `FunctionJoinGet::prepare` checks when the function is built -
            /// which for a mutation happens in the background, with full access.
            bool unknown_object = false;
            auto table_id = tryGetFunctionObject(*arguments[0], unknown_object);
            if (unknown_object)
            {
                required_access.emplace_back(AccessType::SELECT);
            }
            else if (table_id)
            {
                std::optional<Strings> columns;
                if (const auto * column = arguments[1]->as<ASTLiteral>();
                    column && column->value.getType() == Field::Types::String)
                    columns = tryGetJoinGetColumns(*table_id, column->value.safeGet<String>());

                if (columns)
                    required_access.emplace_back(
                        AccessType::SELECT, databaseOfTable(*table_id), table_id->table_name, *columns);
                else
                    required_access.emplace_back(
                        AccessType::SELECT, databaseOfTable(*table_id), table_id->table_name);
            }
        }
        else if (functionIsDictGet(function.name) && !arguments.empty())
        {
            bool unknown_object = false;
            auto dictionary_id = tryGetFunctionObject(*arguments[0], unknown_object);
            if (unknown_object)
                required_access.emplace_back(AccessType::dictGet);
            else if (dictionary_id)
                required_access.emplace_back(dictionaryAccess(*dictionary_id));
        }
    }

    /// A table named by an identifier (`x IN other`, `dictGet(db.dict, ...)`) or by a string
    /// literal (`joinGet('db.tbl', ...)`, `dictGet('db.dict', ...)`).
    static std::optional<StorageID> tryGetNamedTable(const IAST & argument)
    {
        if (const auto * identifier = argument.as<ASTIdentifier>())
        {
            /// Handles both a bare name and a compound `db.name`, and `ASTTableIdentifier` with it.
            DatabaseAndTableWithAlias database_and_table(*identifier);
            if (database_and_table.table.empty())
                return {};
            return StorageID{database_and_table.database, database_and_table.table};
        }

        const auto * literal = argument.as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::String)
            return {};

        return tryGetNamedTable(literal->value.safeGet<String>());
    }

    /// A table named by a string, as `joinGet` and `dictGet` name theirs: `tbl` or `db.tbl`.
    static std::optional<StorageID> tryGetNamedTable(const String & name)
    {
        if (name.empty())
            return {};

        const auto dot = name.find('.');
        if (dot == String::npos)
            return StorageID{"", name};
        return StorageID{name.substr(0, dot), name.substr(dot + 1)};
    }

    /// The dictionary or `Join` table a `dictGet` / `joinGet` first argument names. The runtime
    /// takes that name from any constant `String` expression (`FunctionDictHelper::getDictionary`,
    /// `getJoin`), and the analyzer resolves the argument as an expression identifier first
    /// (`resolveFunction.cpp`), so a one-part identifier may be an alias standing for the name, as
    /// in `WITH 'dict' AS d SELECT dictGet(d, ...)`. Only two shapes name one object provably: a
    /// string literal, and an identifier that is not an in-scope alias. An alias is followed when
    /// it holds a string literal; anything else - an alias holding another expression, `concat('db',
    /// '.dict')`, a column reference - leaves the object unknown, which is reported in
    /// `unknown_object` so that the caller requires the access on every object instead of skipping
    /// the read. Skipping it is what would let a mutation with `validate_mutation_query = 0` read
    /// the object in the background, under full access.
    std::optional<StorageID> tryGetFunctionObject(const IAST & argument, bool & unknown_object) const
    {
        unknown_object = false;

        if (const auto * literal = argument.as<ASTLiteral>())
        {
            if (literal->value.getType() != Field::Types::String)
            {
                unknown_object = true;
                return {};
            }
            return tryGetNamedTable(literal->value.safeGet<String>());
        }

        const auto * identifier = argument.as<ASTIdentifier>();
        if (!identifier)
        {
            unknown_object = true;
            return {};
        }

        if (!identifier->compound())
        {
            if (const auto * alias_value = findAliasValue(identifier->name()))
            {
                if (!*alias_value)
                {
                    unknown_object = true;
                    return {};
                }
                return tryGetNamedTable(**alias_value);
            }
        }

        return tryGetNamedTable(argument);
    }

    /// The columns `joinGet` reads from a `Join` table: the attribute it names and the key columns
    /// it probes (`FunctionJoinGet::prepare`). Nothing when the table cannot be resolved here -
    /// then the requirement falls back to the whole table, which is a superset of what it reads.
    std::optional<Strings> tryGetJoinGetColumns(const StorageID & table_id, const String & attribute) const
    {
        StorageID resolved{databaseOfTable(table_id), table_id.table_name};
        if (resolved.database_name.empty())
            return {};

        const auto storage_join = std::dynamic_pointer_cast<StorageJoin>(
            DatabaseCatalog::instance().tryGetTable(resolved, context));
        if (!storage_join)
            return {};

        Strings columns = storage_join->getKeyNames();
        columns.push_back(attribute);
        return columns;
    }

    /// The value an in-scope alias of this name stands for: an expression alias of the `SELECT`
    /// level being visited, or, failing that, the innermost scalar `WITH <expression> AS name`.
    /// Nothing when the name is not an alias at all; an alias whose value is not a string literal
    /// is an empty `optional`, the value it stands for being unknown here.
    const std::optional<String> * findAliasValue(const String & name) const
    {
        if (const auto it = expression_aliases.find(name); it != expression_aliases.end())
            return &it->second;

        for (auto it = with_scalars.rbegin(); it != with_scalars.rend(); ++it)
            if (it->name == name)
                return &it->string_value;

        return nullptr;
    }

    void visitSelectOrUnion(const IAST & ast)
    {
        const bool prev_inside_subquery = inside_subquery;
        inside_subquery = true;
        visitSelectOrUnionImpl(ast);
        inside_subquery = prev_inside_subquery;
    }

    void visitSelectOrUnionImpl(const IAST & ast)
    {
        if (const auto * select = ast.as<ASTSelectQuery>())
        {
            visitSelect(*select);
            return;
        }

        /// `ASTSubquery` and `ASTSelectWithUnionQuery` hold the selects among their children; a
        /// union contributes every branch.
        for (const auto & child : ast.children)
            if (child)
                visitSelectOrUnion(*child);
    }

    void visitSelect(const ASTSelectQuery & select)
    {
        /// A `WITH` name is visible in the `SELECT` that defines it and in the subqueries below it,
        /// and nowhere else: once this level is done its names go out of scope again, so that a
        /// later `id IN s` is not taken for this level's `s`.
        const size_t enclosing_cte_names = cte_names.size();
        const size_t enclosing_with_scalars = with_scalars.size();
        SCOPE_EXIT({
            cte_names.resize(enclosing_cte_names);
            with_scalars.resize(enclosing_with_scalars);
        });

        /// An expression alias is visible only at the `SELECT` level that defines it - not in a
        /// nested one, and not in the enclosing one - so this level's aliases replace the enclosing
        /// level's for as long as it is being visited, the same scoping `AddDefaultDatabaseVisitor`
        /// applies when it decides whether to qualify a name on the right of `IN`.
        auto enclosing_expression_aliases = std::move(expression_aliases);
        expression_aliases.clear();
        for (const auto & child : select.children)
            collectExpressionAliases(child);
        SCOPE_EXIT({ expression_aliases = std::move(enclosing_expression_aliases); });

        if (const auto with = select.with())
        {
            for (const auto & child : with->children)
            {
                if (const auto * with_element = child->as<ASTWithElement>())
                {
                    /// A `WITH` name is not a table to grant on, but its body reads tables.
                    cte_names.push_back(with_element->name);
                    if (with_element->subquery)
                        visitSelectOrUnion(*with_element->subquery);
                }
                else
                {
                    /// A scalar `WITH <expression> AS name`. `dictGet` and `joinGet` accept such a
                    /// name for their object and the analyzer resolves it to its value first, so
                    /// the alias is remembered with the value when that value is a string literal,
                    /// and without one otherwise - see `tryGetFunctionObject`.
                    if (const String alias = child->tryGetAlias(); !alias.empty())
                    {
                        const auto * literal = child->as<ASTLiteral>();
                        if (literal && literal->value.getType() == Field::Types::String)
                            with_scalars.emplace_back(alias, literal->value.safeGet<String>());
                        else
                            with_scalars.emplace_back(alias, std::nullopt);
                    }
                    visitExpression(child.get());
                }
            }
        }

        std::vector<StorageID> tables;
        Strings aliases;
        /// Whether a column reference at this level can be attributed to one table.
        bool attributable = true;
        /// Whether every table expression at this level is an ordinary named table whose columns can
        /// be looked up; see `visibleColumns`.
        bool all_tables_named = true;

        for (const auto * table_expression : getTableExpressions(select))
        {
            if (table_expression->subquery)
            {
                visitSelectOrUnion(*table_expression->subquery);
                attributable = false;
                all_tables_named = false;
            }
            else if (table_expression->table_function)
            {
                /// Not covered - walk the arguments for nested subqueries at least.
                visitExpression(table_expression->table_function.get());
                attributable = false;
                all_tables_named = false;
            }
            else if (const auto & name = table_expression->database_and_table_name)
            {
                const auto * identifier = name->as<ASTTableIdentifier>();
                if (!identifier)
                {
                    attributable = false;
                    all_tables_named = false;
                    continue;
                }

                auto table_id = identifier->getTableId();
                if (needsNoGrant(table_id))
                {
                    /// A `WITH` element or a temporary table needs no grant, but its columns are not
                    /// known from the catalog either.
                    all_tables_named = false;
                    continue;
                }

                aliases.push_back(identifier->tryGetAlias());
                tables.push_back(std::move(table_id));
            }
        }

        ASTs expressions{
            select.select(), select.where(), select.prewhere(), select.having(), select.qualify(),
            select.groupBy(), select.orderBy(), select.limitBy()};

        /// A `JOIN ... ON` / `USING` condition and an `ARRAY JOIN` list read columns and can hold
        /// named reads of their own, and belong to none of the clauses above.
        if (const auto tables_in_select = select.tables())
        {
            for (const auto & child : tables_in_select->children)
            {
                const auto * element = child->as<ASTTablesInSelectQueryElement>();
                if (!element)
                    continue;

                if (const auto * table_join = element->table_join ? element->table_join->as<ASTTableJoin>() : nullptr)
                {
                    expressions.push_back(table_join->on_expression);
                    expressions.push_back(table_join->using_expression_list);
                }

                if (const auto * array_join = element->array_join ? element->array_join->as<ASTArrayJoin>() : nullptr)
                    expressions.push_back(array_join->expression_list);
            }
        }

        /// The names this level's tables can be referred to by, so that a name on the right of an
        /// `IN` below the top level can be told apart from a table; see `namesATable`.
        subquery_levels.push_back(visibleColumns(tables, aliases, all_tables_named));
        SCOPE_EXIT({ subquery_levels.pop_back(); });

        /// Nested subqueries and named reads inside this level's expressions are reads of their own.
        for (const auto & expression : expressions)
            visitExpression(expression.get());

        if (tables.empty())
            return;

        if (attributable && tables.size() == 1)
        {
            if (auto columns = tryAttributeColumns(expressions, tables.front(), aliases.front()))
            {
                required_access.emplace_back(
                    AccessType::SELECT, databaseOfTable(tables.front()), tables.front().table_name, *columns);
                return;
            }
        }

        /// Fall back to the whole table, a superset of any column set it may read.
        for (const auto & table_id : tables)
            required_access.emplace_back(AccessType::SELECT, databaseOfTable(table_id), table_id.table_name);
    }

    /// The columns this level reads from its single table, or nothing when they cannot all be
    /// attributed to it.
    std::optional<Strings> tryAttributeColumns(
        const ASTs & expressions, const StorageID & table_id, const String & alias) const
    {
        RequiredSourceColumnsVisitor::Data columns_context;
        for (const auto & expression : expressions)
        {
            if (!expression)
                continue;
            if (selectsEverything(*expression))
                return {};

            auto expression_clone = expression->clone();
            RequiredSourceColumnsVisitor(columns_context).visit(expression_clone);
        }

        /// A name on the right of `IN` that is a table and not a column of this level is required as
        /// a table by `visitNamedReads`; requiring it as a column of this level too would ask for a
        /// grant no one can give. `namesATable` decides the two apart the same way.
        const auto in_right_hand_side_names = collectInRightHandSideIdentifiers(expressions);
        /// `visitSelect` pushes this level before it calls this, so the stack is never empty here.
        const NameSet & visible = subquery_levels.back();

        /// The metadata of this level's table, when it can be looked up: a virtual column of it
        /// needs no `SELECT` grant, exactly as in a plain `SELECT` from it, and requiring one on a
        /// name like `_part` would ask for a grant that cannot be given.
        const auto metadata = tryGetMetadata(table_id);

        /// The qualifications a reference to this table may carry.
        Strings prefixes;
        if (!alias.empty())
            prefixes.emplace_back(alias + ".");
        prefixes.emplace_back(table_id.table_name + ".");
        if (!table_id.database_name.empty())
            prefixes.emplace_back(table_id.database_name + "." + table_id.table_name + ".");

        Strings columns;
        for (const auto & name : columns_context.requiredColumns())
        {
            if (in_right_hand_side_names.bare.contains(name))
                continue;
            if (in_right_hand_side_names.compound.contains(name) && !visible.contains(name))
                continue;

            std::string_view bare = name;
            for (const auto & prefix : prefixes)
            {
                if (bare.starts_with(prefix))
                {
                    bare.remove_prefix(prefix.size());
                    break;
                }
            }

            /// A name that is still dotted is either a column of a table this level does not name,
            /// or a real dotted column name - without the table's metadata the two are
            /// indistinguishable here, so stop attributing.
            if (bare.contains('.'))
                return {};

            if (metadata && !metadata->columns.has(String(bare)) && metadata->isVirtualColumn(String(bare)))
                continue;

            columns.emplace_back(bare);
        }

        if (columns.empty())
            return {};

        return columns;
    }

    /// Whether an identifier on the right of `IN` names a table (or a set) rather than reading an
    /// array-valued column: the two are the same identifier as far as the AST is concerned.
    ///
    /// A one-part name is a table here unless the qualifier leaves it alone. The mutation expression
    /// is qualified with a database before it is stored, so `WHERE 1 IN arr` becomes `1 IN (db.arr)`
    /// and reads a table `db.arr` on every entry point, even when `arr` is an array column of the
    /// mutated table - the column is only read by a qualified `WHERE 1 IN t.arr`. A `WITH` name is
    /// not a table to grant on, and a temporary table needs no grant, exactly as in a plain `SELECT`.
    /// Neither is an expression alias of the `SELECT` level the name appears at: `AddDefaultDatabaseVisitor`
    /// keeps such a name as written instead of making a table identifier of it, so the mutation reads
    /// a column and asking for `SELECT` on a table of that name would deny a mutation the grants allow.
    ///
    /// A qualified name is a column when it resolves to one: against the mutated table's columns at
    /// the top level of the mutation expression, and against the columns of the enclosing subquery's
    /// tables below it (see `visibleColumns`). Where those columns are unknown this fails closed and
    /// requires the grant, so that a read of a real table is never missed.
    bool namesATable(const ASTIdentifier & identifier) const
    {
        const String & name = identifier.name();
        if (isCteName(name))
            return false;

        if (expression_aliases.contains(name))
            return false;

        if (!identifier.compound())
            return true;

        if (inside_subquery)
            return subquery_levels.empty() || !subquery_levels.back().contains(name);

        if (!mutated_metadata)
            return true;

        /// Resolve the name against the mutated table the same way `addExpressionColumnsSelectAccess`
        /// does: as written first, then with a `table.` / `db.table.` qualifier stripped.
        if (isColumnOfMutatedTable(name))
            return false;

        std::string_view bare = name;
        const String db_table_prefix = mutated_database.empty() ? String{} : mutated_database + "." + mutated_table + ".";
        const String table_prefix = mutated_table + ".";
        if (!db_table_prefix.empty() && bare.starts_with(db_table_prefix))
            bare.remove_prefix(db_table_prefix.size());
        else if (bare.starts_with(table_prefix))
            bare.remove_prefix(table_prefix.size());

        return !isColumnOfMutatedTable(String(bare));
    }

    bool isColumnOfMutatedTable(const String & name) const
    {
        return mutated_metadata->columns.has(name) || mutated_metadata->isVirtualColumn(name);
    }

    /// The in-memory metadata of a table, when it is an ordinary table the catalog knows. Nothing
    /// when it cannot be resolved here - then the columns it reads are required as written, which
    /// asks for at least as much access as the read needs.
    StorageMetadataHandle tryGetMetadata(const StorageID & table_id) const
    {
        StorageID resolved{databaseOfTable(table_id), table_id.table_name};
        if (resolved.database_name.empty())
            return {};

        const auto storage = DatabaseCatalog::instance().tryGetTable(resolved, context);
        if (!storage)
            return {};

        return storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    }

    /// Every name a column of the given tables can be written as at this level: bare, and qualified
    /// with the table's alias, its name and its database and name. Empty when a table expression at
    /// this level is not an ordinary named table, or when a table cannot be found in the catalog -
    /// then nothing is a known column and `namesATable` fails closed.
    NameSet visibleColumns(const std::vector<StorageID> & tables, const Strings & aliases, bool all_tables_named) const
    {
        NameSet names;
        if (!all_tables_named)
            return names;

        for (size_t i = 0; i < tables.size(); ++i)
        {
            const auto metadata = tryGetMetadata(tables[i]);
            if (!metadata)
                return {};

            StorageID table_id{databaseOfTable(tables[i]), tables[i].table_name};
            Strings prefixes{""};
            if (!aliases[i].empty())
                prefixes.emplace_back(aliases[i] + ".");
            prefixes.emplace_back(table_id.table_name + ".");
            prefixes.emplace_back(table_id.database_name + "." + table_id.table_name + ".");

            for (const auto & column : metadata->columns)
                for (const auto & prefix : prefixes)
                    names.insert(prefix + column.name);
        }

        return names;
    }

    /// A `WITH` name and a session temporary table are not tables to grant `SELECT` on, exactly as
    /// in a plain `SELECT`.
    bool needsNoGrant(const StorageID & table_id) const
    {
        if (!table_id.database_name.empty())
            return false;

        return isCteName(table_id.table_name)
            || static_cast<bool>(context->tryResolveStorageID(
                   StorageID{"", table_id.table_name}, Context::ResolveExternal));
    }

    /// The aliases of the expressions of one `SELECT` level, including those of an `ARRAY JOIN`
    /// list, collected exactly as `AddDefaultDatabaseVisitor::collectAliases` collects them: the
    /// rule that decides a name on the right of `IN` is an alias rather than a table has to be the
    /// same one that decided whether to qualify it, or the access this asks for is not the access
    /// the mutation takes. A nested `SELECT` and a table expression are skipped - their aliases
    /// belong to their own level.
    void collectExpressionAliases(const ASTPtr & ast)
    {
        if (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>() || ast->as<ASTTableExpression>())
            return;

        if (const String alias = ast->tryGetAlias(); !alias.empty())
        {
            /// The value is kept when it is a string literal, because `dictGet` and `joinGet` may
            /// name their object by such an alias - see `tryGetFunctionObject`. Two expressions of
            /// the same alias do not tell which one a name stands for, so the value becomes unknown.
            std::optional<String> string_value;
            if (const auto * literal = ast->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
                string_value = literal->value.safeGet<String>();

            const auto [it, inserted] = expression_aliases.emplace(alias, string_value);
            if (!inserted && it->second != string_value)
                it->second.reset();
        }

        for (const auto & child : ast->children)
            collectExpressionAliases(child);
    }

    bool isCteName(const String & name) const
    {
        return std::ranges::find(cte_names, name) != cte_names.end();
    }

    /// The database an unqualified ordinary table in a mutation expression is read from: the
    /// database of the mutated table, not the session's current one. `InterpreterAlterQuery` and
    /// `InterpreterUpdateQuery` qualify the expression with `AddDefaultDatabaseVisitor(...,
    /// table_id.getDatabaseName())` before the mutation is stored, and the stored predicate of a
    /// lightweight `DELETE FROM` is later run in a background context that has no current database
    /// at all, so a bare `other` in `ALTER TABLE db1.t DELETE WHERE id IN other` never means the
    /// session's `other`. Requiring the grant on the same table that is read leaves no room for a
    /// user who can read `current_db.other` but not `db1.other`.
    ///
    /// The object of a `dictGet` / `joinGet` is qualified by the same visitor
    /// (`visitFunctionTableNameArguments`) and with the same database, so it is resolved here the
    /// same way: a session on `db2` mutating `db1.t` reads `db1.join_tab`, and requiring the grant
    /// on `db2.join_tab` would leave this hole open for same-named objects in another database.
    ///
    /// An empty database is kept when the mutated table's database is unknown and there is no
    /// current one: `executeDDLQueryOnCluster` expands an empty database in an access element to
    /// each host's default database, so the requirement travels with the query instead of being
    /// dropped.
    String databaseOfTable(const StorageID & table_id) const
    {
        if (!table_id.database_name.empty())
            return table_id.database_name;
        if (!mutated_database.empty())
            return mutated_database;
        return context->getCurrentDatabase();
    }

    /// The access a `dictGet` needs on the dictionary its first argument names, resolved the same
    /// way the mutation resolves it: an unqualified name is qualified with the database of the
    /// mutated table when a dictionary of that name exists there, and names an XML dictionary
    /// otherwise - which is granted under `IDictionary::NO_DATABASE_TAG`, as
    /// `FunctionDictHelper::getDictionary` checks it.
    AccessRightsElement dictionaryAccess(const StorageID & dictionary_id) const
    {
        if (!dictionary_id.database_name.empty())
            return {AccessType::dictGet, dictionary_id.database_name, dictionary_id.table_name};

        const auto qualified = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(
            dictionary_id.table_name, databaseOfTable(dictionary_id));

        if (!qualified.database.empty())
            return {AccessType::dictGet, qualified.database, qualified.table};

        return {AccessType::dictGet, IDictionary::NO_DATABASE_TAG, qualified.table};
    }

    AccessRightsElements & required_access;
    ContextPtr context;
    const String & mutated_database;
    const String & mutated_table;
    const StorageInMemoryMetadata * mutated_metadata;
    /// The `WITH` names in scope, innermost last; see `visitSelect`.
    std::vector<String> cte_names;
    /// The scalar `WITH` aliases in scope, innermost last; see `tryGetFunctionObject`.
    std::vector<WithScalar> with_scalars;
    /// The column names visible at each enclosing subquery level, innermost last; see `visibleColumns`.
    std::vector<NameSet> subquery_levels;
    /// The expression aliases of the `SELECT` level being visited, each with the string literal it
    /// stands for when it is one; see `collectExpressionAliases`.
    std::unordered_map<String, std::optional<String>> expression_aliases;
    bool inside_subquery = false;
};

}

void addExpressionIndirectReadsAccess(
    AccessRightsElements & required_access,
    const IAST * expression,
    const ContextPtr & context,
    const String & mutated_database,
    const String & mutated_table,
    const StorageInMemoryMetadata * mutated_metadata)
{
    if (!expression)
        return;

    IndirectReadsCollector(required_access, context, mutated_database, mutated_table, mutated_metadata)
        .visitExpression(expression);
}

}
