#pragma once

#include <Common/typeid_cast.h>
#include <Core/QualifiedTableName.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTAlterQuery.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/misc.h>
#include <Poco/String.h>
#include <optional>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

/// Visitors consist of functions with unified interface 'void visit(Cast & x, ASTPtr & y)', there x is y, successfully cast to Cast.
/// Both types and function could have const specifiers. The second argument is used by visitor to replaces AST node (y) if needed.

/// Visits AST nodes, add default database to tables if not set. There's different logic for DDLs and selects.
class AddDefaultDatabaseVisitor
{
public:
    explicit AddDefaultDatabaseVisitor(
        ContextPtr context_,
        const String & database_name_,
        bool only_replace_current_database_function_ = false,
        bool only_replace_in_join_ = false)
        : context(context_)
        , database_name(database_name_)
        , only_replace_current_database_function(only_replace_current_database_function_)
        , only_replace_in_join(only_replace_in_join_)
    {
        if (!context->isGlobalContext())
        {
            for (const auto & [table_name, _ /* storage */] : context->getExternalTables())
            {
                external_tables.insert(table_name);
            }
        }
    }

    void visitDDL(ASTPtr & ast) const
    {
        visitDDLWithParent(nullptr, ast);
    }

    /// TODO: Add `parent` to the IAST
    void visitDDLWithParent(ASTPtr parent, ASTPtr & ast) const
    {
        visitDDLChildren(ast);

        if (!tryVisitDynamicCast<ASTAlterQuery>(parent, ast) &&
            !tryVisitDynamicCast<ASTQueryWithTableAndOutput>(parent, ast) &&
            !tryVisitDynamicCast<ASTRenameQuery>(parent, ast) &&
            !tryVisitDynamicCast<ASTSystemQuery>(parent, ast) &&
            !tryVisitDynamicCast<ASTFunction>(parent, ast))
        {}
    }

    void visit(ASTPtr & ast) const
    {
        if (auto * subquery = ast->as<ASTSubquery>(); subquery && isCopyOfWithElementBody(*subquery))
        {
            visitCopyOfWithElementBody(*subquery, [&](ASTPtr & node) { visit(node); });
            return;
        }

        if (!tryVisit<ASTSelectQuery>(ast) &&
            !tryVisit<ASTSelectWithUnionQuery>(ast) &&
            !tryVisit<ASTFunction>(ast) &&
            !tryVisit<ASTRefreshStrategy>(ast))
            visitChildren(*ast);
    }

    /// Add the default database to the table names only, without rewriting anything else.
    /// The table names live in table expressions and in the arguments of the functions which
    /// take a table (or dictionary) name: the right argument of `IN`, the first argument of
    /// `dictGet`. This is used after SQL UDF expansion, where the rest of the query has already
    /// been normalized and only the names brought in by the expansion are still unqualified,
    /// and for the metadata written before the names were qualified at CREATE time.
    void visitTableExpressions(IAST & ast) const
    {
        /// `visitTableExpressionsImpl` collects the `WITH` aliases of the select queries
        /// it walks, and the callers reuse one visitor for the `SELECT` of a view and for the
        /// column list of the same `CREATE` query. The aliases of one top-level call must not leak
        /// into the next one, where the same name is an ordinary table name and has to be qualified.
        auto enclosing_with_aliases = std::move(with_aliases);
        auto enclosing_with_expression_aliases = std::move(with_expression_aliases);
        with_aliases.clear();
        with_expression_aliases.clear();
        visitTableExpressionsImpl(ast);
        with_aliases = std::move(enclosing_with_aliases);
        with_expression_aliases = std::move(enclosing_with_expression_aliases);
    }

    void visit(ASTSelectQuery & select) const
    {
        ASTPtr unused;
        visit(select, unused);
    }

    void visit(ASTSelectWithUnionQuery & select) const
    {
        ASTPtr unused;
        visit(select, unused);
    }

    void visit(ASTColumns & columns) const
    {
        for (auto & child : columns.children)
            visit(child);
    }

    void visit(ASTRefreshStrategy & refresh) const
    {
        ASTPtr unused;
        visit(refresh, unused);
    }

    /// Substitute the database only into table functions that use the current database implicitly,
    /// e.g. `merge('tables_regexp')`, without qualifying table identifiers.
    /// It is used for `ALTER ... ON CLUSTER`: the identifiers are qualified when the query
    /// is interpreted on each host, but the table functions have to be canonicalized before
    /// `executeDDLQueryOnCluster` replaces `currentDatabase()` with the database of the session.
    void substituteDatabaseInTableFunctions(IAST & ast) const
    {
        if (const auto * table_expression = ast.as<ASTTableExpression>(); table_expression && table_expression->table_function)
            visitTableFunction(*table_expression->table_function);

        for (auto & child : ast.children)
            substituteDatabaseInTableFunctions(*child);
    }

private:

    ContextPtr context;

    const String database_name;
    std::set<String> external_tables;
    struct WithDeclaration
    {
        /// Whether a bare name that refers to the declaration is left unqualified.
        bool hides_table = false;
        bool is_recursive = false;
    };

    /// The declarations of the common table expressions of the enclosing select queries by name,
    /// the nearest last.
    mutable std::unordered_map<String, std::vector<WithDeclaration>> with_aliases;
    /// The aliases of the expressions of the `WITH` clauses of the enclosing select queries,
    /// e.g. `WITH [1, 2] AS s`. Unlike other aliases, they are visible in the nested select queries.
    mutable std::unordered_set<String> with_expression_aliases;
    /// The aliases of the expressions of the current select query.
    mutable std::unordered_set<String> expression_aliases;

    bool only_replace_current_database_function = false;
    bool only_replace_in_join = false;

    void visitTableExpressionsImpl(IAST & ast) const
    {
        if (auto * select = ast.as<ASTSelectQuery>())
        {
            /// A name defined by a `WITH` element is a common table expression and not a table,
            /// recursive or not, so a table expression (or the right argument of `IN`) which refers
            /// to it must not be qualified. Unlike the callers of the full traversal, the callers of
            /// this pass do not inline the common table expressions first: the metadata of a view is
            /// repaired for the dependency graphs as it is stored, `WITH cte AS (...) SELECT ... FROM cte`,
            /// and qualifying `cte` there turned the dependency of the view on its source table into
            /// a dependency on a nonexistent table. The names are visible in this select query and in
            /// its subqueries, and are forgotten when the walk leaves it.
            auto enclosing_with_aliases = with_aliases;
            auto enclosing_with_expression_aliases = with_expression_aliases;
            addWithAliases(*select, /* only_recursive_with_hides_table = */ false);

            /// The right argument of `IN` may refer to an alias of an expression defined
            /// elsewhere in the query, possibly after the point of use - then it is not
            /// a table name. Collect the aliases of this select query before descending
            /// into the children, exactly like `visit(ASTSelectQuery &, ASTPtr &)` of the
            /// full traversal does, with the same scoping.
            auto enclosing_query_aliases = std::move(expression_aliases);
            expression_aliases.clear();
            for (const auto & child : select->children)
                collectAliases(child);

            for (auto & child : select->children)
            {
                if (child.get() == select->with().get())
                    visitWithList(*select, [&](ASTPtr & node) { visitTableExpressionsImpl(*node); });
                else
                    visitTableExpressionsImpl(*child);
            }

            expression_aliases = std::move(enclosing_query_aliases);
            with_aliases = std::move(enclosing_with_aliases);
            with_expression_aliases = std::move(enclosing_with_expression_aliases);
            return;
        }

        if (auto * subquery = ast.as<ASTSubquery>(); subquery && isCopyOfWithElementBody(*subquery))
        {
            visitCopyOfWithElementBody(*subquery, [&](ASTPtr & node) { visitTableExpressionsImpl(*node); });
            return;
        }

        if (auto * table_expression = ast.as<ASTTableExpression>())
        {
            if (table_expression->database_and_table_name)
            {
                auto table_identifier = table_expression->database_and_table_name;
                tryVisit<ASTTableIdentifier>(table_identifier);

                /// Keep `database_and_table_name` and `children` synchronized.
                if (table_identifier != table_expression->database_and_table_name)
                    table_expression->setOrReplace(table_expression->database_and_table_name, std::move(table_identifier));
            }
            else if (table_expression->table_function)
                visitTableFunction(*table_expression->table_function);
        }

        std::optional<std::unordered_set<String>> enclosing_lambda_aliases;
        if (auto * function = ast.as<ASTFunction>(); function && function->arguments)
        {
            if (isASTLambdaFunction(*function))
            {
                enclosing_lambda_aliases = expression_aliases;
                addLambdaAliases(*function);
            }
            visitFunctionTableNameArguments(*function);
        }

        for (auto & child : ast.children)
            visitTableExpressionsImpl(*child);

        if (enclosing_lambda_aliases)
            expression_aliases = std::move(*enclosing_lambda_aliases);
    }

    /// Qualify the table names which are carried by function arguments rather than by table
    /// expressions: the dictionary name in the first argument of `dictGet`, the table name in the
    /// first argument of `joinGet` and the table name in the right argument of `IN` (and of the
    /// similar operators) - the same carriers `MarkTableIdentifiersVisitor` and the dependency
    /// visitors know. The subqueries among the arguments are covered by the generic recursion of
    /// `visitTableExpressionsImpl`.
    ///
    /// `joinGet` is qualified here although `visit(ASTFunction &)` of the full traversal leaves it
    /// alone: the loading dependency graph resolves a bare name against the database owning the
    /// definition, while `joinGet` itself resolves it against the current database of the query
    /// reading the view or inserting into the table. Persisting the qualified name is what makes
    /// the two agree.
    void visitFunctionTableNameArguments(ASTFunction & function) const
    {
        const bool is_operator_in = functionIsInOrGlobalInOperator(function.name);
        const bool is_dict_get = functionIsDictGet(function.name);
        const bool is_join_get = functionIsJoinGet(function.name);
        if (!is_operator_in && !is_dict_get && !is_join_get)
            return;

        auto & arguments = function.arguments->children;

        if (is_join_get && !arguments.empty())
        {
            if (auto * identifier = arguments[0]->as<ASTIdentifier>())
            {
                /// A compound identifier is already qualified, a parameterized name is only known
                /// when the view is called, a temporary table has no database, and an alias of an
                /// expression is not a table name at all.
                if (!identifier->compound() && !identifier->isParam()
                    && !external_tables.contains(identifier->name()) && !isExpressionAlias(identifier->name()))
                {
                    arguments[0] = make_intrusive<ASTIdentifier>(std::vector<String>{database_name, identifier->name()});
                }
            }
            else if (auto * literal = arguments[0]->as<ASTLiteral>())
            {
                auto & literal_value = literal->value;
                if (literal_value.getType() == Field::Types::String)
                {
                    auto qualified_table_name = QualifiedTableName::tryParseFromString(literal_value.safeGet<String>());
                    if (qualified_table_name && qualified_table_name->database.empty() && !external_tables.contains(qualified_table_name->table))
                    {
                        qualified_table_name->database = database_name;
                        literal_value = qualified_table_name->getFullName();
                    }
                }
            }
        }

        if (is_dict_get && !arguments.empty())
        {
            if (auto * identifier = arguments[0]->as<ASTIdentifier>())
            {
                /// A compound identifier is already qualified, and a parameterized name is only
                /// known when the view is called, so there is nothing to qualify.
                /// The name is resolved against `database_name` and not against the current database
                /// of `context`: on the metadata-load paths the context is the loading context, whose
                /// current database is unrelated to the database owning the definition.
                if (!identifier->compound() && !identifier->isParam())
                {
                    auto qualified_dictionary_name = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(identifier->name(), database_name);
                    arguments[0] = make_intrusive<ASTIdentifier>(qualified_dictionary_name.getParts());
                }
            }
            else if (auto * literal = arguments[0]->as<ASTLiteral>())
            {
                auto & literal_value = literal->value;
                if (literal_value.getType() == Field::Types::String)
                {
                    auto qualified_dictionary_name = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(literal_value.safeGet<String>(), database_name);
                    literal_value = qualified_dictionary_name.getFullName();
                }
            }
        }

        if (is_operator_in && arguments.size() > 1)
        {
            /// A plain identifier in the right argument of `IN` is a table name.
            if (auto * identifier = arguments[1]->as<ASTIdentifier>(); identifier && !identifier->as<ASTTableIdentifier>())
            {
                /// Unless it is an alias of an expression defined elsewhere in the query -
                /// then it is not a table name and must not be qualified with the database,
                /// like in `visit(ASTFunction &, ASTPtr &)` of the full traversal.
                if (isExpressionAlias(identifier->name()))
                    return;

                if (auto maybe_table_identifier = identifier->createTable())
                    arguments[1] = maybe_table_identifier;
            }

            tryVisit<ASTTableIdentifier>(arguments[1]);
        }
    }

    void visit(ASTSelectWithUnionQuery & select, ASTPtr &) const
    {
        for (auto & child : select.list_of_selects->children)
        {
            if (child->as<ASTSelectQuery>())
                tryVisit<ASTSelectQuery>(child);
            else if (child->as<ASTSelectIntersectExceptQuery>())
                tryVisit<ASTSelectIntersectExceptQuery>(child);
        }
    }

    void visit(ASTSelectQuery & select, ASTPtr &) const
    {
        /// The callers of the full traversal expand the common table expressions with
        /// `ApplyWithSubqueryVisitor` first, so a name it left in place is a table, except for a
        /// reference of a recursive common table expression to itself: the names of a `WITH RECURSIVE`
        /// list must not be qualified. They are visible in this select query and in its subqueries,
        /// and are forgotten when the walk leaves it: another select query of the same `UNION`, or
        /// a sibling subquery, may use the same name for a table, which has to be qualified.
        auto enclosing_with_aliases = with_aliases;
        addWithAliases(select, /* only_recursive_with_hides_table = */ true);

        /// The right argument of IN may refer to an alias of an expression defined elsewhere
        /// in the query, possibly after the point of use - then it is not a table name.
        /// Collect the aliases before descending into the children.
        /// Like in `MarkTableIdentifiersVisitor`, only the aliases of the current select query
        /// are considered: an alias is not visible inside a nested select query and vice versa.
        auto enclosing_query_aliases = std::move(expression_aliases);
        expression_aliases.clear();
        for (const auto & child : select.children)
            collectAliases(child);

        if (select.tables())
            tryVisit<ASTTablesInSelectQuery>(select.refTables());

        for (auto & child : select.children)
        {
            if (child.get() == select.with().get())
                visitWithList(select, [&](ASTPtr & node) { visit(node); });
            else
                visit(child);
        }

        expression_aliases = std::move(enclosing_query_aliases);
        with_aliases = std::move(enclosing_with_aliases);
    }

    /// Add the names declared by the `WITH` clause of the select query: the names of the common
    /// table expressions, and the aliases of the expressions, which the analyzer also makes visible
    /// in the nested select queries. With `only_recursive_with_hides_table`, only the names of
    /// a `WITH RECURSIVE` list hide a table, for the full traversal, which runs after the others are
    /// expanded, and the aliases of the expressions are not collected.
    void addWithAliases(const ASTSelectQuery & select, bool only_recursive_with_hides_table) const
    {
        if (!select.with())
            return;

        for (const auto & child : select.with()->children)
        {
            if (const auto * with_element = typeid_cast<const ASTWithElement *>(child.get()))
            {
                with_aliases[with_element->name].push_back(
                {
                    .hides_table = select.recursive_with || !only_recursive_with_hides_table,
                    .is_recursive = select.recursive_with && getRecursiveBodyBranches(with_element->subquery),
                });
            }
            else if (!only_recursive_with_hides_table)
            {
                collectWithExpressionAliases(child);
            }
        }
    }

    void collectWithExpressionAliases(const ASTPtr & ast) const
    {
        ApplyWithSubqueryVisitor::forEachExpressionAlias(
            ast, [&](const String & alias, const ASTPtr &) { with_expression_aliases.insert(alias); });
    }

    bool isExpressionAlias(const String & name) const
    {
        return expression_aliases.contains(name) || with_expression_aliases.contains(name);
    }

    bool isWithAlias(const String & name) const
    {
        auto it = with_aliases.find(name);
        return it != with_aliases.end() && !it->second.empty() && it->second.back().hides_table;
    }

    /// Walk the `WITH` list of the select query with `visit_child`, each element as a body of the
    /// common table expression it declares.
    template <typename Visit>
    void visitWithList(ASTSelectQuery & select, Visit && visit_child) const
    {
        for (auto & child : select.with()->children)
        {
            if (auto * with_element = typeid_cast<ASTWithElement *>(child.get()))
                visitWithElementBody(with_element->name, with_element->subquery, child, visit_child);
            else
                visit_child(child);
        }
    }

    /// `ApplyWithSubqueryVisitor` replaces a reference to a common table expression with a copy of
    /// its body, which keeps the name in `cte_name`. The copy is a body of the nearest declaration
    /// of that name, like the element in the `WITH` list.
    bool isCopyOfWithElementBody(const ASTSubquery & subquery) const
    {
        if (subquery.cte_name.empty())
            return false;
        auto it = with_aliases.find(subquery.cte_name);
        return it != with_aliases.end() && !it->second.empty();
    }

    template <typename Visit>
    void visitCopyOfWithElementBody(ASTSubquery & subquery, Visit && visit_child) const
    {
        String name = subquery.cte_name;
        ASTPtr subquery_ptr = subquery.ptr();
        for (auto & child : subquery.children)
            visitWithElementBody(name, subquery_ptr, child, visit_child);
    }

    /// Walk the body of the nearest declaration of the common table expression `name`: `subquery`
    /// is the body, and `node` is what contains it (the `WITH` element) or what it contains. Only
    /// the recursive members of a recursive common table expression reference it, i.e. the branches
    /// of its body after the first one. Inside the body of an ordinary one, and inside the seed of
    /// a recursive one, its name is a table, unless an enclosing select query defines the same name.
    template <typename Visit>
    void visitWithElementBody(const String & name, const ASTPtr & subquery, ASTPtr & node, Visit && visit_child) const
    {
        auto & declarations = with_aliases[name];
        const WithDeclaration declaration = declarations.back();
        ASTs * branches = declaration.is_recursive ? getRecursiveBodyBranches(subquery) : nullptr;

        declarations.pop_back();
        visit_child(branches ? branches->front() : node);
        with_aliases[name].push_back(declaration);

        if (branches)
        {
            for (size_t i = 1; i < branches->size(); ++i)
                visit_child((*branches)[i]);
        }
    }

    /// The rule of `QueryTreeBuilder`: an element of a `WITH RECURSIVE` list is a recursive common
    /// table expression when its body is a `UNION`, an `INTERSECT` or an `EXCEPT` of several queries,
    /// possibly in parentheses. Returns the branches of such a body, the seed first, and null for
    /// a body of a single select query, which is an ordinary common table expression.
    static ASTs * getRecursiveBodyBranches(const ASTPtr & subquery)
    {
        if (!subquery || subquery->children.empty())
            return nullptr;

        IAST * body = subquery->children.front().get();
        while (auto * union_query = body->as<ASTSelectWithUnionQuery>())
        {
            auto & selects = union_query->list_of_selects->children;
            if (selects.size() != 1)
                return &selects;
            body = selects.front().get();
        }

        if (auto * intersect_except = body->as<ASTSelectIntersectExceptQuery>())
            return &intersect_except->children;
        return nullptr;
    }

    /// Collect aliases of expressions in the subtree, skipping nested select queries:
    /// their aliases are collected when the visitor descends into them.
    void collectAliases(const ASTPtr & ast) const
    {
        if (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
            return;

        /// The alias of a table expression names a table, but the aliases in the arguments of a table function belong
        /// to the select query, as in the analyzer, except inside a lambda or a subquery.
        if (const auto * table_expression = ast->as<ASTTableExpression>())
        {
            if (const auto * table_function = table_expression->table_function ? table_expression->table_function->as<ASTFunction>() : nullptr;
                table_function && table_function->arguments)
            {
                ApplyWithSubqueryVisitor::forEachExpressionAlias(
                    table_function->arguments, [&](const String & alias, const ASTPtr &) { expression_aliases.insert(alias); });
            }
            return;
        }

        String alias = ast->tryGetAlias();
        if (!alias.empty())
            expression_aliases.insert(alias);

        /// The parameters of a lambda and the aliases in its body are visible only inside the lambda,
        /// they are added when the visitor descends into it (`addLambdaAliases`).
        if (const auto * function = ast->as<ASTFunction>(); function && isASTLambdaFunction(*function))
            return;

        for (const auto & child : ast->children)
            collectAliases(child);
    }

    /// Add the names a lambda declares for its body: its parameters and the aliases in the body,
    /// except inside a nested lambda or a subquery.
    void addLambdaAliases(const ASTFunction & lambda) const
    {
        for (const auto & parameter : lambda.arguments->children[0]->as<ASTFunction &>().arguments->children)
        {
            if (const auto * identifier = parameter->as<ASTIdentifier>())
                expression_aliases.insert(identifier->name());
        }
        ApplyWithSubqueryVisitor::forEachExpressionAlias(
            lambda.arguments->children[1], [&](const String & alias, const ASTPtr &) { expression_aliases.insert(alias); });
    }

    void visit(ASTSelectIntersectExceptQuery & select, ASTPtr &) const
    {
        for (auto & child : select.getListOfSelects())
        {
            if (child->as<ASTSelectQuery>())
                tryVisit<ASTSelectQuery>(child);
            else if (child->as<ASTSelectIntersectExceptQuery>())
                tryVisit<ASTSelectIntersectExceptQuery>(child);
            else if (child->as<ASTSelectWithUnionQuery>())
                tryVisit<ASTSelectWithUnionQuery>(child);
        }
    }

    void visit(ASTTablesInSelectQuery & tables, ASTPtr &) const
    {
        for (auto & child : tables.children)
            tryVisit<ASTTablesInSelectQueryElement>(child);
    }

    void visit(ASTTablesInSelectQueryElement & tables_element, ASTPtr &) const
    {
        if (only_replace_in_join && !tables_element.table_join)
            return;

        if (tables_element.table_expression)
            tryVisit<ASTTableExpression>(tables_element.table_expression);
    }

    void visit(ASTTableExpression & table_expression, ASTPtr &) const
    {
        if (table_expression.database_and_table_name)
            tryVisit<ASTTableIdentifier>(table_expression.database_and_table_name);
        else if (table_expression.table_function)
            visitTableFunction(*table_expression.table_function);
    }

    /// Some table functions use the current database when it is not specified explicitly, e.g. `merge('regexp')`.
    /// The query can be interpreted later in a context where the current database is not set
    /// (for example, a mutation is interpreted in a background thread), so the database has to be
    /// substituted here, in the same way as it is done for table names.
    void visitTableFunction(IAST & table_function) const
    {
        if (database_name.empty())
            return;

        auto * function = table_function.as<ASTFunction>();
        if (!function || !function->arguments)
            return;

        auto & arguments = function->arguments->children;

        if (function->name == "merge")
        {
            /// merge('tables_regexp') -> merge('database_name', 'tables_regexp')
            if (arguments.size() == 1)
            {
                arguments.insert(arguments.begin(), make_intrusive<ASTLiteral>(database_name));
            }
            /// merge(currentDatabase(), 'tables_regexp') -> merge('database_name', 'tables_regexp'),
            /// because `currentDatabase` would be evaluated too late, in a context where the current database can be different.
            /// The database argument can be an arbitrary constant expression, e.g. `merge(concat(currentDatabase(), ''), 'tables_regexp')`,
            /// so `currentDatabase()` is substituted everywhere in the first argument, not only when it is the whole argument.
            else if (arguments.size() == 2)
            {
                substituteCurrentDatabase(arguments[0], *function->arguments);
            }
        }

        /// A table function can be an argument of another table function, e.g. `remote('127.0.0.1', merge('tables_regexp'))`.
        for (auto & argument : arguments)
            visitTableFunction(*argument);
    }

    /// Whether the function is `currentDatabase` or one of its aliases (`DATABASE`, `SCHEMA`, `current_database`),
    /// which are registered in `FunctionFactory` as case-insensitive.
    static bool isCurrentDatabaseFunction(const ASTFunction & function)
    {
        if (function.arguments && !function.arguments->children.empty())
            return false;

        if (function.name == "currentDatabase")
            return true;

        const String lowered_name = Poco::toLower(function.name);
        return lowered_name == "database" || lowered_name == "schema" || lowered_name == "current_database";
    }

    /// Replace `currentDatabase()` with a literal everywhere in the subtree.
    void substituteCurrentDatabase(ASTPtr & ast, IAST & parent) const
    {
        if (const auto * function = ast->as<ASTFunction>(); function && isCurrentDatabaseFunction(*function))
        {
            /// The `updatePointerToChild` function replaces the old address with the new one without access, so it is safe to invalidate it in place.
            /// However, just for safety, let's store the old node for a little longer.
            ASTPtr old_ast = ast;
            ast = make_intrusive<ASTLiteral>(database_name);
            parent.updatePointerToChild(old_ast.get(), ast);
            return;
        }

        for (auto & child : ast->children)
            substituteCurrentDatabase(child, *ast);
    }

    void visit(const ASTTableIdentifier & identifier, ASTPtr & ast) const
    {
        /// Already has database.
        if (identifier.compound())
            return;
        /// A parameterized name is only known when the view is called, and it has no
        /// resolvable name to qualify here.
        if (identifier.isParam())
            return;
        /// There is temporary table with such name, should not be rewritten.
        if (external_tables.contains(identifier.shortName()))
            return;
        /// This is a common table expression of an enclosing select query.
        if (isWithAlias(identifier.name()))
            return;

        auto qualified_identifier = make_intrusive<ASTTableIdentifier>(database_name, identifier.name());
        if (!identifier.alias.empty())
            qualified_identifier->setAlias(identifier.alias);
        ast = qualified_identifier;
    }

    void visit(ASTFunction & function, ASTPtr &) const
    {
        bool is_operator_in = functionIsInOrGlobalInOperator(function.name);
        bool is_dict_get = functionIsDictGet(function.name);

        std::optional<std::unordered_set<String>> enclosing_lambda_aliases;
        if (isASTLambdaFunction(function))
        {
            enclosing_lambda_aliases = expression_aliases;
            addLambdaAliases(function);
        }

        for (auto & child : function.children)
        {
            if (child.get() == function.arguments.get())
            {
                for (size_t i = 0; i < child->children.size(); ++i)
                {
                    if (is_dict_get && i == 0)
                    {
                        if (auto * identifier = child->children[i]->as<ASTIdentifier>())
                        {
                            /// Identifier already qualified
                            if (identifier->compound())
                                continue;

                            /// A parameterized name is only known when the view is called, and it
                            /// has no resolvable name to qualify here.
                            if (identifier->isParam())
                                continue;

                            auto qualified_dictionary_name = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(identifier->name(), context);
                            child->children[i] = make_intrusive<ASTIdentifier>(qualified_dictionary_name.getParts());
                        }
                        else if (auto * literal = child->children[i]->as<ASTLiteral>())
                        {
                            auto & literal_value = literal->value;

                            if (literal_value.getType() != Field::Types::String)
                                continue;

                            auto dictionary_name = literal_value.safeGet<String>();
                            auto qualified_dictionary_name = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(dictionary_name, context);
                            literal_value = qualified_dictionary_name.getFullName();
                        }
                    }
                    else if (is_operator_in && i == 1)
                    {
                        if (auto * identifier = child->children[i]->as<ASTIdentifier>())
                        {
                            /// The argument may be an alias of an expression defined elsewhere in the query,
                            /// e.g. `SELECT 'foo' AS object WHERE (object IN (('foo', 'bar') AS objects)) AND object IN objects`.
                            /// Then it is not a table name and must not be qualified with the database
                            /// (the similar code in `MarkTableIdentifiersVisitor` also checks the aliases).
                            if (!identifier->as<ASTTableIdentifier>() && isExpressionAlias(identifier->name()))
                                continue;

                            /// If identifier is broken then we can do nothing and get an exception
                            auto maybe_table_identifier = identifier->createTable();
                            if (maybe_table_identifier)
                                child->children[i] = maybe_table_identifier;
                        }

                        /// Second argument of the "in" function (or similar) may be a table name or a subselect.
                        /// Rewrite the table name or descend into subselect.
                        if (!tryVisit<ASTTableIdentifier>(child->children[i]))
                            visit(child->children[i]);
                    }
                    else
                    {
                        visit(child->children[i]);
                    }
                }
            }
            else
            {
                visit(child);
            }
        }

        if (enclosing_lambda_aliases)
            expression_aliases = std::move(*enclosing_lambda_aliases);
    }

    void visit(ASTRefreshStrategy & refresh, ASTPtr &) const
    {
        if (refresh.dependencies)
            for (auto & table : refresh.dependencies->children)
                tryVisit<ASTTableIdentifier>(table);
    }

    void visitChildren(IAST & ast) const
    {
        for (auto & child : ast.children)
            visit(child);
    }

    template <typename T>
    bool tryVisit(ASTPtr & ast) const
    {
        if (T * t = typeid_cast<T *>(ast.get()))
        {
            visit(*t, ast);
            return true;
        }
        return false;
    }


    void visitDDL(ASTPtr & /* parent */, ASTQueryWithTableAndOutput & node, ASTPtr &) const
    {
        if (only_replace_current_database_function)
            return;

        if (!node.database)
            node.setDatabase(database_name);
    }

    void visitDDL(ASTPtr & /* parent */, ASTRenameQuery & node, ASTPtr &) const
    {
        if (only_replace_current_database_function)
            return;

        node.setDatabaseIfNotExists(database_name);
    }

    void visitDDL(ASTPtr & /* parent */, ASTAlterQuery & node, ASTPtr &) const
    {
        if (only_replace_current_database_function)
            return;

        if (!node.database)
            node.setDatabase(database_name);

        for (const auto & child : node.command_list->children)
        {
            auto * command_ast = child->as<ASTAlterCommand>();
            if (command_ast->from_database.empty())
                command_ast->from_database = database_name;
            if (command_ast->to_database.empty())
                command_ast->to_database = database_name;
        }
    }

    void visitDDL(ASTPtr & /* parent */, ASTSystemQuery & query, ASTPtr &) const
    {
        if (query.type != ASTSystemQuery::Type::RELOAD_DICTIONARY
            && query.type != ASTSystemQuery::Type::UNLOAD_DICTIONARY)
            return;

        if (!query.table || query.database || only_replace_current_database_function)
            return;

        query.setDatabase(database_name);
    }

    void visitDDL(ASTPtr & parent, ASTFunction & function, ASTPtr & node) const
    {
        if (function.name == "currentDatabase")
        {
            /// The `updatePointerToChild` function replaces the old address with the new one without access, so it is safe to invalidate it in place.
            /// However, just for safety, let's store the old node for a little longer.
            ASTPtr old_node = node;
            node = make_intrusive<ASTLiteral>(database_name);

            if (parent)
            {
                parent->updatePointerToChild(old_node.get(), node.get());
            }
        }
    }

    void visitDDLChildren(ASTPtr & ast) const
    {
        for (auto & child : ast->children)
            visitDDLWithParent(ast, child);
    }

    template <typename T>
    bool tryVisitDynamicCast(ASTPtr & parent, ASTPtr & ast) const
    {
        if (T * t = dynamic_cast<T *>(ast.get()))
        {
            visitDDL(parent, *t, ast);
            return true;
        }
        return false;
    }
};

}
