#include <Core/Block.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/MutationCommands.h>
#include <Interpreters/StorageID.h>
#include <Storages/ColumnsDescription.h>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool mutations_execute_nondeterministic_on_initiator;
    extern const SettingsBool mutations_execute_subqueries_on_initiator;
    extern const SettingsUInt64 mutations_max_literal_size_to_replace;
}

namespace
{

/// Helps to detect situations, where non-deterministic functions may be used in mutations.
class FirstNonDeterministicFunctionMatcher
{
public:
    struct Data
    {
        ContextPtr context;
        const NameSet & nondeterministic_virtual_columns;
        const StorageID * storage_id;
        const ColumnsDescription * columns;
        /// Inside the definition of an `ALIAS` or `EPHEMERAL` column only virtual columns are looked for:
        /// non-deterministic functions there were never refused and are out of scope of this check.
        bool only_virtual_columns = false;
        /// Parameters of the enclosing lambdas: an identifier whose first part is one of them is bound
        /// to the parameter, not to a column of the table.
        NameSet lambda_parameters = {};
        FirstNonDeterministicFunctionResult result = {};
    };

    static bool isLambda(const ASTPtr & node)
    {
        const auto * function = node->as<ASTFunction>();
        return function && function->name == "lambda";
    }

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & /*child*/)
    {
        /// The body of a lambda is visited separately in `visit`, with the lambda parameters bound.
        return !isLambda(node);
    }

    static String joinParts(const Strings & parts, size_t begin, size_t end)
    {
        String name = parts[begin];
        for (size_t i = begin + 1; i < end; ++i)
            name += "." + parts[i];
        return name;
    }

    static String joinParts(const Strings & parts, size_t begin) { return joinParts(parts, begin, parts.size()); }

    /// Returns the storage column named by `parts` starting at `begin` or, for a subcolumn (`a.s`),
    /// its parent column (`a`), if any.
    static const ColumnDescription * tryGetStorageColumn(const Strings & parts, size_t begin, const ColumnsDescription & columns)
    {
        if (auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, joinParts(parts, begin)))
            return columns.tryGet(column->getNameInStorage());

        /// Subcolumns of `ALIAS` columns are not registered, they are extracted after evaluating the alias.
        for (size_t end = parts.size() - 1; end > begin; --end)
        {
            const auto * column = columns.tryGet(joinParts(parts, begin, end));
            if (column && column->default_desc.kind == ColumnDefaultKind::Alias
                && column->type->tryGetSubcolumnType(joinParts(parts, end)))
                return column;
        }
        return nullptr;
    }

    /// Returns the index of the first part of `identifier` that names a column of the mutated table:
    /// the qualifier of the mutated table (`t._table`, `db.t.a.s`) is skipped, like the analyzer does.
    /// A compound identifier which names a real column or its subcolumn (`b._table` for a column
    /// `b Tuple(_table String)` of table `b`) is resolved to that column, so it is not stripped.
    static size_t getColumnPartsBegin(const ASTIdentifier & identifier, const Data & data)
    {
        const auto & parts = identifier.name_parts;
        if (!data.storage_id || parts.size() < 2)
            return 0;

        auto names_column = [&](size_t begin)
        {
            return data.nondeterministic_virtual_columns.contains(joinParts(parts, begin))
                || (data.columns && tryGetStorageColumn(parts, begin, *data.columns));
        };

        if (names_column(0))
            return 0;
        if (parts.size() >= 3 && parts[0] == data.storage_id->database_name && parts[1] == data.storage_id->table_name && names_column(2))
            return 2;
        if (parts[0] == data.storage_id->table_name && names_column(1))
            return 1;
        return 0;
    }

    /// Visits `expression` with the given lambda parameters bound and stores the result into `data`.
    static void visitScope(const ASTPtr & expression, NameSet lambda_parameters, bool only_virtual_columns, Data & data)
    {
        Data scope_data{data.context, data.nondeterministic_virtual_columns, data.storage_id, data.columns, only_virtual_columns, std::move(lambda_parameters)};
        ASTPtr expression_copy = expression;
        InDepthNodeVisitor<FirstNonDeterministicFunctionMatcher, true>(scope_data).visit(expression_copy);
        data.result = std::move(scope_data.result);
    }

    static void visit(const ASTPtr & node, Data & data)
    {
        if (data.result.nondeterministic_function_name || data.result.nondeterministic_virtual_column_name || data.result.subquery)
            return;

        if (data.only_virtual_columns && !node->as<ASTIdentifier>() && !isLambda(node))
            return;

        if (node->as<ASTSelectQuery>())
        {
            /// We cannot determine if subquery is deterministic or not,
            /// so we do not allow to use subqueries in mutation without allow_nondeterministic_mutations=1
            data.result.subquery = true;
        }
        else if (const auto * function = typeid_cast<const ASTFunction *>(node.get()))
        {
            /// Property of being deterministic for lambda expression is completely determined
            /// by the contents of its definition, so we just proceed to it. Its parameters are
            /// bound inside the body and shadow the columns with the same names there.
            if (function->name == "lambda")
            {
                NameSet lambda_parameters = data.lambda_parameters;
                for (const auto & name : RequiredSourceColumnsMatcher::extractNamesFromLambda(*function))
                    lambda_parameters.insert(name);

                visitScope(function->arguments->children[1], std::move(lambda_parameters), data.only_virtual_columns, data);
            }
            else
            {
                /// NOTE It may be an aggregate function, so get(...) may throw.
                /// However, an aggregate function can be used only in subquery and we do not go into subquery.
                const auto func = FunctionFactory::instance().get(function->name, data.context);
                if (!func->isDeterministic())
                    data.result.nondeterministic_function_name = func->getName();
            }
        }
        else if (const auto * identifier = node->as<ASTIdentifier>())
        {
            const auto & parts = identifier->name_parts;
            if (data.lambda_parameters.contains(parts[0]))
                return;

            /// A virtual column such as `_table` or `_database` is a constant on one server, but replicas
            /// of the same table may have different local names, so it is as non-deterministic as `hostName`.
            size_t begin = getColumnPartsBegin(*identifier, data);
            String column_name = joinParts(parts, begin);
            if (data.nondeterministic_virtual_columns.contains(column_name))
            {
                data.result.nondeterministic_virtual_column_name = std::move(column_name);
                return;
            }

            /// `ALIAS` and `EPHEMERAL` columns are computed on read from their definitions, which may
            /// reference such virtual columns (`v String ALIAS _table`), so follow them, also when only
            /// their subcolumn is read (`a.s`). `DEFAULT` and `MATERIALIZED` columns cannot reference
            /// `_table` and similar virtual columns at all.
            if (!data.columns || data.nondeterministic_virtual_columns.empty())
                return;
            const auto * column = tryGetStorageColumn(parts, begin, *data.columns);
            if (column && column->default_desc.expression
                && (column->default_desc.kind == ColumnDefaultKind::Alias || column->default_desc.kind == ColumnDefaultKind::Ephemeral))
            {
                /// The definition is resolved in the scope of the table, where the lambda parameters of
                /// the mutation are not visible. Column definitions cannot reference each other cyclically,
                /// so the recursion terminates.
                visitScope(column->default_desc.expression, {}, /*only_virtual_columns=*/ true, data);
            }
        }
    }
};

using FirstNonDeterministicFunctionFinder = InDepthNodeVisitor<FirstNonDeterministicFunctionMatcher, true>;
using FirstNonDeterministicFunctionData = FirstNonDeterministicFunctionMatcher::Data;

/// Executes and replaces with literals
/// non-deterministic functions in query.
/// Similar to ExecuteScalarSubqueriesVisitor.
class ExecuteNonDeterministicConstFunctionsMatcher
{
public:

    struct Data
    {
        ContextPtr context;
        std::optional<size_t> max_literal_size;
    };

    static bool needChildVisit(const ASTPtr & ast, const ASTPtr & /*child*/)
    {
        /// Do not visit subqueries because they are executed separately.
        return !ast->as<ASTSelectQuery>();
    }

    static void visit(ASTPtr & ast, const Data & data)
    {
        if (auto * function = ast->as<ASTFunction>())
            visit(*function, ast, data);
    }

    static void visit(ASTFunction & function, ASTPtr & ast, const Data & data)
    {
        if (!FunctionFactory::instance().has(function.name))
            return;

        /// It makes sense to execute functions which are deterministic
        /// in scope of query because they are usually constant expressions.
        auto builder = FunctionFactory::instance().get(function.name, data.context);
        if (builder->isDeterministic() || !builder->isDeterministicInScopeOfQuery())
            return;

        Field field;
        DataTypePtr type;

        try
        {
            std::tie(field, type) = evaluateConstantExpression(ast, data.context);
        }
        catch (...)
        {
            /// An exception can be thrown if the expression is not constant.
            /// It's ok in that context and we just do nothing in that case.
            /// It's bad pattern but it's quite hard to implement it in another way.
            return;
        }

        auto column = type->createColumn();
        column->insert(field);

        Block scalar{{std::move(column), type, "_constant"}};
        if (worthConvertingScalarToLiteral(scalar, data.max_literal_size))
        {
            auto literal = make_intrusive<ASTLiteral>(std::move(field));
            ast = addTypeConversionToAST(std::move(literal), type->getName());
        }
    }
};

using ExecuteNonDeterministicConstFunctionsVisitor = InDepthNodeVisitor<ExecuteNonDeterministicConstFunctionsMatcher, true>;

}

FirstNonDeterministicFunctionResult findFirstNonDeterministicFunction(
    const MutationCommand & command,
    ContextPtr context,
    const NameSet & nondeterministic_virtual_columns,
    const StorageID * storage_id,
    const ColumnsDescription * columns)
{
    FirstNonDeterministicFunctionMatcher::Data finder_data{context, nondeterministic_virtual_columns, storage_id, columns};

    switch (command.type)
    {
        case MutationCommand::UPDATE:
        {
            auto alter = command.ast();
            auto update_assignments_ast = alter->update_assignments->clone();
            FirstNonDeterministicFunctionFinder(finder_data).visit(update_assignments_ast);

            if (finder_data.result.nondeterministic_function_name || finder_data.result.nondeterministic_virtual_column_name)
                return finder_data.result;

            ASTPtr predicate_ast(alter->predicate);
            FirstNonDeterministicFunctionFinder(finder_data).visit(predicate_ast);
            return finder_data.result;
        }

        case MutationCommand::DELETE:
        {
            auto alter = command.ast();
            ASTPtr predicate_ast(alter->predicate);
            FirstNonDeterministicFunctionFinder(finder_data).visit(predicate_ast);
            return finder_data.result;
        }

        default:
            break;
    }

    return {};
}

ASTPtr replaceNonDeterministicToScalars(const ASTAlterCommand & alter_command, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::mutations_execute_subqueries_on_initiator] && !settings[Setting::mutations_execute_nondeterministic_on_initiator])
        return nullptr;

    auto query = alter_command.clone();
    auto & new_alter_command = *query->as<ASTAlterCommand>();

    auto remove_child = [](auto & children, IAST *& erase_ptr)
    {
        auto it = std::find_if(children.begin(), children.end(), [&](const auto & ptr) { return ptr.get() == erase_ptr; });
        erase_ptr = nullptr;
        children.erase(it);
    };
    auto visit = [&](auto & visitor)
    {
        if (new_alter_command.update_assignments)
        {
            ASTPtr update_assignments = new_alter_command.update_assignments->clone();
            remove_child(new_alter_command.children, new_alter_command.update_assignments);
            visitor.visit(update_assignments);
            new_alter_command.update_assignments = new_alter_command.children.emplace_back(std::move(update_assignments)).get();
        }
        if (new_alter_command.predicate)
        {
            ASTPtr predicate = new_alter_command.predicate->clone();
            remove_child(new_alter_command.children, new_alter_command.predicate);
            visitor.visit(predicate);
            new_alter_command.predicate = new_alter_command.children.emplace_back(std::move(predicate)).get();
        }
    };

    if (settings[Setting::mutations_execute_subqueries_on_initiator])
    {
        Scalars scalars;
        Scalars local_scalars;

        ExecuteScalarSubqueriesVisitor::Data data{
            WithContext{context},
            /*subquery_depth=*/0,
            scalars,
            local_scalars,
            /*only_analyze=*/false,
            /*is_create_parameterized_view=*/false,
            /*replace_only_to_literals=*/true,
            settings[Setting::mutations_max_literal_size_to_replace]};

        ExecuteScalarSubqueriesVisitor visitor(data);
        visit(visitor);
    }

    if (settings[Setting::mutations_execute_nondeterministic_on_initiator])
    {
        ExecuteNonDeterministicConstFunctionsVisitor::Data data{context, settings[Setting::mutations_max_literal_size_to_replace]};

        ExecuteNonDeterministicConstFunctionsVisitor visitor(data);
        visit(visitor);
    }

    return query;
}

}
