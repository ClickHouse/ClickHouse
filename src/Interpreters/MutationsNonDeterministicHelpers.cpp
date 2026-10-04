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
        FirstNonDeterministicFunctionResult result;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & /*child*/)
    {
        /// The body of a lambda is visited separately in `visit`, with the lambda parameters masked.
        const auto * function = node->as<ASTFunction>();
        return !function || function->name != "lambda";
    }

    /// Returns the column name that `identifier` refers to in the mutated table: the short name when
    /// the identifier is qualified with the mutated table (`t._table`, `db.t._table`), the name itself
    /// otherwise. A qualifier which does not name the mutated table (e.g. a tuple element access)
    /// leaves the full compound name, which is not a virtual column name.
    static const String & getColumnName(const ASTIdentifier & identifier, const StorageID * storage_id)
    {
        if (!storage_id || !identifier.compound())
            return identifier.name();

        const auto & parts = identifier.name_parts;
        if (parts.size() == 2 && parts[0] == storage_id->table_name)
            return parts[1];
        if (parts.size() == 3 && parts[0] == storage_id->database_name && parts[1] == storage_id->table_name)
            return parts[2];
        return identifier.name();
    }

    static void visit(const ASTPtr & node, Data & data)
    {
        if (data.result.nondeterministic_function_name || data.result.nondeterministic_virtual_column_name || data.result.subquery)
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
            /// by the contents of its definition, so we just proceed to it. Its parameters shadow
            /// the virtual columns with the same names inside the body.
            if (function->name == "lambda")
            {
                NameSet masked_virtual_columns = data.nondeterministic_virtual_columns;
                for (const auto & name : RequiredSourceColumnsMatcher::extractNamesFromLambda(*function))
                    masked_virtual_columns.erase(name);

                Data body_data{data.context, masked_virtual_columns, data.storage_id, {}};
                ASTPtr body = function->arguments->children[1];
                InDepthNodeVisitor<FirstNonDeterministicFunctionMatcher, true>(body_data).visit(body);
                data.result = std::move(body_data.result);
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
            /// A virtual column such as `_table` or `_database` is a constant on one server, but replicas
            /// of the same table may have different local names, so it is as non-deterministic as `hostName`.
            const auto & column_name = getColumnName(*identifier, data.storage_id);
            if (data.nondeterministic_virtual_columns.contains(column_name))
                data.result.nondeterministic_virtual_column_name = column_name;
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
    const MutationCommand & command, ContextPtr context, const NameSet & nondeterministic_virtual_columns, const StorageID * storage_id)
{
    FirstNonDeterministicFunctionMatcher::Data finder_data{context, nondeterministic_virtual_columns, storage_id, {}};

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
