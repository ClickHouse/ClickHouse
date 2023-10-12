#include "Parsers/IAST_fwd.h"
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/MutationCommands.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

/// Helps to detect situations, where non-deterministic functions may be used in mutations.
class FirstNonDeterministicFunctionMatcher
{
public:
    struct Data
    {
        ContextPtr context;
        FirstNonDeterministicFunctionResult result;
    };

    static bool needChildVisit(const ASTPtr & /*node*/, const ASTPtr & /*child*/)
    {
        return true;
    }

    static void visit(const ASTPtr & node, Data & data)
    {
        if (data.result.nondeterministic_function_name || data.result.subquery)
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
            /// by the contents of its definition, so we just proceed to it.
            if (function->name != "lambda")
            {
                /// NOTE It may be an aggregate function, so get(...) may throw.
                /// However, an aggregate function can be used only in subquery and we do not go into subquery.
                const auto func = FunctionFactory::instance().get(function->name, data.context);
                if (!func->isDeterministic())
                    data.result.nondeterministic_function_name = func->getName();
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
            auto literal = std::make_unique<ASTLiteral>(std::move(field));
            ast = addTypeConversionToAST(std::move(literal), type->getName());
        }
    }
};

using ExecuteNonDeterministicConstFunctionsVisitor = InDepthNodeVisitor<ExecuteNonDeterministicConstFunctionsMatcher, true>;

}

FirstNonDeterministicFunctionResult findFirstNonDeterministicFunction(const MutationCommand & command, ContextPtr context)
{
    FirstNonDeterministicFunctionMatcher::Data finder_data{context, {}};

    switch (command.type)
    {
        case MutationCommand::UPDATE:
        {
            auto update_assignments_ast = command.ast->as<const ASTAlterCommand &>().update_assignments->clone();
            FirstNonDeterministicFunctionFinder(finder_data).visit(update_assignments_ast);

            if (finder_data.result.nondeterministic_function_name)
                return finder_data.result;

            /// Currently UPDATE and DELETE both always have predicates so we can use fallthrough
            [[fallthrough]];
        }

        case MutationCommand::DELETE:
        {
            auto predicate_ast = command.predicate->clone();
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
    if (!settings.mutations_execute_subqueries_on_initiator
        && !settings.mutations_execute_nondeterministic_on_initiator)
        return nullptr;

    auto query = alter_command.clone();
    auto & new_alter_command = *query->as<ASTAlterCommand>();

    if (settings.mutations_execute_subqueries_on_initiator)
    {
        Scalars scalars;
        Scalars local_scalars;

        ExecuteScalarSubqueriesVisitor::Data data{
            WithContext{context},
            /*subquery_depth=*/ 0,
            scalars,
            local_scalars,
            /*only_analyze=*/ false,
            /*is_create_parameterized_view=*/ false,
            /*replace_only_to_literals=*/ true,
            settings.mutations_max_literal_size_to_replace};

        ExecuteScalarSubqueriesVisitor visitor(data);
        if (new_alter_command.update_assignments)
            visitor.visit(new_alter_command.update_assignments);
        if (new_alter_command.predicate)
            visitor.visit(new_alter_command.predicate);
    }

    if (settings.mutations_execute_nondeterministic_on_initiator)
    {
        ExecuteNonDeterministicConstFunctionsVisitor::Data data{
            context, settings.mutations_max_literal_size_to_replace};

        ExecuteNonDeterministicConstFunctionsVisitor visitor(data);
        if (new_alter_command.update_assignments)
            visitor.visit(new_alter_command.update_assignments);
        if (new_alter_command.predicate)
            visitor.visit(new_alter_command.predicate);
    }

    return query;
}

}
