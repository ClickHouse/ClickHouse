#include <Core/Block.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MutationCommands.h>
#include <Analyzer/Utils.h>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <base/scope_guard.h>

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
            auto literal = make_intrusive<ASTLiteral>(std::move(field));
            literal->alias = function.alias;
            literal->setPreferAliasToColumnName(function.preferAliasToColumnName());
            ast = addTypeConversionToAST(std::move(literal), type->getName());
        }
    }
};

using ExecuteNonDeterministicConstFunctionsVisitor = InDepthNodeVisitor<ExecuteNonDeterministicConstFunctionsMatcher, true>;

bool isSubquery(const IAST & ast)
{
    return ast.as<ASTSubquery>() || ast.as<ASTSelectWithUnionQuery>() || ast.as<ASTSelectQuery>();
}

ASTFunction * tryGetLambda(IAST & ast)
{
    auto * function = ast.as<ASTFunction>();
    if (function && function->name == "lambda" && function->arguments && function->arguments->children.size() == 2)
        return function;
    return nullptr;
}

/// A lambda and a subquery have their own scope of aliases, but the alias of such a node belongs to the enclosing one.
void collectAliases(IAST & ast, NameSet & aliases)
{
    if (auto alias = ast.tryGetAlias(); !alias.empty())
        aliases.insert(std::move(alias));

    if (tryGetLambda(ast) || isSubquery(ast))
        return;

    for (const auto & child : ast.children)
        collectAliases(*child, aliases);
}

class NiladicFunctionCallsMaker
{
public:
    NiladicFunctionCallsMaker(
        const ColumnsDescription & table_columns_,
        const NameSet & new_column_names_,
        const NameSet & dropped_column_names_,
        const ContextPtr & context_,
        NameSet aliases)
        : table_columns(table_columns_), new_column_names(new_column_names_), dropped_column_names(dropped_column_names_), context(context_)
    {
        bound_names.push_back({{}, std::move(aliases)});
    }

    /// `ast` is in an expression position.
    void rewrite(ASTPtr & ast)
    {
        const String alias = ast->tryGetAlias();
        if (!alias.empty())
            aliases_in_definition.emplace_back(alias, bound_names.size() - 1);
        SCOPE_EXIT({
            if (!alias.empty())
                aliases_in_definition.pop_back();
        });

        if (const auto * identifier = ast->as<ASTIdentifier>())
        {
            if (!identifier->isShort() || isBound(identifier->name()))
                return;

            auto resolver = FunctionFactory::instance().tryGet(identifier->name(), context);
            if (!resolver || !resolver->allowsOmittingParentheses())
                return;

            auto function = makeASTFunction(identifier->name());
            function->setAlias(identifier->tryGetAlias());
            ast = std::move(function);
        }
        else if (auto * lambda = tryGetLambda(*ast))
        {
            auto & arguments = lambda->arguments->children;
            Scope scope;
            if (const auto * parameters = arguments[0]->as<ASTFunction>(); parameters && parameters->arguments)
            {
                for (const auto & parameter : parameters->arguments->children)
                    if (const auto * parameter_identifier = parameter->as<ASTIdentifier>())
                        scope.parameters.insert(parameter_identifier->name());
            }
            collectAliases(*arguments[1], scope.aliases);

            bound_names.push_back(std::move(scope));
            rewrite(arguments[1]);
            bound_names.pop_back();
        }
        else if (auto * function = ast->as<ASTFunction>())
        {
            if (!function->arguments)
                return;

            auto & arguments = function->arguments->children;
            size_t size = arguments.size();
            /// The right side of `IN` may be a table function, whose arguments are not resolved this way.
            if (isNameOfInFunction(function->name) && size >= 2)
            {
                const auto * right_function = arguments[1]->as<ASTFunction>();
                if (right_function && !FunctionFactory::instance().has(right_function->name)
                    && TableFunctionFactory::instance().isTableFunctionName(right_function->name))
                    size = 1;
            }

            for (size_t i = 0; i < size; ++i)
                rewrite(arguments[i]);
        }
        else if (ast->as<ASTAssignment>() || ast->as<ASTExpressionList>())
        {
            for (auto & child : ast->children)
                rewrite(child);
        }
    }

private:
    struct Scope
    {
        NameSet parameters;
        NameSet aliases;
    };

    const ColumnsDescription & table_columns;
    const NameSet & new_column_names;
    const NameSet & dropped_column_names;
    ContextPtr context;
    std::vector<Scope> bound_names;
    /// An alias is not visible inside its own expression: the alias and the index of its scope in `bound_names`.
    std::vector<std::pair<String, size_t>> aliases_in_definition;

    bool isBound(const String & name) const
    {
        if (new_column_names.contains(name))
            return true;
        if (!dropped_column_names.contains(name) && (table_columns.has(name) || isPrefixOfArrayColumn(name)))
            return true;

        for (size_t i = 0; i < bound_names.size(); ++i)
        {
            if (bound_names[i].parameters.contains(name))
                return true;
            if (bound_names[i].aliases.contains(name)
                && std::ranges::find(aliases_in_definition, std::pair{name, i}) == aliases_in_definition.end())
                return true;
        }
        return false;
    }

    /// As in the analyzer, `n` is a Nested only when a column `n.<...>` is an `Array`.
    bool isPrefixOfArrayColumn(const String & name) const
    {
        const String prefix = name + ".";
        for (const auto & column : table_columns)
            if (column.name.starts_with(prefix) && typeid_cast<const DataTypeArray *>(column.type.get()))
                return true;
        return false;
    }
};

/// Written without parentheses, `now`, `today` or `CURRENT_DATE` is a call of the function unless a column of the table,
/// a lambda parameter or an alias binds the name. Makes such calls explicit (`today` -> `today()`).
void makeNiladicFunctionCallsExplicit(
    ASTs & expressions,
    const ColumnsDescription & table_columns,
    const ContextPtr & context,
    const NameSet & new_column_names = {},
    const NameSet & dropped_column_names = {})
{
    NameSet aliases;
    for (const auto & expression : expressions)
        collectAliases(*expression, aliases);

    NiladicFunctionCallsMaker maker(table_columns, new_column_names, dropped_column_names, context, std::move(aliases));
    for (auto & expression : expressions)
        maker.rewrite(expression);
}

}

FirstNonDeterministicFunctionResult findFirstNonDeterministicFunction(
    const MutationCommand & command, ContextPtr context, const ColumnsDescription * table_columns)
{
    FirstNonDeterministicFunctionMatcher::Data finder_data{context, {}};

    switch (command.type)
    {
        case MutationCommand::UPDATE:
        {
            auto alter = command.ast();
            ASTs expressions{alter->update_assignments->clone(), alter->predicate->clone()};
            if (table_columns)
                makeNiladicFunctionCallsExplicit(expressions, *table_columns, context);

            FirstNonDeterministicFunctionFinder(finder_data).visit(expressions[0]);

            if (finder_data.result.nondeterministic_function_name)
                return finder_data.result;

            FirstNonDeterministicFunctionFinder(finder_data).visit(expressions[1]);
            return finder_data.result;
        }

        case MutationCommand::DELETE:
        {
            auto alter = command.ast();
            ASTs expressions{alter->predicate->clone()};
            if (table_columns)
                makeNiladicFunctionCallsExplicit(expressions, *table_columns, context);

            FirstNonDeterministicFunctionFinder(finder_data).visit(expressions[0]);
            return finder_data.result;
        }

        default:
            break;
    }

    return {};
}

ASTPtr replaceNonDeterministicToScalars(
    const ASTAlterCommand & alter_command,
    ContextPtr context,
    const ColumnsDescription * table_columns,
    const NameSet & new_column_names,
    const NameSet & dropped_column_names)
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
        if (table_columns)
        {
            ASTs expressions;
            for (IAST * expression : {new_alter_command.update_assignments, new_alter_command.predicate})
                if (expression)
                    expressions.push_back(expression->ptr());

            makeNiladicFunctionCallsExplicit(expressions, *table_columns, context, new_column_names, dropped_column_names);

            if (new_alter_command.predicate && expressions.back().get() != new_alter_command.predicate)
            {
                remove_child(new_alter_command.children, new_alter_command.predicate);
                new_alter_command.predicate = new_alter_command.children.emplace_back(expressions.back()).get();
            }
        }

        ExecuteNonDeterministicConstFunctionsVisitor::Data data{context, settings[Setting::mutations_max_literal_size_to_replace]};

        ExecuteNonDeterministicConstFunctionsVisitor visitor(data);
        visit(visitor);
    }

    return query;
}

}
