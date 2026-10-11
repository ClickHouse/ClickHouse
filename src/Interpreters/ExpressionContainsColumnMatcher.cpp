#include <Interpreters/ExpressionContainsColumnMatcher.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>

#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>

#include <base/types.h>

namespace DB
{

namespace
{

ASTPtr * getMatcherQualifierPtr(IAST & ast)
{
    if (auto * matcher = ast.as<ASTQualifiedAsterisk>())
        return &matcher->qualifier;
    if (auto * matcher = ast.as<ASTQualifiedColumnsRegexpMatcher>())
        return &matcher->qualifier;
    if (auto * matcher = ast.as<ASTQualifiedColumnsListMatcher>())
        return &matcher->qualifier;
    return nullptr;
}

const IAST * getMatcherQualifier(const IAST & ast)
{
    if (const auto * matcher = ast.as<ASTQualifiedAsterisk>())
        return matcher->qualifier.get();
    if (const auto * matcher = ast.as<ASTQualifiedColumnsRegexpMatcher>())
        return matcher->qualifier.get();
    if (const auto * matcher = ast.as<ASTQualifiedColumnsListMatcher>())
        return matcher->qualifier.get();
    return nullptr;
}

bool isColumnMatcher(const IAST & ast, const IsColumnQualifier & is_column_qualifier)
{
    if (ast.as<ASTAsterisk>() || ast.as<ASTColumnsRegexpMatcher>() || ast.as<ASTColumnsListMatcher>())
        return true;

    const auto * qualifier = getMatcherQualifier(ast);
    if (!qualifier)
        return false;

    const auto * qualifier_identifier = qualifier->as<ASTIdentifier>();
    return !qualifier_identifier || !is_column_qualifier(*qualifier_identifier);
}

using ParameterToArgument = UnorderedMapWithMemoryTracking<String, ASTPtr>;

/// The parameters and the body of a lambda `(x, y) -> body`, or nullptrs if `function` is not a lambda.
std::pair<const ASTs *, ASTPtr *> getLambdaParametersAndBody(ASTFunction & function)
{
    if (function.name != "lambda" || !function.arguments || function.arguments->children.size() != 2)
        return {};
    const auto * parameters = function.arguments->children[0]->as<ASTFunction>();
    if (!parameters || parameters->name != "tuple" || !parameters->arguments)
        return {};
    return {&parameters->arguments->children, &function.arguments->children[1]};
}

/// The argument substituted for an identifier that is a parameter, e.g. `tup` for `x`, or a subcolumn of a parameter,
/// e.g. `tup.inner` for `x.inner` when the argument is an identifier as well. Otherwise nullptr.
ASTPtr substituteIdentifier(const ASTIdentifier & identifier, const ParameterToArgument & parameter_to_argument)
{
    const auto & parts = identifier.name_parts;
    if (identifier.isParam() || parts.empty() || parts[0].empty())
        return nullptr;

    auto it = parameter_to_argument.find(parts[0]);
    if (it == parameter_to_argument.end())
        return nullptr;

    if (parts.size() == 1)
        return it->second->clone();

    const auto * argument_identifier = it->second->as<ASTIdentifier>();
    if (!argument_identifier || argument_identifier->isParam())
        return nullptr;

    std::vector<String> new_parts = argument_identifier->name_parts;
    new_parts.insert(new_parts.end(), parts.begin() + 1, parts.end());
    return make_intrusive<ASTIdentifier>(std::move(new_parts));
}

/// Replaces the parameters of a SQL UDF with the arguments of its call, like the analyzer does when it inlines the UDF,
/// so that e.g. the qualifier `x` of `x.*` becomes the column `tup` for the call `f(tup)`. A nested lambda hides
/// the parameters that it redefines.
void substituteParameters(ASTPtr & ast, const ParameterToArgument & parameter_to_argument)
{
    if (parameter_to_argument.empty() || ast->as<ASTSelectQuery>())
        return;

    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        if (auto replacement = substituteIdentifier(*identifier, parameter_to_argument))
        {
            if (auto alias = ast->tryGetAlias(); !alias.empty())
                replacement->setAlias(alias);
            ast = std::move(replacement);
        }
        return;
    }

    if (auto * function = ast->as<ASTFunction>())
    {
        if (auto [lambda_parameters, lambda_body] = getLambdaParametersAndBody(*function); lambda_body)
        {
            auto inner_parameter_to_argument = parameter_to_argument;
            for (const auto & lambda_parameter : *lambda_parameters)
                if (const auto * lambda_parameter_identifier = lambda_parameter->as<ASTIdentifier>())
                    inner_parameter_to_argument.erase(lambda_parameter_identifier->name());

            const auto * old_ptr = lambda_body->get();
            substituteParameters(*lambda_body, inner_parameter_to_argument);
            if (lambda_body->get() != old_ptr)
                function->arguments->updatePointerToChild(old_ptr, *lambda_body);
            return;
        }
    }

    /// The qualified matchers do not expose their `qualifier` member to `updatePointerToChild`, so it is updated here.
    ASTPtr * qualifier = getMatcherQualifierPtr(*ast);
    for (auto & child : ast->children)
    {
        const auto * old_ptr = child.get();
        substituteParameters(child, parameter_to_argument);
        if (child.get() != old_ptr)
        {
            ast->updatePointerToChild(old_ptr, child);
            if (qualifier && qualifier->get() == old_ptr)
                *qualifier = child;
        }
    }
}

/// The body of the SQL UDF `udf` with its parameters replaced by the arguments of the call `function`,
/// or nullptr if `udf` is not a SQL UDF or does not accept these arguments.
ASTPtr getSubstitutedUDFBody(const ASTFunction & function, const IAST & udf)
{
    const auto * create_function_query = udf.as<ASTCreateSQLFunctionQuery>();
    if (!create_function_query || !create_function_query->function_core)
        return nullptr;

    auto lambda = create_function_query->function_core->clone();
    auto [parameters, body] = getLambdaParametersAndBody(*lambda->as<ASTFunction>());
    if (!body)
        return nullptr;

    const ASTs no_arguments;
    const auto & arguments = function.arguments ? function.arguments->children : no_arguments;
    if (arguments.size() != parameters->size())
        return nullptr;

    ParameterToArgument parameter_to_argument;
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const auto * parameter = (*parameters)[i]->as<ASTIdentifier>();
        if (!parameter)
            return nullptr;
        parameter_to_argument.emplace(parameter->name(), arguments[i]);
    }

    ASTPtr result = *body;
    substituteParameters(result, parameter_to_argument);
    return result;
}

ASTPtr findColumnMatcherInExpressionImpl(
    const ASTPtr & ast, const IsColumnQualifier & is_column_qualifier, UnorderedSetWithMemoryTracking<String> & udfs_in_progress)
{
    if (isColumnMatcher(*ast, is_column_qualifier))
        return ast;

    if (const auto * function = ast->as<ASTFunction>())
    {
        /// The body is walked with the arguments of the call substituted, because a parameter may be the qualifier
        /// of a matcher, e.g. `x.*` in `(x) -> greatest(x.*)` is a `Tuple` expansion for the call `f(tup)`.
        /// A UDF that is already being walked is skipped, so that a cycle among them cannot make this recurse forever.
        auto udf = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
        if (udf && udfs_in_progress.insert(function->name).second)
        {
            auto substituted_body = getSubstitutedUDFBody(*function, *udf);
            auto matcher = findColumnMatcherInExpressionImpl(substituted_body ? substituted_body : udf, is_column_qualifier, udfs_in_progress);
            udfs_in_progress.erase(function->name);
            if (matcher)
                return matcher;
        }
    }

    for (const auto & child : ast->children)
    {
        if (child->as<ASTSelectQuery>())
            continue;
        if (auto matcher = findColumnMatcherInExpressionImpl(child, is_column_qualifier, udfs_in_progress))
            return matcher;
    }

    return nullptr;
}

}

ASTPtr findColumnMatcherInExpression(const ASTPtr & ast, const IsColumnQualifier & is_column_qualifier)
{
    UnorderedSetWithMemoryTracking<String> udfs_in_progress;
    return findColumnMatcherInExpressionImpl(ast, is_column_qualifier, udfs_in_progress);
}

IsColumnQualifier makeTupleColumnQualifierCheck(
    std::function<std::optional<NameAndTypePair>(const String &)> try_get_column_,
    NameSet table_names_,
    std::optional<std::pair<String, String>> database_and_table_name_,
    bool prefer_table_over_column)
{
    return [try_get_column = std::move(try_get_column_),
            table_names = std::move(table_names_),
            database_and_table_name = std::move(database_and_table_name_),
            prefer_table_over_column](const ASTIdentifier & qualifier)
    {
        /// The parts are compared one by one rather than the flattened name, because a quoted part may contain a dot,
        /// e.g. `tab.with.dots`.tup is a single-part table prefix followed by a column.
        const auto & parts = qualifier.name_parts;
        if (parts.empty())
            return false;

        auto join_parts = [&](size_t start)
        {
            String result;
            for (size_t i = start; i < parts.size(); ++i)
            {
                if (i != start)
                    result += '.';
                result += parts[i];
            }
            return result;
        };

        bool has_single_part_prefix = table_names.contains(parts[0]);
        bool has_database_and_table_prefix = database_and_table_name && parts.size() >= 2
            && parts[0] == database_and_table_name->first && parts[1] == database_and_table_name->second;

        if (prefer_table_over_column
            && ((parts.size() == 1 && has_single_part_prefix) || (parts.size() == 2 && has_database_and_table_prefix)))
            return false;

        auto try_get_column_without_single_part_prefix = [&]() -> std::optional<NameAndTypePair>
        {
            if (parts.size() > 1 && has_single_part_prefix)
                return try_get_column(join_parts(1));
            return {};
        };

        auto try_get_column_without_database_and_table_prefix = [&]() -> std::optional<NameAndTypePair>
        {
            if (parts.size() > 2 && has_database_and_table_prefix)
                return try_get_column(join_parts(2));
            return {};
        };

        /// The qualifier is resolved as an identifier in the scope of the table, so it may be the name of a column,
        /// e.g. `tup` or a nested `tup.inner`, or the name of a column qualified with the table, e.g. `t.tup`.
        /// Like `IdentifierResolver::tryResolveIdentifierFromTableExpression`, with
        /// `analyzer_compatibility_prefer_alias_over_subcolumn` a qualifier whose first part is the table name or
        /// the alias, e.g. `t.a`, is looked up without that part first, and only then as a whole.
        /// Like `IdentifierResolver`, a single-part prefix (the table name or the alias) is always tried before
        /// a `database.table` prefix, e.g. for `db.tbl.tup.*` over `db.tbl AS db` the column `tbl.tup` is tried
        /// before `tup`.
        std::optional<NameAndTypePair> column;
        if (prefer_table_over_column)
            column = try_get_column_without_single_part_prefix();
        if (!column)
            column = try_get_column(join_parts(0));
        if (!column && !prefer_table_over_column)
            column = try_get_column_without_single_part_prefix();
        if (!column)
            column = try_get_column_without_database_and_table_prefix();
        if (!column)
            return false;

        DataTypePtr type = column->type;
        while (true)
        {
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
                type = nullable_type->getNestedType();
            else if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
                type = array_type->getNestedType();
            else if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
                type = map_type->getNestedType();
            else
                break;
        }

        return typeid_cast<const DataTypeTuple *>(type.get()) != nullptr;
    };
}

}
