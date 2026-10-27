#include <Interpreters/getCustomKeyFilterForParallelReplicas.h>
#include <DataTypes/DataTypesNumber.h>

#include <Core/Settings.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/misc.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>
#include <Storages/ReplaceAliasByExpressionVisitor.h>
#include <Storages/extractKeyExpressionList.h>

#include <Common/checkStackSize.h>

#include <boost/rational.hpp>
#include <fmt/ranges.h>

#include <unordered_set>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int INVALID_SETTING_VALUE;
}

namespace
{

/// Collects the identifiers on the right-hand side of `IN` which name an argument of an enclosing lambda,
/// e.g. `z` in `arrayCount(z -> a IN z, arrs)`, or a field of such an argument, e.g. `z.v` in
/// `arrayCount(z -> a IN z.v, arrs)`. Such an identifier is an array, not a table.
void collectInLambdaArgumentIdentifiers(
    const IAST & ast, std::vector<String> & lambda_arguments, std::unordered_set<const IAST *> & result)
{
    checkStackSize();

    const auto * func = ast.as<ASTFunction>();
    if (func && func->name == "lambda" && func->arguments && func->arguments->children.size() == 2)
    {
        size_t num_outer_arguments = lambda_arguments.size();
        if (const auto * lambda_args_tuple = func->arguments->children[0]->as<ASTFunction>();
            lambda_args_tuple && lambda_args_tuple->arguments)
        {
            for (const auto & argument : lambda_args_tuple->arguments->children)
                if (const auto * argument_identifier = argument->as<ASTIdentifier>())
                    lambda_arguments.push_back(argument_identifier->name());
        }

        collectInLambdaArgumentIdentifiers(*func->arguments->children[1], lambda_arguments, result);
        lambda_arguments.resize(num_outer_arguments);
        return;
    }

    if (func && functionIsInOrGlobalInOperator(func->name) && func->arguments && func->arguments->children.size() == 2)
    {
        const auto & rhs = func->arguments->children[1];
        if (const auto * rhs_identifier = rhs->as<ASTIdentifier>(); rhs_identifier && !rhs->as<ASTTableIdentifier>()
            && std::find(lambda_arguments.begin(), lambda_arguments.end(), rhs_identifier->name_parts.front()) != lambda_arguments.end())
            result.insert(rhs.get());
    }

    for (const auto & child : ast.children)
        collectInLambdaArgumentIdentifiers(*child, lambda_arguments, result);
}

/// Whether an identifier on the right-hand side of `IN` in the custom key names a column of the table,
/// possibly qualified with the table name (`t.arr`) or with the database and table names (`db.t.arr`).
bool isColumnIdentifier(const ASTIdentifier & identifier, const ColumnsDescription & columns)
{
    if (columns.hasColumnOrSubcolumn(GetColumnsOptions::All, identifier.name()))
        return true;

    const auto & parts = identifier.name_parts;
    for (size_t num_qualifiers = 1; num_qualifiers <= 2 && num_qualifiers < parts.size(); ++num_qualifiers)
    {
        String unqualified_name = fmt::format("{}", fmt::join(parts.begin() + num_qualifiers, parts.end(), "."));
        if (columns.hasColumnOrSubcolumn(GetColumnsOptions::All, unqualified_name))
            return true;
    }

    return false;
}

/// Drop the table qualifier (`t.a`) or the database and table qualifiers (`db.t.a`) from identifiers
/// that name an `ALIAS` column of the table, so that `ReplaceAliasByExpressionMatcher`, which matches
/// the identifier as written, expands them too. The planner resolves such an identifier to the alias
/// expression regardless of the qualifier.
void removeQualifiersFromAliasColumnIdentifiers(ASTPtr & ast, const ColumnsDescription & columns)
{
    checkStackSize();

    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        const auto & parts = identifier->name_parts;
        if (parts.size() < 2 || columns.hasColumnOrSubcolumn(GetColumnsOptions::All, identifier->name()))
            return;

        for (size_t num_qualifiers = 1; num_qualifiers <= 2 && num_qualifiers < parts.size(); ++num_qualifiers)
        {
            String unqualified_name = fmt::format("{}", fmt::join(parts.begin() + num_qualifiers, parts.end(), "."));
            if (columns.hasAlias(unqualified_name))
            {
                auto unqualified_identifier = make_intrusive<ASTIdentifier>(unqualified_name);
                unqualified_identifier->setAlias(identifier->tryGetAlias());
                ast = std::move(unqualified_identifier);
                return;
            }
        }
        return;
    }

    for (auto & child : ast->children)
    {
        auto * old_ptr = child.get();
        removeQualifiersFromAliasColumnIdentifiers(child, columns);
        if (child.get() != old_ptr)
            ast->updatePointerToChild(old_ptr, child.get());
    }
}

}

ASTPtr getCustomKeyFilterForParallelReplica(
    size_t replicas_count,
    size_t replica_num,
    ASTPtr custom_key_ast,
    ParallelReplicasCustomKeyFilter filter,
    const ColumnsDescription & columns,
    const ContextPtr & context)
{
    chassert(replicas_count > 1);
    chassert(filter.filter_type == ParallelReplicasMode::CUSTOM_KEY_SAMPLING || filter.filter_type == ParallelReplicasMode::CUSTOM_KEY_RANGE);

    /// `parseCustomKeyForTable` validates the key as written, but the key may reference an `ALIAS`
    /// column of the table, and the filter built below resolves it to the alias expression. Check the
    /// key again after alias replacement, so a subquery or a column matcher hidden in an `ALIAS` column
    /// is rejected too. The key is a setting of the current query, so there is no stored metadata to
    /// keep loading, unlike an index over such a column.
    {
        using ReplaceAliasToExprVisitor = InDepthNodeVisitor<ReplaceAliasByExpressionMatcher, true>;
        ASTPtr expanded_custom_key_ast = custom_key_ast->clone();
        removeQualifiersFromAliasColumnIdentifiers(expanded_custom_key_ast, columns);
        ReplaceAliasToExprVisitor::Data data{columns, {}};
        ReplaceAliasToExprVisitor{data}.visit(expanded_custom_key_ast);
        /// Unlike a DDL key, the setting is not rewritten by `AddDefaultDatabaseVisitor`, so `a IN arr`
        /// arrives with a plain identifier on the right-hand side. It is a table only if it is neither
        /// a column (possibly table-qualified, `a IN t.arr`) nor a lambda argument (`z -> a IN z`).
        std::vector<String> lambda_arguments;
        std::unordered_set<const IAST *> lambda_argument_identifiers;
        collectInLambdaArgumentIdentifiers(*expanded_custom_key_ast, lambda_arguments, lambda_argument_identifiers);
        checkExpressionDoesntContainSubqueries(
            *expanded_custom_key_ast,
            [&](const ASTIdentifier & identifier)
            { return lambda_argument_identifiers.contains(&identifier) || isColumnIdentifier(identifier, columns); });
        checkExpressionDoesntContainMatchers(*expanded_custom_key_ast);
    }

    if (filter.filter_type == ParallelReplicasMode::CUSTOM_KEY_SAMPLING)
    {
        // first we do modulo with replica count
        auto modulo_function = makeASTFunction("positiveModulo", custom_key_ast, make_intrusive<ASTLiteral>(replicas_count));

        /// then we compare result to the current replica number (offset)
        auto equals_function = makeASTOperator("equals", std::move(modulo_function), make_intrusive<ASTLiteral>(replica_num));

        return equals_function;
    }

    chassert(filter.filter_type == ParallelReplicasMode::CUSTOM_KEY_RANGE);

    KeyDescription custom_key_description = KeyDescription::getKeyFromAST(custom_key_ast, columns, {}, context);

    using RelativeSize = boost::rational<ASTSampleRatio::BigNum>;

    RelativeSize range_upper = RelativeSize(0);
    RelativeSize range_lower = RelativeSize(filter.range_lower);

    /// The custom key is an arbitrary expression, and it does not have to describe a single column.
    /// `tuple()` describes no columns at all, so the number of the types has to be checked before reading them.
    if (custom_key_description.data_types.size() != 1)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Invalid custom key expression: it describes {} columns. Must be one unsigned integer column",
            custom_key_description.data_types.size());

    DataTypePtr custom_key_column_type = custom_key_description.data_types[0];

    if (typeid_cast<const DataTypeUInt64 *>(custom_key_column_type.get()))
    {
        range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                             : RelativeSize(std::numeric_limits<UInt64>::max()) + RelativeSize(1);
        if (range_upper > RelativeSize(std::numeric_limits<UInt64>::max()) + RelativeSize(1))
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt64) max value",
                rational_cast<double>(range_upper));
    }
    else if (typeid_cast<const DataTypeUInt32 *>(custom_key_column_type.get()))
    {
        range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                             : RelativeSize(std::numeric_limits<UInt32>::max()) + RelativeSize(1);
        if (range_upper > RelativeSize(std::numeric_limits<UInt32>::max()) + RelativeSize(1))
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt32) max value",
                rational_cast<double>(range_upper));
    }
    else if (typeid_cast<const DataTypeUInt16 *>(custom_key_column_type.get()))
    {
        range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                             : RelativeSize(std::numeric_limits<UInt16>::max()) + RelativeSize(1);
        if (range_upper > RelativeSize(std::numeric_limits<UInt16>::max()) + RelativeSize(1))
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt16) max value",
                rational_cast<double>(range_upper));
    }
    else if (typeid_cast<const DataTypeUInt8 *>(custom_key_column_type.get()))
    {
        range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                             : RelativeSize(std::numeric_limits<UInt8>::max()) + RelativeSize(1);
        if (range_upper > RelativeSize(std::numeric_limits<UInt8>::max()) + RelativeSize(1))
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt8) max value",
                rational_cast<double>(range_upper));
    }

    if (range_upper == RelativeSize(0))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Invalid custom key column type: {}. Must be one unsigned integer type",
            custom_key_column_type->getName());

    if (range_lower >= range_upper)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Invalid custom key filter range: Range upper bound {} must be larger than range lower bound {}",
            rational_cast<double>(range_lower),
            rational_cast<double>(range_upper));

    RelativeSize size_of_universum = range_upper - range_lower;

    if (size_of_universum <= RelativeSize(replicas_count))
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE, "Invalid custom key filter range: Range must be larger than than the number of replicas");

    RelativeSize relative_range_size = RelativeSize(1) / replicas_count;
    RelativeSize relative_range_offset = relative_range_size * RelativeSize(replica_num);

    /// Calculate the half-interval of `[lower, upper)` column values.
    bool has_lower_limit = false;
    bool has_upper_limit = false;

    RelativeSize lower_limit_rational = range_lower + relative_range_offset * size_of_universum;
    RelativeSize upper_limit_rational = range_lower + (relative_range_offset + relative_range_size) * size_of_universum;

    UInt64 lower = static_cast<UInt64>(boost::rational_cast<ASTSampleRatio::BigNum>(lower_limit_rational));
    UInt64 upper = static_cast<UInt64>(boost::rational_cast<ASTSampleRatio::BigNum>(upper_limit_rational));

    if (lower_limit_rational > range_lower)
        has_lower_limit = true;

    if (upper_limit_rational < range_upper)
        has_upper_limit = true;

    chassert(has_lower_limit || has_upper_limit);

    /// Let's add the conditions to cut off something else when the index is scanned again and when the request is processed.
    boost::intrusive_ptr<ASTFunction> lower_function;
    boost::intrusive_ptr<ASTFunction> upper_function;

    if (has_lower_limit)
    {
        lower_function = makeASTOperator("greaterOrEquals", custom_key_ast, make_intrusive<ASTLiteral>(lower));

        if (!has_upper_limit)
            return lower_function;
    }

    if (has_upper_limit)
    {
        upper_function = makeASTOperator("less", custom_key_ast, make_intrusive<ASTLiteral>(upper));

        if (!has_lower_limit)
            return upper_function;
    }

    chassert(upper_function && lower_function);

    return makeASTOperator("and", std::move(lower_function), std::move(upper_function));
}

ASTPtr parseCustomKeyForTable(const String & custom_key, const Context & context)
{
    /// The callers get here only when the custom key filtering is requested. Every replica reads only the part of
    /// the data its filter selects, so without the key every replica would read everything and the result would be
    /// multiplied by the number of the replicas. Fail instead, the same way for every caller.
    if (custom_key.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The custom key filtering for the parallel replicas is requested (setting 'parallel_replicas_mode'), "
            "but the custom key itself is not set (setting 'parallel_replicas_custom_key')");

    /// Try to parse expression
    ParserExpression parser;
    const auto & settings = context.getSettingsRef();
    ASTPtr custom_key_ast = parseQuery(
        parser,
        custom_key.data(),
        custom_key.data() + custom_key.size(),
        "parallel replicas custom key",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    /// The DDL interpreters inline SQL UDFs before table metadata is built, but the custom key
    /// arrives as a setting string parsed right here, so inline UDFs explicitly. Otherwise a UDF
    /// body could hide a subquery or a column matcher from the key-expression validation.
    ///
    /// Validate right here as well, rather than only in `KeyDescription::getKeyFromAST`: the callers
    /// analyze the key over the table (e.g. `buildFilterQueryTree` for the access check) before a
    /// `custom_key_range` key reaches `getKeyFromAST`, and a `custom_key_sampling` key never does.
    if (custom_key_ast)
    {
        UserDefinedSQLFunctionVisitor::visit(custom_key_ast, context.shared_from_this());
        /// The columns of the table are not known here, so an identifier on the right-hand side of `IN`
        /// may be an array column (`a IN arr`). `getCustomKeyFilterForParallelReplica` checks it against
        /// the columns before the key is used to filter the data.
        checkExpressionDoesntContainSubqueries(*custom_key_ast, [](const ASTIdentifier &) { return true; });
        checkExpressionDoesntContainMatchers(*custom_key_ast);
    }

    return custom_key_ast;
}

}
