#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Planner/PlannerContext.h>
#include <Planner/CollectTableExpressionData.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/applyColumnsTransformer.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/QueryConstructionSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProjectionMetadataValidation.h>
#include <Interpreters/DDLTask.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/StorageView.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageDummy.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSQLSecurity.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <Common/StringUtils.h>

#include <Poco/String.h>

#include <functional>
#include <iterator>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <re2/re2.h>

#if CLICKHOUSE_CLOUD
#include <Interpreters/SharedDatabaseCatalog.h>
#endif

namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_json_lazy_type_hints;
    extern const SettingsBool allow_metadata_only_named_tuple_alter;
    extern const SettingsBool allow_statistics;
    extern const SettingsBool allow_suspicious_ttl_expressions;
    extern const SettingsBool flatten_nested;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_STATISTICS;
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int NO_SUCH_PROJECTION_IN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool share_nested_offsets;
    extern const MergeTreeSettingsBool add_minmax_index_for_numeric_columns;
    extern const MergeTreeSettingsBool add_minmax_index_for_string_columns;
    extern const MergeTreeSettingsBool add_minmax_index_for_temporal_columns;
    extern const MergeTreeSettingsBool add_minmax_index_for_block_number_column;
    extern const MergeTreeSettingsBool add_minmax_index_for_block_offset_column;
    extern const MergeTreeSettingsBool enable_block_number_column;
    extern const MergeTreeSettingsBool enable_block_offset_column;
}

namespace
{

using ProjectionAliases = std::unordered_multimap<String, const IAST *>;

/// An unavailable projection cannot be analyzed here, but its stored aliases can still be followed.
ProjectionAliases getProjectionAliases(const ASTProjectionDeclaration & declaration)
{
    ProjectionAliases aliases;
    const auto * query = declaration.query ? declaration.query->as<ASTProjectionSelectQuery>() : nullptr;
    if (!query)
        return aliases;

    std::function<void(const IAST &)> collect = [&](const IAST & expression)
    {
        if (const auto alias = expression.tryGetAlias(); !alias.empty())
            aliases.emplace(alias, &expression);
        for (const auto & child : expression.children)
            collect(*child);
    };
    collect(*query);
    return aliases;
}

bool isUnqualifiedProjectionMatcher(const IAST & ast);
bool countFunctionIgnoresMatchers(const ASTFunction & function);

/// The analyzer discards direct unqualified matcher arguments of safe count variants. Every
/// unavailable-projection dependency check must skip those same arguments.
template <typename Predicate>
bool anyRelevantProjectionChild(const IAST & ast, Predicate && predicate)
{
    const auto * function = ast.as<ASTFunction>();
    const bool ignore_matchers = function && function->arguments && countFunctionIgnoresMatchers(*function);
    for (const auto & child : ast.children)
    {
        if (ignore_matchers && child.get() == function->arguments.get())
        {
            for (const auto & argument : function->arguments->children)
                if (!isUnqualifiedProjectionMatcher(*argument) && predicate(*argument))
                    return true;
        }
        else if (predicate(*child))
            return true;
    }
    return false;
}

bool projectionQueryReferencesColumn(
    const IAST & ast,
    const String & column_name,
    const ColumnsDescription & columns,
    const ProjectionAliases & aliases,
    bool expand_table_aliases,
    std::unordered_set<String> & expanded_aliases,
    const std::unordered_set<String> & lambda_arguments)
{
    /// Removing a column from `SELECT *` removes that output too; it does not leave a broken
    /// identifier in the stored query. Explicitly declared outputs are checked separately.
    if (const auto * asterisk = ast.as<ASTAsterisk>())
        return asterisk->transformers
            && projectionQueryReferencesColumn(
                *asterisk->transformers, column_name, columns, aliases, expand_table_aliases, expanded_aliases, lambda_arguments);
    if (const auto * asterisk = ast.as<ASTQualifiedAsterisk>())
        return asterisk->transformers
            && projectionQueryReferencesColumn(
                *asterisk->transformers, column_name, columns, aliases, expand_table_aliases, expanded_aliases, lambda_arguments);
    if (const auto * except = ast.as<ASTColumnsExceptTransformer>())
    {
        /// A non-strict `EXCEPT` name can disappear; a strict one must still resolve.
        if (!except->is_strict)
            return false;
    }
    if (const auto * replace = ast.as<ASTColumnsReplaceTransformer>(); replace && replace->is_strict)
    {
        for (const auto & replacement : replace->children)
            if (const auto * item = replacement->as<ASTColumnsReplaceTransformer::Replacement>();
                item && item->name == column_name)
                return true;
    }
    if (const auto * apply = ast.as<ASTColumnsApplyTransformer>())
    {
        return (apply->parameters
                && projectionQueryReferencesColumn(
                    *apply->parameters, column_name, columns, aliases, expand_table_aliases, expanded_aliases, lambda_arguments))
            || (apply->lambda
                && projectionQueryReferencesColumn(
                    *apply->lambda, column_name, columns, aliases, expand_table_aliases, expanded_aliases, lambda_arguments));
    }

    if (const auto * function = ast.as<ASTFunction>(); function && function->name == "lambda"
        && function->arguments && function->arguments->children.size() == 2)
    {
        auto local_arguments = lambda_arguments;
        if (const auto * tuple = function->arguments->children.front()->as<ASTFunction>(); tuple && tuple->arguments)
        {
            for (const auto & argument : tuple->arguments->children)
                if (const auto * identifier = argument->as<ASTIdentifier>())
                    local_arguments.insert(identifier->name());
        }
        return projectionQueryReferencesColumn(
            *function->arguments->children.back(), column_name, columns, aliases,
            expand_table_aliases, expanded_aliases, local_arguments);
    }

    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        for (const auto & argument : lambda_arguments)
            if (identifier->name() == argument || identifier->name().starts_with(argument + "."))
                return false;
        if (identifier->name() == column_name || identifier->name().starts_with(column_name + "."))
            return true;
    }

    String alias_name;
    if (const auto * identifier = ast.as<ASTIdentifier>())
        alias_name = identifier->name();

    if (!alias_name.empty() && expanded_aliases.insert(alias_name).second)
    {
        auto [begin, end] = aliases.equal_range(alias_name);
        for (auto it = begin; it != end; ++it)
            if (projectionQueryReferencesColumn(*it->second, column_name, columns, aliases, expand_table_aliases, expanded_aliases, lambda_arguments))
                return true;

        if (expand_table_aliases && ast.as<ASTIdentifier>())
        {
            if (const auto * alias_column = columns.tryGet(alias_name);
                alias_column && alias_column->default_desc.kind == ColumnDefaultKind::Alias
                    && alias_column->default_desc.expression
                    && projectionQueryReferencesColumn(
                        *alias_column->default_desc.expression, column_name, columns, aliases, expand_table_aliases, expanded_aliases, lambda_arguments))
                return true;
        }

        expanded_aliases.erase(alias_name);
    }

    return anyRelevantProjectionChild(ast, [&](const IAST & child)
    {
        return projectionQueryReferencesColumn(
            child, column_name, columns, aliases, expand_table_aliases, expanded_aliases, lambda_arguments);
    });
}

bool projectionQueryReferencesColumn(
    const IAST & ast,
    const String & column_name,
    const ColumnsDescription & columns,
    const ProjectionAliases & aliases,
    bool expand_table_aliases);

/// Expand the same matcher transformers used by query analysis before comparing dependencies
/// or the argument shape of an enclosing expression.
std::optional<ASTs> expandProjectionMatcher(const IAST & expression, const ColumnsDescription & columns)
{
    ASTs outputs;
    const IAST * transformers = nullptr;
    if (const auto * asterisk = expression.as<ASTAsterisk>())
        transformers = asterisk->transformers.get();
    else if (const auto * qualified_asterisk = expression.as<ASTQualifiedAsterisk>())
        transformers = qualified_asterisk->transformers.get();
    else if (const auto * regexp = expression.as<ASTColumnsRegexpMatcher>())
        transformers = regexp->transformers.get();
    else if (const auto * qualified_regexp = expression.as<ASTQualifiedColumnsRegexpMatcher>())
        transformers = qualified_regexp->transformers.get();
    else if (const auto * list = expression.as<ASTColumnsListMatcher>())
        transformers = list->transformers.get();
    else if (const auto * qualified_list = expression.as<ASTQualifiedColumnsListMatcher>())
        transformers = qualified_list->transformers.get();
    else
        return std::nullopt;

    const String * pattern = nullptr;
    if (const auto * regexp = expression.as<ASTColumnsRegexpMatcher>())
        pattern = &regexp->getPattern();
    else if (const auto * qualified_regexp = expression.as<ASTQualifiedColumnsRegexpMatcher>())
        pattern = &qualified_regexp->getPattern();

    const IAST * column_list = nullptr;
    if (const auto * list = expression.as<ASTColumnsListMatcher>())
        column_list = list->column_list.get();
    else if (const auto * qualified_list = expression.as<ASTQualifiedColumnsListMatcher>())
        column_list = qualified_list->column_list.get();

    if (column_list)
    {
        for (const auto & child : column_list->children)
            outputs.push_back(child->clone());
    }
    else
    {
        for (const auto & column : columns.getAll())
            if (!pattern || re2::RE2::PartialMatch(column.name, *pattern))
                outputs.push_back(make_intrusive<ASTIdentifier>(column.name));
    }

    if (transformers)
        for (const auto & child : transformers->children)
            applyColumnsTransformer(child, outputs);

    return outputs;
}

bool projectionMatcherTypedOutputDependsOn(
    const IAST & expression, const String & declared_column, const String & source_column,
    const ColumnsDescription & columns, const ProjectionAliases & aliases)
{
    const auto outputs = expandProjectionMatcher(expression, columns);
    if (!outputs)
        return false;

    for (const auto & output : *outputs)
        if ((declared_column.empty() || output->getAliasOrColumnName() == declared_column)
            && projectionQueryReferencesColumn(
                *output, source_column, columns, aliases, /*expand_table_aliases=*/true))
                return true;
    return false;
}

bool isProjectionExpandableMatcher(const IAST & ast)
{
    return ast.as<ASTColumnsRegexpMatcher>() || ast.as<ASTQualifiedColumnsRegexpMatcher>()
        || ast.as<ASTColumnsListMatcher>() || ast.as<ASTQualifiedColumnsListMatcher>()
        || ast.as<ASTAsterisk>() || ast.as<ASTQualifiedAsterisk>();
}

bool isUnqualifiedProjectionMatcher(const IAST & ast)
{
    return ast.as<ASTAsterisk>() || ast.as<ASTColumnsRegexpMatcher>() || ast.as<ASTColumnsListMatcher>();
}

/// The analyzer removes unqualified matcher arguments from safe count variants, so they do not
/// depend on the table's column list. Keep this in step with resolveFunction's combinator check.
bool countFunctionIgnoresMatchers(const ASTFunction & function)
{
    if (function.name.size() < 5 || !equalsCaseInsensitive(std::string_view(function.name).substr(0, 5), "count")
        || !function.arguments
        || std::ranges::none_of(function.arguments->children, [](const auto & argument)
            { return isUnqualifiedProjectionMatcher(*argument); }))
        return false;

    String base_name = function.name;
    while (auto combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(base_name))
    {
        if (combinator->transformsArgumentTypes())
            return false;
        base_name.resize(base_name.size() - combinator->getName().size());
    }

    const auto base_lower = Poco::toLower(base_name);
    const auto name_lower = Poco::toLower(function.name);
    return (base_lower == "count" || base_lower == "countstate")
        && name_lower.starts_with(base_lower) && name_lower != "countdistinct";
}

/// Projection output names are derived after matcher expansion and ignore SELECT aliases.
/// Expand a clone so a declaration such as `plus(a, b)` can be matched to `plus(COLUMNS(...)) AS x`.
void expandProjectionMatchersForName(ASTPtr & ast, const ColumnsDescription & columns)
{
    if (auto * function = ast->as<ASTFunction>(); function && function->arguments)
    {
        ASTs arguments;
        const bool ignore_matchers = countFunctionIgnoresMatchers(*function);
        for (auto & argument : function->arguments->children)
        {
            if (ignore_matchers && isUnqualifiedProjectionMatcher(*argument))
                continue;

            if (auto outputs = expandProjectionMatcher(*argument, columns))
            {
                for (auto & output : *outputs)
                {
                    expandProjectionMatchersForName(output, columns);
                    arguments.push_back(std::move(output));
                }
            }
            else
            {
                expandProjectionMatchersForName(argument, columns);
                arguments.push_back(argument);
            }
        }
        function->arguments->children = std::move(arguments);
        for (auto & child : ast->children)
            if (child.get() != function->arguments.get())
                expandProjectionMatchersForName(child, columns);
        return;
    }

    for (auto & child : ast->children)
        expandProjectionMatchersForName(child, columns);
}

String expandedProjectionExpressionName(const ASTPtr & expression, const ColumnsDescription & columns)
{
    auto expanded = expression->clone();
    expandProjectionMatchersForName(expanded, columns);
    return expanded->getColumnName();
}

/// Unlike a top-level matcher, a matcher inside a function changes that function's arguments
/// when a source column disappears. A remaining match is not enough to keep the expression valid.
bool projectionNestedMatcherReferencesColumn(
    const IAST & ast, const String & source_column, const ColumnsDescription & columns,
    const ProjectionAliases & aliases, bool inside_function, std::unordered_set<String> & expanded_aliases)
{
    if (inside_function && isProjectionExpandableMatcher(ast) && projectionMatcherTypedOutputDependsOn(
            ast, /*declared_column=*/"", source_column, columns, aliases))
        return true;

    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        const auto & name = identifier->name();
        if (expanded_aliases.insert(name).second)
        {
            auto [begin, end] = aliases.equal_range(name);
            for (auto it = begin; it != end; ++it)
                if (projectionNestedMatcherReferencesColumn(
                        *it->second, source_column, columns, aliases, inside_function, expanded_aliases))
                    return true;
            expanded_aliases.erase(name);
        }
    }

    const bool child_inside_function = inside_function || ast.as<ASTFunction>();
    return anyRelevantProjectionChild(ast, [&](const IAST & child)
    {
        return projectionNestedMatcherReferencesColumn(
            child, source_column, columns, aliases, child_inside_function, expanded_aliases);
    });
}

bool projectionNestedMatcherReferencesColumn(
    const IAST & ast, const String & source_column, const ColumnsDescription & columns,
    const ProjectionAliases & aliases)
{
    std::unordered_set<String> expanded_aliases;
    return projectionNestedMatcherReferencesColumn(
        ast, source_column, columns, aliases, /*inside_function=*/false, expanded_aliases);
}

bool projectionNestedMatcherReferencesColumn(
    const ASTProjectionSelectQuery & query, const String & source_column, const ColumnsDescription & columns,
    const ProjectionAliases & aliases)
{
    std::unordered_set<String> expanded_aliases;
    for (const auto & expression : {query.select(), query.where(), query.groupBy(), query.orderBy()})
        if (expression && projectionNestedMatcherReferencesColumn(
                *expression, source_column, columns, aliases, /*inside_function=*/false, expanded_aliases))
            return true;
    return false;
}

/// A matcher inside a function supplies a variable number of arguments. Rebuilding an unavailable
/// projection after an ALTER must not silently change that function's argument expressions.
bool projectionNestedMatcherChangesShape(
    const IAST & ast, const ColumnsDescription & old_columns, const ColumnsDescription & new_columns,
    const ProjectionAliases & aliases, bool inside_function, std::unordered_set<String> & expanded_aliases)
{
    if (inside_function && isProjectionExpandableMatcher(ast))
    {
        const auto old_outputs = expandProjectionMatcher(ast, old_columns);
        if (old_outputs)
        {
            std::optional<ASTs> new_outputs;
            try
            {
                new_outputs = expandProjectionMatcher(ast, new_columns);
            }
            catch (const Exception &)
            {
                return true;
            }

            if (!new_outputs || old_outputs->size() != new_outputs->size())
                return true;
            for (size_t i = 0; i < old_outputs->size(); ++i)
                if ((*old_outputs)[i]->getTreeHash(/*ignore_aliases=*/false)
                    != (*new_outputs)[i]->getTreeHash(/*ignore_aliases=*/false))
                    return true;
        }
    }

    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        const auto & name = identifier->name();
        if (expanded_aliases.insert(name).second)
        {
            auto [begin, end] = aliases.equal_range(name);
            for (auto it = begin; it != end; ++it)
                if (projectionNestedMatcherChangesShape(
                        *it->second, old_columns, new_columns, aliases, inside_function, expanded_aliases))
                    return true;
            expanded_aliases.erase(name);
        }
    }

    const bool child_inside_function = inside_function || ast.as<ASTFunction>();
    return anyRelevantProjectionChild(ast, [&](const IAST & child)
    {
        return projectionNestedMatcherChangesShape(
            child, old_columns, new_columns, aliases, child_inside_function, expanded_aliases);
    });
}

bool projectionNestedMatcherChangesShape(
    const ASTProjectionSelectQuery & query,
    const ColumnsDescription & old_columns, const ColumnsDescription & new_columns,
    const ProjectionAliases & aliases)
{
    std::unordered_set<String> expanded_aliases;
    for (const auto & expression : {query.select(), query.where(), query.groupBy(), query.orderBy()})
        if (expression && projectionNestedMatcherChangesShape(
                *expression, old_columns, new_columns, aliases, /*inside_function=*/false, expanded_aliases))
            return true;
    return false;
}

/// A direct SELECT matcher may safely shed columns, but an existing positional GROUP BY must
/// still resolve to the same output expression after expansion with the new table schema.
/// Projection ORDER BY is a sorting-key expression: cloneToASTSelect() adds it to SELECT rather
/// than creating an ORDER BY clause. Its numeric literals are rejected as constant sorting keys,
/// not resolved as positions.
bool projectionGroupByPositionChangesOutput(
    const ASTProjectionDeclaration & declaration,
    const ColumnsDescription & old_columns, const ColumnsDescription & new_columns)
{
    const auto * query = declaration.query ? declaration.query->as<ASTProjectionSelectQuery>() : nullptr;
    if (!query || !query->select() || !query->groupBy())
        return false;

    auto select_outputs = [&](const ColumnsDescription & columns)
    {
        ASTs result;
        for (const auto & expression : query->select()->children)
        {
            if (auto outputs = expandProjectionMatcher(*expression, columns))
                result.insert(result.end(), outputs->begin(), outputs->end());
            else
                result.push_back(expression);
        }
        return result;
    };

    const auto old_outputs = select_outputs(old_columns);
    const auto new_outputs = select_outputs(new_columns);

    auto position_index = [](const Field & value, size_t output_count) -> std::optional<size_t>
    {
        if (value.getType() == Field::Types::UInt64)
        {
            const auto position = value.safeGet<UInt64>();
            if (position > 0 && position <= output_count)
                return position - 1;
        }
        else if (value.getType() == Field::Types::Int64)
        {
            const auto position = value.safeGet<Int64>();
            if (position > 0 && static_cast<UInt64>(position) <= output_count)
                return static_cast<size_t>(position - 1);
            if (position < 0)
            {
                const auto magnitude = static_cast<UInt64>(-(position + 1)) + 1;
                if (magnitude <= output_count)
                    return output_count - magnitude;
            }
        }
        return std::nullopt;
    };

    for (const auto & expression : query->groupBy()->children)
    {
        const auto * literal = expression->as<ASTLiteral>();
        if (!literal || !literal->tryGetAlias().empty())
            continue;

        const auto type = literal->value.getType();
        if (type != Field::Types::UInt64 && type != Field::Types::Int64)
            continue;

        const auto old_index = position_index(literal->value, old_outputs.size());
        const auto new_index = position_index(literal->value, new_outputs.size());
        if (!old_index || !new_index
            || old_outputs[*old_index]->getTreeHash(/*ignore_aliases=*/false)
                != new_outputs[*new_index]->getTreeHash(/*ignore_aliases=*/false))
            return true;
    }
    return false;
}

/// A direct ORDER BY matcher contributes its last expanded expression as the projection's sorting
/// key. Unlike a direct SELECT matcher, it cannot shed that expression while the projection is
/// unavailable: reanalyzing the stored declaration would silently change the key.
bool projectionOrderByMatcherChangesKey(
    const ASTProjectionDeclaration & declaration,
    const ColumnsDescription & old_columns, const ColumnsDescription & new_columns)
{
    const auto * query = declaration.query ? declaration.query->as<ASTProjectionSelectQuery>() : nullptr;
    if (!query || !query->orderBy())
        return false;

    const auto old_outputs = expandProjectionMatcher(*query->orderBy(), old_columns);
    if (!old_outputs)
        return false;

    const auto new_outputs = expandProjectionMatcher(*query->orderBy(), new_columns);
    return !new_outputs || old_outputs->empty() || new_outputs->empty()
        || old_outputs->back()->getTreeHash(/*ignore_aliases=*/false)
            != new_outputs->back()->getTreeHash(/*ignore_aliases=*/false);
}

bool projectionTupleElementReferencesSubcolumn(
    const IAST & ast, const String & source_column, const String & subcolumn_name,
    const ColumnsDescription & columns, const ColumnsDescription & new_columns,
    const ProjectionAliases & aliases)
{
    if (const auto * function = ast.as<ASTFunction>(); function && function->name == "tupleElement"
        && function->arguments && function->arguments->children.size() >= 2)
    {
        const auto & arguments = function->arguments->children;
        String field_name;
        if (const auto * field = arguments[1]->as<ASTLiteral>();
            field && projectionQueryReferencesColumn(
                *arguments[0], source_column, columns, aliases, /*expand_table_aliases=*/true))
        {
            if (field->value.tryGet<String>(field_name)
                && subcolumn_name == source_column + "." + field_name)
                return true;

            UInt64 index = 0;
            if (field->value.tryGet<UInt64>(index))
            {
                const auto * new_tuple = typeid_cast<const DataTypeTuple *>(new_columns.get(source_column).type.get());
                if (!new_tuple || index > new_tuple->getElements().size())
                    return true;
            }
        }
    }

    return anyRelevantProjectionChild(ast, [&](const IAST & child)
    {
        return projectionTupleElementReferencesSubcolumn(
            child, source_column, subcolumn_name, columns, new_columns, aliases);
    });
}

/// A dynamic regexp selection may lose columns, but it cannot become empty on recovery.
bool projectionMatcherBecomesEmpty(
    const IAST & ast, const String & removed_column, const ColumnsDescription & new_columns,
    const ProjectionAliases & aliases, std::unordered_set<String> & expanded_aliases)
{
    const String * pattern = nullptr;
    if (const auto * matcher = ast.as<ASTColumnsRegexpMatcher>())
        pattern = &matcher->getPattern();
    else if (const auto * qualified_matcher = ast.as<ASTQualifiedColumnsRegexpMatcher>())
        pattern = &qualified_matcher->getPattern();

    if (pattern && re2::RE2::PartialMatch(removed_column, *pattern))
    {
        const auto remaining_columns = new_columns.getAll();
        if (std::ranges::none_of(remaining_columns, [&](const auto & column)
            { return re2::RE2::PartialMatch(column.name, *pattern); }))
            return true;
    }

    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        const auto & name = identifier->name();
        if (expanded_aliases.insert(name).second)
        {
            auto [begin, end] = aliases.equal_range(name);
            for (auto it = begin; it != end; ++it)
                if (projectionMatcherBecomesEmpty(
                        *it->second, removed_column, new_columns, aliases, expanded_aliases))
                    return true;
            expanded_aliases.erase(name);
        }
    }

    return anyRelevantProjectionChild(ast, [&](const IAST & child)
    {
        return projectionMatcherBecomesEmpty(child, removed_column, new_columns, aliases, expanded_aliases);
    });
}

bool projectionMatcherBecomesEmpty(
    const ASTProjectionSelectQuery & query, const String & removed_column, const ColumnsDescription & new_columns,
    const ProjectionAliases & aliases)
{
    std::unordered_set<String> expanded_aliases;
    for (const auto & expression : {query.select(), query.where(), query.groupBy(), query.orderBy()})
        if (expression && projectionMatcherBecomesEmpty(
                *expression, removed_column, new_columns, aliases, expanded_aliases))
            return true;
    return false;
}

bool projectionQueryReferencesColumn(
    const IAST & ast,
    const String & column_name,
    const ColumnsDescription & columns,
    const ProjectionAliases & aliases,
    bool expand_table_aliases)
{
    std::unordered_set<String> expanded_aliases;
    std::unordered_set<String> lambda_arguments;
    return projectionQueryReferencesColumn(
        ast, column_name, columns, aliases, expand_table_aliases, expanded_aliases, lambda_arguments);
}

bool explicitProjectionColumnTypeDependsOn(
    const ASTProjectionDeclaration & declaration,
    const String & source_column,
    const ColumnsDescription & columns,
    const ProjectionAliases & aliases)
{
    if (!declaration.columns)
        return false;

    const auto * query = declaration.query->as<ASTProjectionSelectQuery>();
    if (!query || !query->select())
        return true;

    const auto & select_expressions = query->select()->children;
    for (const auto & child : declaration.columns->children)
    {
        const auto & declared_column = child->as<const ASTColumnDeclaration &>();
        if (!declared_column.getType())
            continue;

        bool matched_output = false;
        bool unmatched_nested_dependency = false;
        for (const auto & expression : select_expressions)
        {
            const auto outputs = expandProjectionMatcher(*expression, columns);
            if (outputs)
            {
                for (const auto & output : *outputs)
                    if (output->getAliasOrColumnName() == declared_column.name)
                        matched_output = true;
            }
            if (projectionMatcherTypedOutputDependsOn(
                    *expression, declared_column.name, source_column, columns, aliases))
                return true;

            const bool nested_dependency = projectionNestedMatcherReferencesColumn(
                *expression, source_column, columns, aliases);
            const bool expression_matches = expression->getAliasOrColumnName() == declared_column.name
                || expression->getColumnName() == declared_column.name
                || (nested_dependency
                    && expandedProjectionExpressionName(expression, columns) == declared_column.name);
            if (expression_matches)
            {
                matched_output = true;
                if (projectionQueryReferencesColumn(
                        *expression, source_column, columns, aliases, /*expand_table_aliases=*/true)
                    || nested_dependency)
                    return true;
            }
            else if (nested_dependency)
                unmatched_nested_dependency = true;

            /// A bare `WITH` or table `ALIAS` identifier can be stored under its source name.
            if (declared_column.name == source_column && expression->as<ASTIdentifier>()
                && expression->tryGetAlias().empty()
                && projectionQueryReferencesColumn(
                    *expression, source_column, columns, aliases, /*expand_table_aliases=*/true))
                return true;
        }

        if (!matched_output && unmatched_nested_dependency)
            return true;
    }

    return false;
}

/// Resolve a bare SELECT identifier through unambiguous query aliases. A table ALIAS keeps its
/// own declared type across source type changes, so stop there when resolving the output type;
/// follow its expression only when resolving the effective output name and dependencies.
std::optional<String> projectionOutputColumn(
    const IAST & expression,
    const ColumnsDescription & columns,
    const ProjectionAliases & aliases,
    bool expand_table_aliases,
    std::unordered_set<String> & visited)
{
    const auto * identifier = expression.as<ASTIdentifier>();
    if (!identifier)
        return std::nullopt;

    const String & name = identifier->name();
    if (!visited.insert(name).second)
        return std::nullopt;

    const auto [begin, end] = aliases.equal_range(name);
    const auto * column = columns.tryGet(name);
    const bool has_query_alias = begin != end;
    if (has_query_alias && (std::next(begin) != end || column))
        return std::nullopt;
    if (has_query_alias)
        return projectionOutputColumn(*begin->second, columns, aliases, expand_table_aliases, visited);
    if (!column)
        return std::nullopt;
    if (expand_table_aliases && column->default_desc.kind == ColumnDefaultKind::Alias)
    {
        if (!column->default_desc.expression)
            return std::nullopt;
        return projectionOutputColumn(*column->default_desc.expression, columns, aliases, expand_table_aliases, visited);
    }
    return name;
}

std::optional<String> projectionOutputColumn(
    const IAST & expression, const ColumnsDescription & columns, const ProjectionAliases & aliases, bool expand_table_aliases)
{
    std::unordered_set<String> visited;
    return projectionOutputColumn(expression, columns, aliases, expand_table_aliases, visited);
}

/// A codec is stored even when the projection cannot be analyzed. Recheck it against a changed
/// output type whenever that type can be proved from a bare SELECT identifier. Otherwise reject
/// a dependent type change rather than persist a declaration that may fail at the next startup.
void checkUnavailableProjectionCodecTypeChange(
    const ASTProjectionDeclaration & declaration,
    const String & changed_column,
    const ColumnsDescription & old_columns,
    const ColumnsDescription & new_columns,
    const ProjectionAliases & aliases)
{
    if (!declaration.columns)
        return;
    const auto * query = declaration.query ? declaration.query->as<ASTProjectionSelectQuery>() : nullptr;
    if (!query || !query->select())
        return;

    for (const auto & child : declaration.columns->children)
    {
        const auto & declared_column = child->as<const ASTColumnDeclaration &>();
        const auto codec_ast = declared_column.getCodec();
        if (!codec_ast || declared_column.getType())
            continue;

        size_t matched_outputs = 0;
        bool unmatched_output_depends_on_change = false;
        bool matched_output_depends_on_change = false;
        for (const auto & expression : query->select()->children)
        {
            const auto expanded = expandProjectionMatcher(*expression, old_columns);
            const auto & outputs = expanded ? *expanded : ASTs{expression};
            for (const auto & output : outputs)
            {
                const auto source = projectionOutputColumn(*output, old_columns, aliases, /*expand_table_aliases=*/true);
                const bool matches = output->getColumnName() == declared_column.name || (source && *source == declared_column.name)
                    || (!expanded && expandedProjectionExpressionName(output, old_columns) == declared_column.name);
                const bool depends_on_change
                    = projectionQueryReferencesColumn(*output, changed_column, old_columns, aliases, /*expand_table_aliases=*/true);
                if (!matches)
                {
                    unmatched_output_depends_on_change |= depends_on_change;
                    continue;
                }

                ++matched_outputs;
                if (!depends_on_change)
                    continue;

                matched_output_depends_on_change = true;

                const auto type_column = projectionOutputColumn(*output, old_columns, aliases, /*expand_table_aliases=*/false);
                if (!type_column || !new_columns.has(*type_column))
                    throw Exception(
                        ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Cannot change type of column {} because projection {} has a codec on a dependent SELECT expression whose new type "
                        "cannot be checked",
                        backQuote(changed_column),
                        backQuote(declaration.name));

                if (source && *source != *type_column)
                    throw Exception(
                        ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Cannot change type of column {} because projection {} has a codec on a table ALIAS output whose name may change",
                        backQuote(changed_column),
                        backQuote(declaration.name));

                const auto & new_type = new_columns.get(*type_column).type;
                try
                {
                    auto codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                        codec_ast, new_type, CodecValidationSettings::trusted());
                    if (isLossyCodecForType(codec, new_type))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "codec would be lossy for type {}", new_type->getName());
                }
                catch (const Exception & exception)
                {
                    throw Exception(
                        ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Cannot change type of column {} because projection {} has an incompatible codec on column {}: {}",
                        backQuote(changed_column),
                        backQuote(declaration.name),
                        backQuote(declared_column.name),
                        exception.message());
                }
            }
        }
        if (matched_outputs > 1 && matched_output_depends_on_change)
            throw Exception(
                ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                "Cannot change type of column {} because projection {} has an ambiguous codec output {}",
                backQuote(changed_column),
                backQuote(declaration.name),
                backQuote(declared_column.name));
        if (!matched_outputs && unmatched_output_depends_on_change)
            throw Exception(
                ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                "Cannot change type of column {} because projection {} has a codec whose SELECT output cannot be identified",
                backQuote(changed_column),
                backQuote(declaration.name));
    }
}

/// Whether the two names name one setting: a `MergeTree` setting can have two names.
bool isSameSetting(const String & left, const String & right)
{
    auto resolve = [](const String & name)
    { return MergeTreeSettings::hasBuiltin(name) ? MergeTreeSettings::resolveName(name) : std::string_view(name); };
    return resolve(left) == resolve(right);
}

/// Removes the settings with the given names from the `SETTINGS` clause of a table definition.
void resetSettings(SettingsChanges & settings_from_storage, const std::set<String> & settings_resets)
{
    for (const auto & setting_name : settings_resets)
    {
        auto same_setting = [&setting_name](const SettingChange & c) { return isSameSetting(c.name, setting_name); };
        auto it = std::remove_if(settings_from_storage.begin(), settings_from_storage.end(), same_setting);

        if (it != settings_from_storage.end())
        {
            settings_from_storage.erase(it, settings_from_storage.end());
        }
        else
        {
            /// Intentionally ignore if there is no such setting name
            LOG_TEST(getLogger("AlterCommands"), "No such setting name {}, will ignore", setting_name);
        }
    }
}

/// Splits a parsed `SETTINGS` clause into changes and resets.
/// The parser keeps `name = DEFAULT` entries apart from `changes`, and such an entry means a reset.
void parseSettingsChangesAndResets(const ASTSetQuery & set_query, SettingsChanges & settings_changes, std::set<String> & settings_resets)
{
    settings_changes = set_query.changes;

    for (const auto & setting_name : set_query.default_settings)
    {
        auto same_setting = [&setting_name](const SettingChange & c) { return isSameSetting(c.name, setting_name); };
        if (std::ranges::any_of(settings_changes, same_setting))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting {} is both modified and reset in one command", backQuote(setting_name));

        auto insertion = settings_resets.emplace(setting_name);
        if (!insertion.second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate setting name {}", backQuote(setting_name));
    }
}

/// Rebuilds the implicit minmax indices from the `SETTINGS` clause. A setting dropped from it falls
/// back to `settings_defaults`, the engine's config defaults. Engines without implicit indices pass none.
void refreshSettingsDerivedMetadata(
    StorageInMemoryMetadata & metadata, const MergeTreeSettings * settings_defaults, ContextPtr context)
{
    if (!settings_defaults)
        return;

    MergeTreeSettings effective_settings = *settings_defaults;
    for (const auto & change : metadata.settings_changes->as<ASTSetQuery &>().changes)
    {
        if (MergeTreeSettings::hasBuiltin(change.name))
            effective_settings.applyChange(change, context, /*is_loading_from_existing_metadata=*/true);
    }

    metadata.add_minmax_index_for_numeric_columns = effective_settings[MergeTreeSetting::add_minmax_index_for_numeric_columns];
    metadata.add_minmax_index_for_string_columns = effective_settings[MergeTreeSetting::add_minmax_index_for_string_columns];
    metadata.add_minmax_index_for_temporal_columns = effective_settings[MergeTreeSetting::add_minmax_index_for_temporal_columns];
    metadata.add_minmax_index_for_block_number_column
        = effective_settings[MergeTreeSetting::add_minmax_index_for_block_number_column] && effective_settings[MergeTreeSetting::enable_block_number_column];
    metadata.add_minmax_index_for_block_offset_column
        = effective_settings[MergeTreeSetting::add_minmax_index_for_block_offset_column] && effective_settings[MergeTreeSetting::enable_block_offset_column];

    for (const auto & column : metadata.columns)
    {
        metadata.dropImplicitIndicesForColumn(column.name);
        metadata.addImplicitIndicesForColumn(column, context);
    }
    metadata.dropImplicitIndicesForVirtualColumns();
    metadata.addImplicitIndicesForVirtualColumns(context);
}

AlterCommand::RemoveProperty removePropertyFromString(const String & property)
{
    if (property.empty())
        return AlterCommand::RemoveProperty::NO_PROPERTY;
    if (property == "DEFAULT")
        return AlterCommand::RemoveProperty::DEFAULT;
    if (property == "MATERIALIZED")
        return AlterCommand::RemoveProperty::MATERIALIZED;
    if (property == "ALIAS")
        return AlterCommand::RemoveProperty::ALIAS;
    if (property == "COMMENT")
        return AlterCommand::RemoveProperty::COMMENT;
    if (property == "CODEC")
        return AlterCommand::RemoveProperty::CODEC;
    if (property == "TTL")
        return AlterCommand::RemoveProperty::TTL;
    if (property == "SETTINGS")
        return AlterCommand::RemoveProperty::SETTINGS;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove unknown property '{}'", property);
}

DataTypePtr tryCreateAddToEnumType(const ASTPtr & type_ast, bool is_enum16)
{
    if (!type_ast || !type_ast->as<ASTExpressionList>())
         return {};

    return createEnumAdd(type_ast, is_enum16);
}

/// Apply the trailing `NULL` / `NOT NULL` column modifier, mirroring the logic in
/// InterpreterCreateQuery so that ALTER ADD/MODIFY COLUMN behaves like CREATE TABLE.
void applyNullModifier(DataTypePtr & data_type, const std::optional<bool> & null_modifier)
{
    if (!null_modifier)
        return;
    if (data_type->isNullable())
        throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Can't use [NOT] NULL modifier with Nullable type");
    if (*null_modifier)
        data_type = makeNullable(data_type);
}

/// A column declaration can syntactically carry more modifiers than `AlterCommand` transfers into
/// `ColumnDescription`. Reject the ones ALTER cannot apply instead of silently dropping them.
void checkColumnDeclarationIsSupportedByAlter(const ASTColumnDeclaration & ast_col_decl, std::string_view alter_name)
{
    if (ast_col_decl.getCollation())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot support collation in ALTER TABLE ... {}", alter_name);
    if (ast_col_decl.primary_key_specifier)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot specify PRIMARY KEY in ALTER TABLE ... {}. The primary key can only be defined when the table is created",
            alter_name);
}

}

std::optional<AlterCommand> AlterCommand::parse(const ASTAlterCommand * command_ast)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    if (command_ast->type == ASTAlterCommand::ADD_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::ADD_COLUMN;

        const auto & ast_col_decl = command_ast->col_decl->as<ASTColumnDeclaration &>();
        checkColumnDeclarationIsSupportedByAlter(ast_col_decl, "ADD COLUMN");

        command.column_name = ast_col_decl.name;
        if (ast_col_decl.getType())
        {
            command.data_type = data_type_factory.get(ast_col_decl.getType());
            applyNullModifier(command.data_type, ast_col_decl.null_modifier);
            /// A stored column has to spell its state version out in the metadata the same way
            /// `CREATE TABLE` does (see `InterpreterCreateQuery::getColumnType`): an unversioned
            /// name in stored metadata denotes the layout from before the function became versioned.
            pinCurrentStateVersionToAggregateFunctions(command.data_type);
        }
        if (ast_col_decl.getDefaultExpression())
        {
            command.default_kind = toColumnDefaultKind(ast_col_decl.default_specifier);
            command.default_expression = ast_col_decl.getDefaultExpression();
        }

        if (ast_col_decl.getComment())
        {
            const auto & ast_comment = typeid_cast<ASTLiteral &>(*ast_col_decl.getComment());
            command.comment = ast_comment.value.safeGet<String>();
        }

        if (ast_col_decl.getCodec())
        {
            if (ast_col_decl.default_specifier == ColumnDefaultSpecifier::Alias)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot specify codec for column type ALIAS");
            command.codec = ast_col_decl.getCodec();
        }
        if (command_ast->column)
            command.after_column = getIdentifierName(command_ast->column);

        if (ast_col_decl.getTTL())
            command.ttl = ast_col_decl.getTTL();

        if (ast_col_decl.getSettings())
            command.settings_changes = ast_col_decl.getSettings()->as<ASTSetQuery &>().changes;

        if (ast_col_decl.getStatisticsDesc())
            command.column_statistics_decl = ast_col_decl.getStatisticsDesc()->clone();

        command.first = command_ast->first;
        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::DROP_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_column)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition->clone();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_COLUMN;

        const auto & ast_col_decl = command_ast->col_decl->as<ASTColumnDeclaration &>();
        checkColumnDeclarationIsSupportedByAlter(ast_col_decl, "MODIFY COLUMN");

        command.column_name = ast_col_decl.name;
        command.to_remove = removePropertyFromString(command_ast->remove_property);

        if (ast_col_decl.getType())
        {
            command.data_type = data_type_factory.get(ast_col_decl.getType());
            applyNullModifier(command.data_type, ast_col_decl.null_modifier);
            /// Deliberately NOT pinning the current state version here, unlike ADD COLUMN above.
            /// `DataTypeAggregateFunction::equals` ignores the state version, so a version change is
            /// a metadata-only conversion (`isMetadataOnlyConversion`) and the existing parts keep
            /// the older layout. A rewrite could not repair them either: a version 0 state does not
            /// carry its skip degree, so deserializing and re-serializing it as version 1 would only
            /// record a skip degree of 0. Pinning would therefore make the metadata claim a layout the
            /// stored data does not have. `MODIFY COLUMN` keeps whatever version the user spells out,
            /// so a column can still be moved to the current layout for future writes explicitly, with
            /// `MODIFY COLUMN ... AggregateFunction(1, quantileDeterministic, ...)`.
        }

        if (ast_col_decl.getDefaultExpression())
        {
            command.default_kind = toColumnDefaultKind(ast_col_decl.default_specifier);
            command.default_expression = ast_col_decl.getDefaultExpression();
        }

        if (ast_col_decl.getComment())
        {
            const auto & ast_comment = ast_col_decl.getComment()->as<ASTLiteral &>();
            command.comment.emplace(ast_comment.value.safeGet<String>());
        }

        if (ast_col_decl.getTTL())
            command.ttl = ast_col_decl.getTTL();

        if (ast_col_decl.getCodec())
            command.codec = ast_col_decl.getCodec();

        if (ast_col_decl.getSettings())
            parseSettingsChangesAndResets(
                ast_col_decl.getSettings()->as<ASTSetQuery &>(), command.settings_changes, command.settings_resets);

        if (ast_col_decl.getStatisticsDesc())
            command.column_statistics_decl = ast_col_decl.getStatisticsDesc()->clone();

        /// At most only one of ast_col_decl.settings or command_ast->settings_changes is non-null
        if (command_ast->settings_changes)
        {
            parseSettingsChangesAndResets(
                command_ast->settings_changes->as<ASTSetQuery &>(), command.settings_changes, command.settings_resets);
            command.append_column_setting = true;
        }

        if (command_ast->settings_resets)
        {
            for (const ASTPtr & identifier_ast : command_ast->settings_resets->children)
            {
                const auto & identifier = identifier_ast->as<ASTIdentifier &>();
                command.settings_resets.emplace(identifier.name());
            }
        }

        if (command_ast->add_enum_values)
            command.add_enum_values = command_ast->add_enum_values;

        if (command_ast->column)
            command.after_column = getIdentifierName(command_ast->column);

        command.first = command_ast->first;
        command.if_exists = command_ast->if_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::COMMENT_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = COMMENT_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        const auto & ast_comment = command_ast->comment->as<ASTLiteral &>();
        command.comment = ast_comment.value.safeGet<String>();
        command.if_exists = command_ast->if_exists;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_COMMENT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = COMMENT_TABLE;
        const auto & ast_comment = command_ast->comment->as<ASTLiteral &>();
        command.comment = ast_comment.value.safeGet<String>();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_DATABASE_COMMENT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = MODIFY_DATABASE_COMMENT;
        const auto & ast_comment = command_ast->comment->as<ASTLiteral &>();
        command.comment = ast_comment.value.safeGet<String>();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_ORDER_BY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_ORDER_BY;
        command.order_by = command_ast->order_by->clone();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_SAMPLE_BY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_SAMPLE_BY;
        command.sample_by = command_ast->sample_by->clone();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::REMOVE_SAMPLE_BY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::REMOVE_SAMPLE_BY;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::ADD_INDEX)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.index_decl = command_ast->index_decl->clone();
        command.type = AlterCommand::ADD_INDEX;

        const auto & ast_index_decl = command_ast->index_decl->as<ASTIndexDeclaration &>();

        command.index_name = ast_index_decl.name;

        if (command_ast->index)
            command.after_index_name = command_ast->index->as<ASTIdentifier &>().name();

        command.if_not_exists = command_ast->if_not_exists;
        command.first = command_ast->first;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::ADD_STATISTICS)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.statistics_decl = command_ast->statistics_decl->clone();
        command.type = AlterCommand::ADD_STATISTICS;

        const auto & ast_stat_decl = command_ast->statistics_decl->as<ASTStatisticsDeclaration &>();

        command.statistics_columns = ast_stat_decl.getColumnNames();
        command.statistics_types = ast_stat_decl.getTypeNames();
        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_STATISTICS)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.statistics_decl = command_ast->statistics_decl->clone();
        command.type = AlterCommand::MODIFY_STATISTICS;

        const auto & ast_stat_decl = command_ast->statistics_decl->as<ASTStatisticsDeclaration &>();

        command.statistics_columns = ast_stat_decl.getColumnNames();
        command.statistics_types = ast_stat_decl.getTypeNames();
        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::ADD_CONSTRAINT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.constraint_decl = command_ast->constraint_decl->clone();
        command.type = AlterCommand::ADD_CONSTRAINT;

        const auto & ast_constraint_decl = command_ast->constraint_decl->as<ASTConstraintDeclaration &>();

        command.constraint_name = ast_constraint_decl.name;

        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_CONSTRAINT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.constraint_decl = command_ast->constraint_decl->clone();
        command.type = AlterCommand::MODIFY_CONSTRAINT;

        const auto & ast_constraint_decl = command_ast->constraint_decl->as<ASTConstraintDeclaration &>();

        command.constraint_name = ast_constraint_decl.name;

        command.if_exists = command_ast->if_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::ADD_PROJECTION)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.projection_decl = command_ast->projection_decl->clone();
        command.type = AlterCommand::ADD_PROJECTION;

        const auto & ast_projection_decl = command_ast->projection_decl->as<ASTProjectionDeclaration &>();

        command.projection_name = ast_projection_decl.name;

        if (command_ast->projection)
            command.after_projection_name = command_ast->projection->as<ASTIdentifier &>().name();

        command.first = command_ast->first;
        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_PROJECTION)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.projection_decl = command_ast->projection_decl->clone();
        command.type = AlterCommand::MODIFY_PROJECTION;

        const auto & ast_projection_decl = command_ast->projection_decl->as<ASTProjectionDeclaration &>();

        command.projection_name = ast_projection_decl.name;
        command.if_exists = command_ast->if_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::DROP_CONSTRAINT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.if_exists = command_ast->if_exists;
        command.type = AlterCommand::DROP_CONSTRAINT;
        command.constraint_name = command_ast->constraint->as<ASTIdentifier &>().name();

        return command;
    }
    if (command_ast->type == ASTAlterCommand::DROP_INDEX)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_INDEX;
        command.index_name = command_ast->index->as<ASTIdentifier &>().name();
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_index)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition->clone();

        return command;
    }
    if (command_ast->type == ASTAlterCommand::DROP_STATISTICS)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_STATISTICS;

        if (command_ast->statistics_decl)
        {
            command.statistics_decl = command_ast->statistics_decl->clone();

            const auto & ast_stat_decl = command_ast->statistics_decl->as<ASTStatisticsDeclaration &>();
            command.statistics_columns = ast_stat_decl.getColumnNames();
        }

        command.if_exists = command_ast->if_exists;
        command.clear = command_ast->clear_statistics;

        if (command_ast->partition)
            command.partition = command_ast->partition->clone();

        return command;
    }
    if (command_ast->type == ASTAlterCommand::DROP_PROJECTION)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_PROJECTION;
        command.projection_name = command_ast->projection->as<ASTIdentifier &>().name();
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_projection)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition->clone();

        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_TTL)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_TTL;
        command.ttl = command_ast->ttl->clone();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::REMOVE_TTL)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::REMOVE_TTL;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_SETTING)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_SETTING;
        parseSettingsChangesAndResets(command_ast->settings_changes->as<ASTSetQuery &>(), command.settings_changes, command.settings_resets);
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_DATABASE_SETTING)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_DATABASE_SETTING;
        const auto & set_query = command_ast->settings_changes->as<ASTSetQuery &>();
        /// Databases have no `RESET SETTING`: an engine applies only the changes, so the reset would be
        /// silently dropped and would also skip the engine checks on the setting it removes.
        if (!set_query.default_settings.empty())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Cannot reset setting {}: ALTER DATABASE does not support resetting a setting to DEFAULT",
                backQuote(set_query.default_settings.front()));
        command.settings_changes = set_query.changes;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::RESET_SETTING)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::RESET_SETTING;
        for (const ASTPtr & identifier_ast : command_ast->settings_resets->children)
        {
            const auto & identifier = identifier_ast->as<ASTIdentifier &>();
            auto insertion = command.settings_resets.emplace(identifier.name());
            if (!insertion.second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate setting name {}", backQuote(identifier.name()));
        }
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_QUERY)
    {
        /// Query-construction settings (`select`/`filter`/`order`/`sort`/`limit`/`offset`/`page`) shape a
        /// result via derived-table wrapping during direct execution; a stored materialized view query
        /// cannot support them equivalently (same reasons as the `CREATE VIEW` guard in
        /// InterpreterCreateQuery::createTable). Reject them here too — including when nested in a
        /// subquery's own `SETTINGS` — so `ALTER TABLE ... MODIFY QUERY` cannot bypass that guard.
        if (hasConstructionSettings(*command_ast->select))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Query-construction settings (`select`/`filter`/`order`/`sort`/`limit`/`offset`/`page`) "
                "are not supported in a materialized view definition. Specify them on the query that reads the view instead.");

        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_QUERY;
        command.select = command_ast->select->clone();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_REFRESH)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_REFRESH;
        command.refresh = command_ast->refresh->ptr();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::RENAME_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::RENAME_COLUMN;
        command.column_name = command_ast->column->as<ASTIdentifier &>().name();
        command.rename_to = command_ast->rename_to->as<ASTIdentifier &>().name();
        command.if_exists = command_ast->if_exists;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_SQL_SECURITY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_SQL_SECURITY;
        command.sql_security = command_ast->sql_security->clone();
        return command;
    }

    return {};
}


/// The exact set of columns an ADD COLUMN command materializes: flatten_nested expansion plus the
/// IF NOT EXISTS existence filter. Shared by AlterCommand::apply and AlterCommands::validate so both
/// model the identical schema (an earlier drift here caused apply/validate to disagree on nested adds).
/// Returns empty when the command is a whole-command no-op (IF NOT EXISTS and the column already exists).
static std::vector<ColumnDescription> columnsAddedByAlter(
    const ColumnsDescription & existing_columns,
    ColumnDescription column,
    ContextPtr context,
    bool if_not_exists,
    bool share_nested_offsets)
{
    /// An exact `column_name` match is always a whole-command no-op; the `n`/`n.*` nested equivalence
    /// (`hasNested`) is only "exists" when share_nested_offsets is enabled, matching prepare()/validate().
    if (if_not_exists
        && (existing_columns.has(column.name)
            || (share_nested_offsets && existing_columns.hasNested(column.name))))
        return {};

    std::vector<ColumnDescription> columns_to_add;
    if (context->getSettingsRef()[Setting::flatten_nested])
    {
        StorageInMemoryMetadata temporary_metadata;
        temporary_metadata.columns.add(column, /*after_column*/ "", /*first*/ true);
        temporary_metadata.columns.flattenNested();

        for (const auto & col : temporary_metadata.columns.getAll())
            columns_to_add.push_back(temporary_metadata.columns.get(col.name));
    }
    else
    {
        columns_to_add.push_back(std::move(column));
    }

    /// Skip only the EXACT transformed names that already exist (not an `n.*` prefix), so a repeated
    /// flattened `n.a` add is a no-op while a genuinely new distinct column is never dropped.
    if (if_not_exists)
        std::erase_if(columns_to_add, [&](const ColumnDescription & c) { return existing_columns.has(c.name); });

    return columns_to_add;
}


std::optional<AlterCommand> AlterCommand::extractSettingsResets()
{
    if (type != MODIFY_SETTING || settings_resets.empty())
        return {};

    if (settings_changes.empty())
    {
        type = RESET_SETTING;
        return {};
    }

    AlterCommand reset_command;
    reset_command.ast = ast;
    reset_command.type = RESET_SETTING;
    reset_command.settings_resets = std::move(settings_resets);
    settings_resets.clear();
    return reset_command;
}


void AlterCommand::apply(
    StorageInMemoryMetadata & metadata,
    ContextPtr context,
    bool share_nested_offsets,
    const ColumnsDescription * columns_before_alter,
    const MergeTreeSettings * settings_defaults) const
{
    /// Helper function for column existence check with IF EXISTS
    auto should_skip_column_operation = [&]() -> bool {
        return if_exists && !metadata.columns.has(column_name);
    };

    /// validate() screens these column names too, but against a model that tracks only ADD/DROP/MODIFY/RENAME
    /// COLUMN - not MODIFY QUERY, which replaces a materialized view's columns with its new query's output.
    auto skip_absent_column_or_fail = [&](std::string_view action) -> bool
    {
        if (should_skip_column_operation())
            return true;
        if (metadata.columns.has(column_name))
            return false;

        auto message = PreformattedMessage::create("Wrong column name. Cannot find column {} to {}", backQuote(column_name), action);
        metadata.columns.appendHintsMessage(message.text, column_name);
        throw Exception(std::move(message), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
    };

    if (type == ADD_COLUMN)
    {
        ColumnDescription column(column_name, data_type);
        if (default_expression)
        {
            column.default_desc.kind = default_kind;
            column.default_desc.expression = default_expression;
        }
        if (comment)
            column.comment = *comment;

        if (codec)
            column.codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(codec, data_type, CodecValidationSettings::trusted());

        column.ttl = ttl;

        if (!settings_changes.empty())
        {
            MergeTreeColumnSettings::validate(settings_changes);
            column.settings = settings_changes;
        }

        /// The declared statistics are transferred like in CREATE (the types are validated against the
        /// column data type by the storage in `checkAlterIsPossible`).
        if (column_statistics_decl)
            column.statistics = ColumnStatisticsDescription::fromStatisticsDescriptionAST(column_statistics_decl, column_name, data_type);

        /// The exact columns this ADD materializes (flatten_nested expansion + IF NOT EXISTS filter).
        /// Empty means a whole-command no-op. validate() advances its snapshot with the same set so
        /// apply() and validate() never disagree on what a (nested) ADD introduces.
        auto columns_to_add = columnsAddedByAlter(metadata.columns, column, context, if_not_exists, share_nested_offsets);
        if (columns_to_add.empty())
            return;

        if (!after_column.empty() || first)
        {
            for (const auto & col : columns_to_add | std::views::reverse)
                metadata.columns.add(col, after_column, first);
        }
        else
        {
            for (const auto & col : columns_to_add)
                metadata.columns.add(col, after_column, first);
        }

        metadata.addImplicitIndicesForColumn(column, context);
    }
    else if (type == DROP_COLUMN)
    {
        metadata.dropImplicitIndicesForColumn(column_name);

        /// Otherwise just clear data on disk
        if (!clear && !partition)
        {
            if (should_skip_column_operation())
                return;
            metadata.columns.remove(column_name);
        }
    }
    else if (type == MODIFY_COLUMN)
    {
        if (skip_absent_column_or_fail("modify"))
            return;
        metadata.columns.modify(column_name, after_column, first, [&](ColumnDescription & column)
        {
            if (to_remove == RemoveProperty::DEFAULT
                || to_remove == RemoveProperty::MATERIALIZED
                || to_remove == RemoveProperty::ALIAS)
            {
                column.default_desc = ColumnDefault{};
            }
            else if (to_remove == RemoveProperty::CODEC)
            {
                column.codec.reset();
            }
            else if (to_remove == RemoveProperty::COMMENT)
            {
                column.comment = String{};
            }
            else if (to_remove == RemoveProperty::TTL)
            {
                column.ttl.reset();
            }
            else if (to_remove == RemoveProperty::SETTINGS)
            {
                column.settings.clear();
            }
            else
            {
                if (codec)
                    column.codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                        codec, data_type ? data_type : column.type, CodecValidationSettings::trusted());

                if (comment)
                    column.comment = *comment;

                if (ttl)
                    column.ttl = ttl;

                if (data_type)
                {
                    column.type = data_type;
                    /// Update statistics data type to match the new column type
                    if (!column.statistics.empty())
                        column.statistics.data_type = data_type;
                    /// The type changed, so assume that implicit indices may change too
                    metadata.dropImplicitIndicesForColumn(column_name);
                    metadata.addImplicitIndicesForColumn(column, context);
                }

                /// The declared statistics replace the explicit statistics of the column, like the other
                /// declared properties (implicit statistics from `auto_statistics_types` are re-added by
                /// the storage). The types are validated by the storage in `checkAlterIsPossible`.
                if (column_statistics_decl)
                    column.statistics = ColumnStatisticsDescription::fromStatisticsDescriptionAST(column_statistics_decl, column_name, column.type);

                if (!settings_changes.empty())
                {
                    MergeTreeColumnSettings::validate(settings_changes);
                    if (append_column_setting)
                        for (const auto & change : settings_changes)
                            column.settings.setSetting(change.name, change.value);
                    else
                        column.settings = settings_changes;
                }

                if (!settings_resets.empty())
                {
                    for (const auto & setting : settings_resets)
                        column.settings.removeSetting(setting);
                }

                /// Restating the type is not a default decision, so the column keeps the default it
                /// currently has. Removals are handled by the `to_remove` branches above.
                if (default_expression)
                {
                    column.default_desc.kind = default_kind;
                    column.default_desc.expression = default_expression;
                }
            }
        });

    }
    else if (type == MODIFY_ORDER_BY)
    {
        auto & sorting_key = metadata.sorting_key;
        auto & primary_key = metadata.primary_key;
        if (primary_key.definition_ast == nullptr && sorting_key.definition_ast != nullptr)
        {
            /// Primary and sorting key become independent after this ALTER so
            /// we have to save the old ORDER BY expression as the new primary
            /// key.
            primary_key = KeyDescription::getKeyFromAST(sorting_key.definition_ast, metadata.columns, metadata.virtuals, context);
        }

        /// An expression added to the sorting key may use only the columns (and their subcolumns) added by
        /// the same ALTER - see `MergeTreeData::checkProperties` - so for a typo in it only those are
        /// suggested: an existing column or a virtual one would pass the analysis and fail that check.
        std::optional<Names> hint_columns;
        if (columns_before_alter)
        {
            hint_columns.emplace();
            for (const auto & column : metadata.columns.get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns()))
                if (!columns_before_alter->hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column.name))
                    hint_columns->push_back(column.name);
        }

        /// Recalculate key with new order_by expression.
        sorting_key.recalculateWithNewAST(order_by, metadata.columns, metadata.virtuals, context, hint_columns);
    }
    else if (type == MODIFY_SAMPLE_BY)
    {
        metadata.sampling_key.recalculateWithNewAST(sample_by, metadata.columns, metadata.virtuals, context);
    }
    else if (type == REMOVE_SAMPLE_BY)
    {
        metadata.sampling_key = {};
    }
    else if (type == COMMENT_COLUMN)
    {
        if (skip_absent_column_or_fail("comment"))
            return;

        metadata.columns.modify(column_name,
            [&](ColumnDescription & column)
            {
                column.comment = *comment;
            });
    }
    else if (type == COMMENT_TABLE)
    {
        metadata.comment = *comment;
    }
    else if (type == ADD_INDEX)
    {
        if (std::any_of(
                metadata.secondary_indices.cbegin(),
                metadata.secondary_indices.cend(),
                [this](const auto & index)
                {
                    return index.name == index_name;
                }))
        {
            if (if_not_exists)
                return;
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot add index {}: index with this name already exists", index_name);
        }


        auto using_auto_minmax_index =
               metadata.add_minmax_index_for_numeric_columns
            || metadata.add_minmax_index_for_string_columns
            || metadata.add_minmax_index_for_temporal_columns
            || metadata.add_minmax_index_for_block_number_column
            || metadata.add_minmax_index_for_block_offset_column;
        if (index_name.starts_with(IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX) && using_auto_minmax_index)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot add index {} because it uses a reserved index name", index_name);
        }

        auto insert_it = metadata.secondary_indices.end();

        /// insert the index in the beginning of the indices list
        if (first)
            insert_it = metadata.secondary_indices.begin();

        if (!after_index_name.empty())
        {
            insert_it = std::find_if(
                    metadata.secondary_indices.begin(),
                    metadata.secondary_indices.end(),
                    [this](const auto & index)
                    {
                        return index.name == after_index_name;
                    });

            if (insert_it == metadata.secondary_indices.end())
            {
                auto hints = metadata.secondary_indices.getHints(after_index_name);
                auto hints_string = !hints.empty() ? ", may be you meant: " + toString(hints) : "";
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong index name. Cannot find index {} to insert after{}",
                    backQuote(after_index_name), hints_string);
            }

            ++insert_it;
        }

        metadata.secondary_indices.emplace(
            insert_it,
            IndexDescription::getIndexFromAST(
                index_decl, metadata.columns, /* is_implicitly_created */ false, metadata.escape_index_filenames, context));
    }
    else if (type == DROP_INDEX)
    {
        if (!partition && !clear)
        {
            auto erase_it = std::find_if(
                    metadata.secondary_indices.begin(),
                    metadata.secondary_indices.end(),
                    [this](const auto & index)
                    {
                        return index.name == index_name;
                    });

            if (erase_it == metadata.secondary_indices.end())
            {
                if (if_exists)
                    return;
                auto hints = metadata.secondary_indices.getHints(index_name);
                auto hints_string = !hints.empty() ? ", may be you meant: " + toString(hints) : "";
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong index name. Cannot find index {} to drop{}",
                    backQuote(index_name), hints_string);
            }

            metadata.secondary_indices.erase(erase_it);
        }
    }
    else if (type == ADD_STATISTICS)
    {
        for (const auto & statistics_column_name : statistics_columns)
        {
            if (!metadata.columns.has(statistics_column_name))
            {
                throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Cannot add statistics for column {}: this column is not found", statistics_column_name);
            }
        }

        auto stats_vec = ColumnStatisticsDescription::fromAST(statistics_decl, metadata.columns);
        for (const auto & [stats_column_name, stats] : stats_vec)
        {
            metadata.columns.modify(stats_column_name,
                [&](ColumnDescription & column) { column.statistics.merge(stats, column.name, column.type, if_not_exists); });
        }
    }
    else if (type == DROP_STATISTICS)
    {
        for (const auto & statistics_column_name : statistics_columns)
        {
            if (!metadata.columns.has(statistics_column_name)
                || metadata.columns.get(statistics_column_name).statistics.empty())
            {
                if (if_exists)
                    return;
                throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Wrong statistics name. Cannot find statistics {} to drop", backQuote(statistics_column_name));
            }

            if (!clear && !partition)
                metadata.columns.modify(statistics_column_name,
                    [&](ColumnDescription & column) { column.statistics.clear(); });
        }
    }
    else if (type == MODIFY_STATISTICS)
    {
        for (const auto & statistics_column_name : statistics_columns)
        {
            if (!metadata.columns.has(statistics_column_name))
            {
                throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Cannot modify statistics for column {}: this column is not found", statistics_column_name);
            }
        }

        auto stats_vec = ColumnStatisticsDescription::fromAST(statistics_decl, metadata.columns);
        for (const auto & [stats_column_name, stats] : stats_vec)
        {
            metadata.columns.modify(stats_column_name,
                [&](ColumnDescription & column) { column.statistics.assign(stats); });
        }
    }
    else if (type == ADD_CONSTRAINT)
    {
        auto constraints = metadata.constraints.getConstraints();
        if (std::any_of(
                constraints.cbegin(),
                constraints.cend(),
                [this](const ASTPtr & constraint_ast)
                {
                    return constraint_ast->as<ASTConstraintDeclaration &>().name == constraint_name;
                }))
        {
            if (if_not_exists)
                return;
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot add constraint {}: constraint with this name already exists",
                        constraint_name);
        }

        auto insert_it = constraints.end();
        constraints.emplace(insert_it, constraint_decl);
        metadata.constraints = ConstraintsDescription(constraints);
    }
    else if (type == DROP_CONSTRAINT)
    {
        auto constraints = metadata.constraints.getConstraints();
        auto erase_it = std::find_if(
            constraints.begin(),
            constraints.end(),
            [this](const ASTPtr & constraint_ast) { return constraint_ast->as<ASTConstraintDeclaration &>().name == constraint_name; });

        if (erase_it == constraints.end())
        {
            if (if_exists)
                return;
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong constraint name. Cannot find constraint `{}` to drop",
                    constraint_name);
        }
        constraints.erase(erase_it);
        metadata.constraints = ConstraintsDescription(constraints);
    }
    else if (type == MODIFY_CONSTRAINT)
    {
        auto constraints = metadata.constraints.getConstraints();
        auto modify_it = std::find_if(
            constraints.begin(),
            constraints.end(),
            [this](const ASTPtr & constraint_ast) { return constraint_ast->as<ASTConstraintDeclaration &>().name == constraint_name; });

        if (modify_it == constraints.end())
        {
            if (if_exists)
                return;
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong constraint name. Cannot find constraint `{}` to modify",
                    constraint_name);
        }

        /// Replace the declaration in place so the constraint keeps its position.
        *modify_it = constraint_decl;
        metadata.constraints = ConstraintsDescription(constraints);
    }
    else if (type == ADD_PROJECTION)
    {
        if (!metadata.projections.checkCanAdd(projection_name, if_not_exists))
            return;
        auto projection = ProjectionDescription::getProjectionFromAST(
            projection_decl, metadata.columns, &metadata.partition_key, context, LoadingStrictnessLevel::CREATE);
        metadata.projections.add(std::move(projection), after_projection_name, first, if_not_exists);
    }
    else if (type == MODIFY_PROJECTION)
    {
        if (!metadata.projections.has(projection_name))
        {
            const bool is_unavailable = metadata.projections.isUnavailable(projection_name);
            /// The initiator validated this settings-only change. A secondary may have retained
            /// the old declaration as unavailable, so update that AST without analyzing it here.
            if (is_unavailable && isSecondaryProjectionMetadataReplay(context))
            {
                const auto & declaration = projection_decl->as<const ASTProjectionDeclaration &>();
                metadata.projections.replaceUnavailableSettings(projection_name, declaration.with_settings);
                return;
            }

            if (is_unavailable)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot modify unavailable projection {}: restore its analysis or drop it before changing its settings",
                    backQuote(projection_name));

            /// With `IF EXISTS` the whole command must be a no-op
            if (if_exists)
                return;

            throw Exception(
                ErrorCodes::NO_SUCH_PROJECTION_IN_TABLE,
                "There is no projection {} in table{}",
                projection_name,
                metadata.projections.getHintsMessage(projection_name));
        }

        /// create a new projection with the modified settings
        auto new_projection = ProjectionDescription::getProjectionFromAST(
            projection_decl, metadata.columns, &metadata.partition_key, context, LoadingStrictnessLevel::CREATE);

        /// Existing parts store projection data built from the query body, so only the `WITH SETTINGS` clause may change
        auto definition_without_settings = [](const IAST & definition_ast)
        {
            auto cloned = definition_ast.clone();
            auto & decl = cloned->as<ASTProjectionDeclaration &>();
            cloned->reset(decl.with_settings);
            return cloned->formatWithSecretsOneLine();
        };

        const auto & old_projection = metadata.projections.get(projection_name);
        if (definition_without_settings(*old_projection.definition_ast) != definition_without_settings(*new_projection.definition_ast))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot modify projection {}: only the WITH SETTINGS clause may be changed, "
                "but the projection query differs from the existing one. "
                "Use DROP PROJECTION and ADD PROJECTION to change the query",
                projection_name);

        /// Intentionally not a mutation: the new settings apply lazily, to projection parts written
        /// by future inserts and merges. `MATERIALIZE PROJECTION` does not rebuild a projection that
        /// a part already has, so existing data picks up the new settings only when its parts are merged.
        metadata.projections.replace(std::move(new_projection));
    }
    else if (type == DROP_PROJECTION)
    {
        if (!partition && !clear)
            metadata.projections.remove(projection_name, if_exists);
    }
    else if (type == MODIFY_TTL)
    {
        metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
            ttl,
            metadata.columns,
            context,
            metadata.primary_key,
            context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions] ? TTLValidationMode::SkipValidation
                                                                                 : TTLValidationMode::Validate);
    }
    else if (type == REMOVE_TTL)
    {
        metadata.table_ttl = TTLTableDescription{};
    }
    else if (type == MODIFY_QUERY)
    {
        metadata.select = SelectQueryDescription::getSelectQueryFromASTForMatView(select, metadata.refresh != nullptr, context);

#if CLICKHOUSE_CLOUD
        /// For Shared Catalog on secondary replicas very likely we don't have the settings used to run the SELECT on the initiator.
        /// Because of that we can fail, or the resulting columns can be different from the original ones.
        /// So we return early and set the columns ourselves, if they differ.
        if (context->getClientInfo().is_shared_catalog_internal && !SharedDatabaseCatalog::isInitialQuery(context))
            return;
#endif

        SharedHeader as_select_sample = InterpreterSelectQueryAnalyzer::getSampleBlock(select->clone(), context);

        metadata.columns = ColumnsDescription(as_select_sample->getNamesAndTypesList());
    }
    else if (type == MODIFY_REFRESH)
    {
        metadata.refresh = refresh->clone();
    }
    else if (type == MODIFY_SETTING)
    {
        if (!metadata.settings_changes)
        {
            auto changes = make_intrusive<ASTSetQuery>();
            changes->is_standalone = false;
            metadata.settings_changes = std::move(changes);
        }

        auto & settings_from_storage = metadata.settings_changes->as<ASTSetQuery &>().changes;
        resetSettings(settings_from_storage, settings_resets);

        for (const auto & change : settings_changes)
        {
            auto same_setting = [&change](const SettingChange & c) { return isSameSetting(c.name, change.name); };
            auto it = std::find_if(settings_from_storage.begin(), settings_from_storage.end(), same_setting);

            if (it == settings_from_storage.end())
            {
                settings_from_storage.push_back(change);
                continue;
            }

            /// The statement states the setting under a name of its own choosing, which need not be the one
            /// the definition was written with. It is still one setting, so it is left holding one entry:
            /// a definition can state a setting under each of its names, and the last of them is in effect.
            it->name = change.name;
            it->value = change.value;
            settings_from_storage.erase(
                std::remove_if(it + 1, settings_from_storage.end(), same_setting), settings_from_storage.end());
        }

        refreshSettingsDerivedMetadata(metadata, settings_defaults, context);
    }
    else if (type == RESET_SETTING)
    {
        if (!metadata.settings_changes)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot reset settings, because table does not have settings changes");

        resetSettings(metadata.settings_changes->as<ASTSetQuery &>().changes, settings_resets);
        refreshSettingsDerivedMetadata(metadata, settings_defaults, context);
    }
    else if (type == RENAME_COLUMN)
    {
        if (should_skip_column_operation())
            return;
        metadata.columns.rename(column_name, rename_to);
        RenameColumnData rename_data{column_name, rename_to};
        RenameColumnVisitor rename_visitor(rename_data);
        for (const auto & column : metadata.columns)
        {
            metadata.columns.modify(column.name, [&](ColumnDescription & column_to_modify)
            {
                if (column_to_modify.default_desc.expression)
                    rename_visitor.visit(column_to_modify.default_desc.expression);
                if (column_to_modify.ttl)
                    rename_visitor.visit(column_to_modify.ttl);
            });
        }
        if (metadata.table_ttl.definition_ast)
            rename_visitor.visit(metadata.table_ttl.definition_ast);

        auto constraints_data = metadata.constraints.getConstraints();
        for (auto & constraint : constraints_data)
            rename_visitor.visit(constraint);
        metadata.constraints = ConstraintsDescription(constraints_data);

        if (metadata.isSortingKeyDefined())
            rename_visitor.visit(metadata.sorting_key.definition_ast);

        if (metadata.isPrimaryKeyDefined())
            rename_visitor.visit(metadata.primary_key.definition_ast);

        if (metadata.isSamplingKeyDefined())
            rename_visitor.visit(metadata.sampling_key.definition_ast);

        if (metadata.isPartitionKeyDefined())
            rename_visitor.visit(metadata.partition_key.definition_ast);

        for (auto & index : metadata.secondary_indices)
        {
            /// For implicit indices, check the index name rather than column_names because
            /// for ALIAS columns, column_names contains the underlying expression columns.
            if (index.isImplicitlyCreated() && index.name == IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX + column_name)
            {
                index.definition_ast = createImplicitMinMaxIndexAST(rename_to);
                index.name = IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX + rename_to;
                /// For an ALIAS column the index covers the columns its expression expands to,
                /// which the column's own name never appears in, so a rename does not affect them.
                if (!metadata.columns.hasAlias(rename_to))
                    index.column_names = {rename_to};
            }
            else
                rename_visitor.visit(index.definition_ast);
        }
    }
    else if (type == MODIFY_SQL_SECURITY)
        metadata.setSQLSecurity(sql_security->as<ASTSQLSecurity &>());
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong parameter type in ALTER query");
}

namespace
{

/// Checks if the only difference between two JSON (DataTypeObject) types is their
/// typed_paths. All other parameters must be identical, making this safe to treat
/// as a metadata-only conversion without rewriting data.
bool isJSONTypeHintOnlyChange(const IDataType * from_type, const IDataType * to_type)
{
    const auto * from_json = typeid_cast<const DataTypeObject *>(from_type);
    const auto * to_json = typeid_cast<const DataTypeObject *>(to_type);

    if (!from_json || !to_json)
        return false;

    if (from_json->getSchemaFormat() != DataTypeObject::SchemaFormat::JSON
        || to_json->getSchemaFormat() != DataTypeObject::SchemaFormat::JSON)
        return false;

    if (from_json->getMaxDynamicPaths() != to_json->getMaxDynamicPaths())
        return false;

    if (from_json->getMaxDynamicTypes() != to_json->getMaxDynamicTypes())
        return false;

    if (from_json->getPathsToSkip() != to_json->getPathsToSkip())
        return false;

    if (from_json->getPathRegexpsToSkip() != to_json->getPathRegexpsToSkip())
        return false;

    return true;
}

/// True metadata-only conversion: identical on-disk bytes, only the logical type changes
/// (e.g. `Date`<->`UInt16`, enum widening). Safe everywhere, including positionally-persisted
/// values. Recurses through `Array`/`Nullable`.
bool isTrueMetadataOnlyConversion(const IDataType * from, const IDataType * to)
{
    auto is_compatible_enum_types_conversion = [](const IDataType * from_type, const IDataType * to_type)
    {
        if (const auto * from_enum8 = typeid_cast<const DataTypeEnum8 *>(from_type))
        {
            if (const auto * to_enum8 = typeid_cast<const DataTypeEnum8 *>(to_type))
                return to_enum8->contains(*from_enum8);
        }

        if (const auto * from_enum16 = typeid_cast<const DataTypeEnum16 *>(from_type))
        {
            if (const auto * to_enum16 = typeid_cast<const DataTypeEnum16 *>(to_type))
                return to_enum16->contains(*from_enum16);
        }

        return false;
    };

    static const std::unordered_multimap<std::type_index, const std::type_info &> allowed_conversions =
        {
            { typeid(DataTypeEnum8),    typeid(DataTypeInt8)     },
            { typeid(DataTypeEnum16),   typeid(DataTypeInt16)    },
            { typeid(DataTypeDateTime), typeid(DataTypeUInt32)   },
            { typeid(DataTypeUInt32),   typeid(DataTypeDateTime) },
            { typeid(DataTypeDate),     typeid(DataTypeUInt16)   },
            { typeid(DataTypeUInt16),   typeid(DataTypeDate)     },
        };

    /// Unwrap some nested and check for valid conversions
    while (true)
    {
        /// types are equal, obviously pure metadata alter
        if (from->equals(*to))
            return true;

        /// We just adding something to enum, nothing changed on disk
        if (is_compatible_enum_types_conversion(from, to))
            return true;

        /// Types changed, but representation on disk didn't
        auto it_range = allowed_conversions.equal_range(typeid(*from));
        for (auto it = it_range.first; it != it_range.second; ++it)
        {
            if (it->second == typeid(*to))
                return true;
        }

        const auto * arr_from = typeid_cast<const DataTypeArray *>(from);
        const auto * arr_to = typeid_cast<const DataTypeArray *>(to);
        if (arr_from && arr_to)
        {
            from = arr_from->getNestedType().get();
            to = arr_to->getNestedType().get();
            continue;
        }

        const auto * nullable_from = typeid_cast<const DataTypeNullable *>(from);
        const auto * nullable_to = typeid_cast<const DataTypeNullable *>(to);
        if (nullable_from && nullable_to)
        {
            from = nullable_from->getNestedType().get();
            to = nullable_to->getNestedType().get();
            continue;
        }

        return false;
    }
}

bool isJSONLazyMetadataConversion(const IDataType * from, const IDataType * to, const ContextPtr & context)
{
    if (!context || !context->getSettingsRef()[Setting::enable_json_lazy_type_hints])
        return false;

    /// Identical types are byte-identical, not a lazy conversion.
    if (from->equals(*to))
        return false;

    /// Unwrap `Array`/`Nullable` to reach a JSON type at any depth.
    while (true)
    {
        if (isJSONTypeHintOnlyChange(from, to))
            return true;

        const auto * arr_from = typeid_cast<const DataTypeArray *>(from);
        const auto * arr_to = typeid_cast<const DataTypeArray *>(to);
        if (arr_from && arr_to)
        {
            from = arr_from->getNestedType().get();
            to = arr_to->getNestedType().get();
            continue;
        }

        const auto * nullable_from = typeid_cast<const DataTypeNullable *>(from);
        const auto * nullable_to = typeid_cast<const DataTypeNullable *>(to);
        if (nullable_from && nullable_to)
        {
            from = nullable_from->getNestedType().get();
            to = nullable_to->getNestedType().get();
            continue;
        }

        return false;
    }
}

/// True for named-Tuple subfield additions through `Array` and `Map` wrappers.
/// Existing fields must keep their names and order. `nested_lazy_settings` records other lazy
/// conversions required by retained fields and is merged only if the whole Tuple change is valid.
bool isNamedTupleSubfieldAddition(
    const IDataType * from,
    const IDataType * to,
    const ContextPtr & context,
    std::set<std::string_view> & nested_lazy_settings)
{
    if (!context || !context->getSettingsRef()[Setting::allow_metadata_only_named_tuple_alter])
        return false;

    if (from->equals(*to))
        return false;

    /// Unwrap Array
    if (const auto * arr_from = typeid_cast<const DataTypeArray *>(from))
    {
        const auto * arr_to = typeid_cast<const DataTypeArray *>(to);
        if (!arr_to)
            return false;
        return isNamedTupleSubfieldAddition(
            arr_from->getNestedType().get(), arr_to->getNestedType().get(), context, nested_lazy_settings);
    }

    if (typeid_cast<const DataTypeNullable *>(from))
        return false;

    /// Unwrap Map (check key equality)
    if (const auto * map_from = typeid_cast<const DataTypeMap *>(from))
    {
        const auto * map_to = typeid_cast<const DataTypeMap *>(to);
        if (!map_to)
            return false;
        if (!map_from->getKeyType()->equals(*map_to->getKeyType()))
            return false;
        return isNamedTupleSubfieldAddition(
            map_from->getValueType().get(), map_to->getValueType().get(), context, nested_lazy_settings);
    }

    /// Tuple level: check names present, ordering preserved, existing fields compatible
    const auto * tuple_from = typeid_cast<const DataTypeTuple *>(from);
    const auto * tuple_to = typeid_cast<const DataTypeTuple *>(to);
    if (!tuple_from || !tuple_to
        || !tuple_from->hasExplicitNames() || !tuple_to->hasExplicitNames())
        return false;

    const auto & from_names = tuple_from->getElementNames();
    const auto & from_types = tuple_from->getElements();
    const auto & to_names = tuple_to->getElementNames();
    const auto & to_types = tuple_to->getElements();

    bool found_addition = to_names.size() > from_names.size();
    std::optional<size_t> last_to_index;
    for (size_t i = 0; i < from_names.size(); ++i)
    {
        auto to_index = tuple_to->tryGetPositionByName(from_names[i]);
        if (!to_index || (last_to_index && *to_index <= *last_to_index))
            return false;
        last_to_index = to_index;
        const IDataType * old_elem = from_types[i].get();
        const IDataType * new_elem = to_types[*to_index].get();
        if (old_elem->equals(*new_elem))
            continue;

        if (isNamedTupleSubfieldAddition(old_elem, new_elem, context, nested_lazy_settings))
        {
            found_addition = true;
            continue;
        }
        if (isJSONLazyMetadataConversion(old_elem, new_elem, context))
        {
            nested_lazy_settings.emplace("enable_json_lazy_type_hints");
            continue;
        }
        if (!isTrueMetadataOnlyConversion(old_elem, new_elem))
            return false;
    }
    return found_addition;
}

/// Collects every setting that enables a lazy conversion for this type change. Lazy conversions
/// change the on-disk representation without scheduling an immediate mutation. Checks are
/// independent because one conversion may be enabled by more than one mechanism.
std::set<std::string_view> getLazyMetadataConversionSettings(
    const IDataType * from, const IDataType * to, const ContextPtr & context)
{
    std::set<std::string_view> settings;
    if (!context)
        return settings;

    std::set<std::string_view> named_tuple_nested_settings;
    if (isNamedTupleSubfieldAddition(from, to, context, named_tuple_nested_settings))
    {
        settings.emplace("allow_metadata_only_named_tuple_alter");
        settings.insert(named_tuple_nested_settings.begin(), named_tuple_nested_settings.end());
    }

    if (isJSONLazyMetadataConversion(from, to, context))
        settings.emplace("enable_json_lazy_type_hints");

    return settings;
}

}

bool AlterCommand::isSettingsAlter() const
{
    return type == MODIFY_SETTING || type == RESET_SETTING;
}

MutationStageDecision AlterCommand::getMutationStageDecision(
    const StorageInMemoryMetadata & metadata, const ContextPtr & context) const
{
    MutationStageDecision decision;
    if (ignore)
        return decision;

    if (isRemovingProperty() || type == REMOVE_TTL || type == REMOVE_SAMPLE_BY)
        return decision;

    if (type == DROP_INDEX || type == DROP_PROJECTION || type == RENAME_COLUMN || type == DROP_STATISTICS)
    {
        decision.requires_mutation = true;
        return decision;
    }

    if (type == DROP_COLUMN)
    {
        decision.requires_mutation = metadata.columns.hasColumnOrNested(GetColumnsOptions::AllPhysical, column_name);
        return decision;
    }

    if (type != MODIFY_COLUMN || data_type == nullptr)
        return decision;

    for (const auto & column : metadata.columns.getAllPhysical())
    {
        if (column.name != column_name)
            continue;

        if (isTrueMetadataOnlyConversion(column.type.get(), data_type.get()))
            return decision;

        decision.lazy_settings = getLazyMetadataConversionSettings(column.type.get(), data_type.get(), context);
        decision.requires_mutation = decision.lazy_settings.empty();
        return decision;
    }
    return decision;
}

bool AlterCommand::isCommentAlter() const
{
    if (type == COMMENT_COLUMN || type == COMMENT_TABLE)
    {
        return true;
    }
    if (type == MODIFY_COLUMN)
    {
        /// Placement (FIRST/AFTER) and per-column SETTINGS change the replicated
        /// /columns (ColumnsDescription::operator== compares column order and
        /// settings, ignoring only the comment), so they are not comment-only.
        return comment.has_value() && codec == nullptr && data_type == nullptr && default_expression == nullptr && ttl == nullptr
            && settings_changes.empty() && settings_resets.empty() && column_statistics_decl == nullptr && after_column.empty() && !first;
    }
    return false;
}

bool AlterCommand::isTTLAlter(const StorageInMemoryMetadata & metadata) const
{
    if (type == MODIFY_TTL)
    {
        if (!metadata.table_ttl.definition_ast)
            return true;
        /// If TTL had not been changed, do not require mutations
        return metadata.table_ttl.definition_ast->formatIgnoringRedundantParentheses() != ttl->formatIgnoringRedundantParentheses();
    }

    if (!ttl || type != MODIFY_COLUMN)
        return false;

    bool column_ttl_changed = true;
    for (const auto & [name, ttl_ast] : metadata.columns.getColumnTTLs())
    {
        if (name == column_name && ttl->formatIgnoringRedundantParentheses() == ttl_ast->formatIgnoringRedundantParentheses())
        {
            column_ttl_changed = false;
            break;
        }
    }

    return column_ttl_changed;
}

bool AlterCommand::isRemovingProperty() const
{
    return to_remove != RemoveProperty::NO_PROPERTY;
}

bool AlterCommand::isDropOrRename() const
{
    return type == Type::DROP_COLUMN
        || type == Type::DROP_INDEX
        || type == Type::DROP_STATISTICS
        || type == Type::DROP_CONSTRAINT
        || type == Type::DROP_PROJECTION
        || type == Type::RENAME_COLUMN;
}

std::optional<MutationCommand> AlterCommand::tryConvertToMutationCommand(StorageInMemoryMetadata & metadata, ContextPtr context, bool share_nested_offsets) const
{
    if (!getMutationStageDecision(metadata, context).requires_mutation)
    {
        /// Even though this command doesn't require a mutation, we still need to apply it
        /// to the metadata so that subsequent commands see the updated state. For example,
        /// ADD COLUMN followed by RENAME COLUMN needs the new column to be visible.
        if (!ignore)
            apply(metadata, context, share_nested_offsets);
        return {};
    }

    MutationCommand result;

    if (type == MODIFY_COLUMN)
    {
        result.type = MutationCommand::Type::READ_COLUMN;
        result.column_name = column_name;
        result.data_type = data_type;
    }
    else if (type == DROP_COLUMN)
    {
        result.type = MutationCommand::Type::DROP_COLUMN;
        result.column_name = column_name;
        if (clear)
            result.clear = true;
    }
    else if (type == DROP_INDEX)
    {
        result.type = MutationCommand::Type::DROP_INDEX;
        result.column_name = index_name;
        if (clear)
            result.clear = true;
    }
    else if (type == DROP_STATISTICS)
    {
        result.type = MutationCommand::Type::DROP_STATISTICS;
        result.statistics_columns = statistics_columns;

        if (clear)
            result.clear = true;
    }
    else if (type == DROP_PROJECTION)
    {
        result.type = MutationCommand::Type::DROP_PROJECTION;
        result.column_name = projection_name;
        if (clear)
            result.clear = true;
    }
    else if (type == RENAME_COLUMN)
    {
        result.type = MutationCommand::Type::RENAME_COLUMN;
        result.column_name = column_name;
        result.rename_to = rename_to;
    }

    result.ast_text = ast->formatWithSecretsOneLine();
    const auto & settings = context->getSettingsRef();
    result.max_parser_depth = settings[Setting::max_parser_depth];
    result.max_parser_backtracks = settings[Setting::max_parser_backtracks];
    apply(metadata, context, share_nested_offsets);
    return result;
}

bool AlterCommands::hasTextIndex(const StorageInMemoryMetadata & metadata)
{
    for (const auto & index : metadata.secondary_indices)
    {
        if (index.type == TEXT_INDEX_NAME)
            return true;
    }
    return false;
}

bool AlterCommands::hasVectorSimilarityIndex(const StorageInMemoryMetadata & metadata)
{
    for (const auto & index : metadata.secondary_indices)
    {
        if (index.type == "vector_similarity")
            return true;
    }
    return false;
}

namespace
{

/// Validate the old-to-proposed column transition before the candidate metadata is published.
/// Unavailable definitions retain their raw AST, so their bindings need independent evidence.
void validateUnavailableProjectionColumnTransition(
    const StorageInMemoryMetadata & metadata,
    const StorageInMemoryMetadata & metadata_copy)
{
    if (metadata_copy.columns != metadata.columns)
    {
        /// An unavailable projection cannot be rebuilt here. Reject changes that the stored
        /// query proves will break when it can be analyzed again: a missing referenced column,
        /// or a new type for an expression with an explicit output type. Keep wildcard
        /// expansion and untyped outputs free to follow source schema changes.
        auto check_projection = [&](const ASTPtr & definition_ast)
        {
            const auto & declaration = definition_ast->as<const ASTProjectionDeclaration &>();
            if (!declaration.query)
                return;
            const auto aliases = getProjectionAliases(declaration);

            const auto * query = declaration.query->as<ASTProjectionSelectQuery>();
            if (query && projectionNestedMatcherChangesShape(
                    *query, metadata.columns, metadata_copy.columns, aliases))
                throw Exception(
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Cannot change columns because projection {} has a nested matcher whose expansion would change",
                    backQuote(declaration.name));

            for (const auto & old_column : metadata.columns)
            {
                const bool explicit_type_depends_on_column = explicitProjectionColumnTypeDependsOn(
                    declaration, old_column.name, metadata.columns, aliases);
                const bool matcher_becomes_empty = !metadata_copy.columns.has(old_column.name) && query
                    && projectionMatcherBecomesEmpty(*query, old_column.name, metadata_copy.columns, aliases);
                const bool nested_matcher_depends_on_column = query && projectionNestedMatcherReferencesColumn(
                    *query, old_column.name, metadata.columns, aliases);
                if (!projectionQueryReferencesColumn(
                        *declaration.query, old_column.name, metadata.columns, aliases,
                        /*expand_table_aliases=*/true)
                    && !explicit_type_depends_on_column && !matcher_becomes_empty && !nested_matcher_depends_on_column)
                    continue;

                if (!metadata_copy.columns.has(old_column.name))
                    throw Exception(
                        ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Cannot remove or rename column {} because projection {} references it",
                        backQuote(old_column.name), backQuote(declaration.name));

                if (explicit_type_depends_on_column
                    && old_column.type->getName() != metadata_copy.columns.get(old_column.name).type->getName())
                    throw Exception(
                        ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Cannot change type of column {} because projection {} references it and declares an explicit column type",
                        backQuote(old_column.name), backQuote(declaration.name));

                if (old_column.type->getName() != metadata_copy.columns.get(old_column.name).type->getName())
                    checkUnavailableProjectionCodecTypeChange(
                        declaration, old_column.name, metadata.columns, metadata_copy.columns, aliases);

                for (const auto & subcolumn : metadata.columns.getSubcolumns(old_column.name))
                    if (!metadata_copy.columns.hasColumnOrSubcolumn(GetColumnsOptions::All, subcolumn.name)
                        && (projectionQueryReferencesColumn(
                                *declaration.query, subcolumn.name, metadata.columns, aliases, /*expand_table_aliases=*/true)
                            || projectionTupleElementReferencesSubcolumn(
                                *declaration.query, old_column.name, subcolumn.name, metadata.columns, metadata_copy.columns, aliases)))
                        throw Exception(
                            ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                            "Cannot change column {} because projection {} references a field that would no longer exist",
                            backQuote(old_column.name), backQuote(declaration.name));
            }

            if (projectionGroupByPositionChangesOutput(declaration, metadata.columns, metadata_copy.columns))
                throw Exception(
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Cannot change columns because projection {} has a positional GROUP BY reference "
                    "that would resolve to a different SELECT output",
                    backQuote(declaration.name));

            if (projectionOrderByMatcherChangesKey(declaration, metadata.columns, metadata_copy.columns))
                throw Exception(
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Cannot change columns because projection {} has an ORDER BY matcher "
                    "that would resolve to a different sorting key",
                    backQuote(declaration.name));
        };

        for (const auto & definition_ast : metadata_copy.projections.getUnavailableDefinitions())
            check_projection(definition_ast);
    }
}

}

void AlterCommands::apply(
    StorageInMemoryMetadata & metadata,
    ContextPtr context,
    bool share_nested_offsets,
    const MergeTreeSettings * settings_defaults) const
{
    if (!prepared)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Alter commands is not prepared. Cannot apply. It's a bug");

    auto metadata_copy = metadata;

    for (const AlterCommand & command : *this)
    {
        if (!command.ignore)
            command.apply(metadata_copy, context, share_nested_offsets, &metadata.columns, settings_defaults);
    }

    const bool columns_changed = metadata_copy.columns != metadata.columns;

    validatePreservedUnavailableProjections(metadata.projections, metadata_copy.projections);
    validateUnavailableProjectionColumnTransition(metadata, metadata_copy);

    /// Changes in columns may lead to changes in keys expression.
    metadata_copy.sorting_key.recalculateWithNewAST(metadata_copy.sorting_key.definition_ast, metadata_copy.columns, metadata_copy.virtuals, context);
    if (metadata_copy.primary_key.definition_ast != nullptr)
    {
        metadata_copy.primary_key = KeyDescription::getPrimaryKeyFromAST(
            metadata_copy.primary_key.definition_ast, metadata_copy.sorting_key, metadata_copy.columns, metadata_copy.virtuals, context);
    }
    else
    {
        metadata_copy.primary_key = KeyDescription::getKeyFromAST(metadata_copy.sorting_key.definition_ast, metadata_copy.columns, metadata_copy.virtuals, context);
        metadata_copy.primary_key.definition_ast = nullptr;
    }

    /// And in partition key expression
    if (metadata_copy.partition_key.definition_ast != nullptr)
        metadata_copy.partition_key.recalculateWithNewAST(metadata_copy.partition_key.definition_ast, metadata_copy.columns, metadata_copy.virtuals, context);

    /// Derived inputs and types can change even when the partition key output structure does not.
    if (metadata_copy.minmax_count_projection && columns_changed)
    {
        auto minmax_columns = metadata_copy.getColumnsRequiredForPartitionKey();
        auto partition_key = metadata_copy.partition_key.expression_list_ast->clone();
        FunctionNameNormalizer::visit(partition_key.get());
        metadata_copy.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            metadata_copy.columns, partition_key, minmax_columns, metadata_copy.primary_key, &metadata_copy.partition_key, context));
    }

    // /// And in sample key expression
    if (metadata_copy.sampling_key.definition_ast != nullptr)
        metadata_copy.sampling_key.recalculateWithNewAST(metadata_copy.sampling_key.definition_ast, metadata_copy.columns, metadata_copy.virtuals, context);

    /// Changes in columns may lead to changes in secondary indices
    const ColumnsDescription columns_with_virtuals = metadata_copy.getColumnsWithVirtuals();
    /// The resolved index type is persisted, so it must be the type a fresh reload resolves: analyse it
    /// in the global context, not in the session that happens to issue the `ALTER`.
    const ContextPtr index_context = context->getGlobalContext();
    for (auto & index : metadata_copy.secondary_indices)
    {
        try
        {
            index = IndexDescription::getIndexFromAST(
                index.definition_ast, columns_with_virtuals, index.isImplicitlyCreated(), index.escape_filenames, index_context);
        }
        catch (const Exception & exception)
        {
            throw Exception(exception.code(), "Cannot apply ALTER because it breaks skip index {}: {}", index.name, exception.message());
        }
    }

    /// Changes in columns may lead to changes in projections. An existing codec was checked
    /// against the user's settings when it was declared; rebuilding it checks compatibility with
    /// the new type and rejects lossy codecs, without depending on this ALTER's session settings.
    ProjectionsDescription new_projections;
    for (const auto & projection : metadata_copy.projections)
    {
        try
        {
            /// Check if we can still build projection from new metadata.
            auto new_projection = ProjectionDescription::getProjectionFromAST(projection.definition_ast, metadata_copy.columns, &metadata_copy.partition_key, context);
            /// Check if new metadata has the same keys as the old one.
            if (!blocksHaveEqualStructure(projection.sample_block_for_keys, new_projection.sample_block_for_keys))
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "Cannot ALTER column");
            /// Check if new metadata is convertible from old metadata for projection.
            Block old_projection_block = projection.sample_block;
            performRequiredConversions(old_projection_block, new_projection.sample_block.getNamesAndTypesList(), context, metadata_copy.getColumns().getDefaults());
            new_projections.add(std::move(new_projection));
        }
        catch (const Exception & exception)
        {
            throw Exception(exception.code(), "Cannot apply ALTER because it breaks projection {}: {}", projection.name, exception.message());
        }
    }
    for (const auto & definition_ast : metadata_copy.projections.getUnavailableDefinitions())
        new_projections.addUnavailable(definition_ast->clone());
    new_projections.preserveDeclarationOrder(metadata_copy.projections);
    metadata_copy.projections = std::move(new_projections);

    /// Changes in columns may lead to changes in TTL expressions.
    auto column_ttl_asts = metadata_copy.columns.getColumnTTLs();
    metadata_copy.column_ttls_by_name.clear();
    for (const auto & [name, ast] : column_ttl_asts)
    {
        try
        {
            auto new_ttl_entry = TTLDescription::getTTLFromAST(
                ast,
                metadata_copy.columns,
                context,
                metadata_copy.primary_key,
                context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions] ? TTLValidationMode::SkipValidation
                                                                                     : TTLValidationMode::Validate);
            metadata_copy.column_ttls_by_name[name] = new_ttl_entry;
        }
        catch (const Exception & exception)
        {
            throw Exception(
                exception.code(), "Cannot apply ALTER because it breaks the TTL of column {}: {}", backQuote(name), exception.message());
        }
    }

    if (metadata_copy.table_ttl.definition_ast != nullptr)
    {
        try
        {
            metadata_copy.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                metadata_copy.table_ttl.definition_ast,
                metadata_copy.columns,
                context,
                metadata_copy.primary_key,
                context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions] ? TTLValidationMode::SkipValidation
                                                                                     : TTLValidationMode::Validate);
        }
        catch (const Exception & exception)
        {
            throw Exception(exception.code(), "Cannot apply ALTER because it breaks the TTL of the table: {}", exception.message());
        }
    }

    metadata = std::move(metadata_copy);
}


void AlterCommands::prepare(const StorageInMemoryMetadata & metadata, bool share_nested_offsets)
{
    auto columns = metadata.columns;
    std::unordered_set<String> columns_with_full_type_modify;
    NameSet projection_names;
    for (const auto & projection : metadata.projections)
        projection_names.insert(projection.name);
    for (const auto & projection_name : metadata.projections.getUnavailableNames())
        projection_names.insert(projection_name);

    /// Used to tell whether a command restates the definition the table already has, so it must not
    /// depend on whether the redundant parentheses were written on one side and not on the other.
    auto ast_to_str = [](const ASTPtr & query) -> String
    {
        if (!query)
            return "";
        return query->formatIgnoringRedundantParentheses();
    };

    for (size_t i = 0; i < size(); ++i)
    {
        auto & command = (*this)[i];
        bool has_column = columns.has(command.column_name) || (share_nested_offsets && columns.hasNested(command.column_name));
        if (command.type == AlterCommand::MODIFY_COLUMN)
        {
            if (!has_column && command.if_exists)
                command.ignore = true;

            if (!command.ignore)
            {
                if (command.add_enum_values)
                {
                    if (columns_with_full_type_modify.contains(command.column_name))
                    {
                        throw Exception(
                            ErrorCodes::NOT_IMPLEMENTED,
                            "Cannot combine `MODIFY COLUMN` with an explicit type and `MODIFY COLUMN ... ADD ENUM VALUES` "
                            "in a single ALTER query");
                    }

                    /// `ADD ENUM VALUES` derives the resulting type by merging against the existing column, so
                    /// the column must be present in the working snapshot. If it is not (e.g. the column is added
                    /// by a preceding `ADD COLUMN` in the same statement, which does not advance the snapshot),
                    /// fail explicitly instead of silently dropping the modification.
                    if (!has_column)
                        throw Exception(
                            ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                            "Cannot ADD ENUM VALUES to column {}: it does not exist in the table. Adding enum values to a "
                            "column created in the same ALTER statement is not supported.",
                            backQuote(command.column_name));

                }
                else if (command.data_type)
                    columns_with_full_type_modify.emplace(command.column_name);
            }

            if (has_column)
            {
                const auto & column_from_table = columns.get(command.column_name);
                struct EnumTypeInfo
                {
                    const IDataTypeEnum * enum_type = nullptr;
                    bool is_nullable = false;
                    bool is_enum16 = false;
                };

                auto get_enum_type = [](const IDataType * dt) -> EnumTypeInfo
                {
                    const auto * column_enum_type = dynamic_cast<const IDataTypeEnum *>(dt);
                    if (column_enum_type)
                    {
                        bool is_enum16 = typeid_cast<const DataTypeEnum16 *>(column_enum_type);
                        return {column_enum_type, false, is_enum16};
                    }

                    const auto * column_nullable_type = dynamic_cast<const DataTypeNullable *>(dt);
                    if (column_nullable_type)
                    {
                        const auto * column_nullable_enum_type = dynamic_cast<const IDataTypeEnum *>(column_nullable_type->getNestedType().get());
                        if (column_nullable_enum_type)
                        {
                            bool is_enum16 = typeid_cast<const DataTypeEnum16 *>(column_nullable_enum_type);
                            return {column_nullable_enum_type, true, is_enum16};
                        }
                    }
                    return {};
                };

                if (command.add_enum_values)
                {
                    EnumTypeInfo eti = get_enum_type(column_from_table.type.get());
                    if (!eti.enum_type)
                        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot ADD ENUM VALUES to column {}", command.column_name);

                    DataTypePtr enum_dt = tryCreateAddToEnumType(command.add_enum_values, eti.is_enum16);
                    if (enum_dt)
                    {
                        const auto * column_enum_type = eti.enum_type;
                        if (const auto * alter_enum_type = dynamic_cast<const IDataTypeEnum *>(enum_dt.get());
                            alter_enum_type && alter_enum_type->isAdd())
                        {
                            if (const auto * base_enum8 = typeid_cast<const DataTypeEnum8 *>(column_enum_type))
                            {
                                if (const auto * add_enum8 = typeid_cast<const DataTypeEnum8 *>(alter_enum_type))
                                    command.data_type = mergeEnumTypes<Int8>(*base_enum8, *add_enum8);
                                else
                                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong Enum type");
                            }
                            else if (const auto * base_enum16 = typeid_cast<const DataTypeEnum16 *>(column_enum_type))
                            {
                                if (const auto * add_enum16 = typeid_cast<const DataTypeEnum16 *>(alter_enum_type))
                                    command.data_type = mergeEnumTypes<Int16>(*base_enum16, *add_enum16);
                                else
                                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong Enum type");
                            }
                            else
                            {
                                throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong Enum type");
                            }


                            if (eti.is_nullable)
                            {
                                command.data_type = std::make_shared<DataTypeNullable>(command.data_type);
                            }

                            /// Advance the working snapshot so that a subsequent command in the same
                            /// ALTER statement (e.g. another `ADD ENUM VALUES` on the same column) merges
                            /// against the already-extended type instead of the original one. Without this,
                            /// `MODIFY COLUMN x ADD ENUM VALUES('a'), MODIFY COLUMN x ADD ENUM VALUES('b')`
                            /// would lose `a`, because the commands are applied sequentially afterwards.
                            columns.modify(command.column_name, [&](ColumnDescription & col) { col.type = command.data_type; });
                        }
                    }
                }
            }
        }
        else if (command.type == AlterCommand::ADD_COLUMN)
        {
            if (has_column && command.if_not_exists)
                command.ignore = true;
        }
        else if (command.type == AlterCommand::ADD_PROJECTION)
        {
            if (command.if_not_exists && projection_names.contains(command.projection_name))
                command.ignore = true;
            projection_names.insert(command.projection_name);
        }
        else if (command.type == AlterCommand::DROP_PROJECTION && !command.partition && !command.clear)
            projection_names.erase(command.projection_name);
        else if (command.type == AlterCommand::DROP_COLUMN
                || command.type == AlterCommand::COMMENT_COLUMN
                || command.type == AlterCommand::RENAME_COLUMN)
        {
            if (!has_column && command.if_exists)
                command.ignore = true;
        }
        else if (command.type == AlterCommand::MODIFY_ORDER_BY)
        {
            if (ast_to_str(command.order_by) == ast_to_str(metadata.sorting_key.definition_ast))
                command.ignore = true;
        }
    }

    prepared = true;
}


void AlterCommands::validate(const StoragePtr & table, ContextPtr context) const
{
    const auto metadata = table->getInMemoryMetadataPtr(context, false);
    const auto virtuals = metadata->virtuals;

    bool share_nested = true;
    if (auto * merge_tree = dynamic_cast<MergeTreeData *>(table.get()))
        share_nested = (*merge_tree->getSettings())[MergeTreeSetting::share_nested_offsets];

    auto all_columns = metadata->columns;
    /// Default expression for all added/modified columns
    ASTPtr default_expr_list = make_intrusive<ASTExpressionList>();
    /// Columns whose default is evaluated at insert time (DEFAULT, MATERIALIZED); their expressions
    /// must not reference virtual columns. An external-target (`TO`) materialized view forwards inserts
    /// to its target using the target metadata and never evaluates its own column defaults, so a default
    /// over a virtual column is inert there and is left out of this set.
    NameSet insert_time_default_columns;
    bool defaults_evaluated_at_insert_time = true;
    if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table.get()))
        defaults_evaluated_at_insert_time = mv->hasInnerTable();
    NameSet modified_columns;
    NameSet renamed_columns;
    /// Projection names need a statement-local snapshot. In particular, an
    /// `ADD PROJECTION IF NOT EXISTS` whose name is already taken is a no-op, while
    /// a plain duplicate must report the name conflict before its codec is validated.
    NameSet projection_names;
    for (const auto & projection : metadata->projections)
        projection_names.insert(projection.name);
    for (const auto & projection_name : metadata->projections.getUnavailableNames())
        projection_names.insert(projection_name);
    const CodecValidationSettings codec_validation_settings(context->getSettingsRef());

    const bool validate_projection_codecs = shouldValidateProjectionCodecsOnAlter(context);

    for (size_t i = 0; i < size(); ++i)
    {
        const auto & command = (*this)[i];

        if (command.ttl && !table->supportsTTL())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine {} doesn't support TTL clause", table->getName());

        /// A constraint expression is evaluated per block and read by block row, so an `arrayJoin` inside
        /// it would check a row against another row's value, or read past the end of a shorter column.
        /// `MODIFY CONSTRAINT` replaces the stored declaration in place, so it installs a new expression
        /// just like `ADD CONSTRAINT` does. Screened wherever the expression is stated, whether or not
        /// `apply()` goes on to install it, so that the answer does not depend on the name being taken.
        if ((command.type == AlterCommand::ADD_CONSTRAINT || command.type == AlterCommand::MODIFY_CONSTRAINT)
            && command.constraint_decl)
            ConstraintsDescription({command.constraint_decl}).checkExpressionsPreserveRowCount();

        /// `column_statistics_decl` covers the column-declaration spelling
        /// `ALTER TABLE t ADD/MODIFY COLUMN c UInt64 STATISTICS(...)`, which must honor the same
        /// gate as the dedicated `ADD/DROP/MODIFY STATISTICS` commands.
        if ((command.type == AlterCommand::ADD_STATISTICS || command.type == AlterCommand::DROP_STATISTICS
             || command.type == AlterCommand::MODIFY_STATISTICS || command.column_statistics_decl != nullptr)
            && !context->getSettingsRef()[Setting::allow_statistics])
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Alter table with statistics is disabled. Turn on allow_statistics");

        /// Storages that reject the dedicated `ADD/DROP/MODIFY STATISTICS` commands in `checkAlterIsPossible`
        /// must not accept statistics through the column-declaration spelling either, so that engine support
        /// doesn't depend on how the same logical alter is spelled.
        if (command.column_statistics_decl != nullptr && !table->supportsStatistics())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Engine {} doesn't support statistics", table->getName());

        const auto & column_name = command.column_name;
        if (command.type == AlterCommand::ADD_COLUMN)
        {
            if (all_columns.has(column_name) || (share_nested && all_columns.hasNested(column_name)))
            {
                if (!command.if_not_exists)
                    throw Exception(ErrorCodes::DUPLICATE_COLUMN,
                                    "Cannot add column {}: column with this name already exists",
                                    backQuote(column_name));
                continue;
            }

            if (!command.data_type)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Data type have to be specified for column {} to add", backQuote(column_name));

            validateDataType(command.data_type, DataTypeValidationSettings(context->getSettingsRef()));
            checkAllTypesAreAllowedInTable(NamesAndTypesList{{command.column_name, command.data_type}});

            if (virtuals.tryGet(column_name, VirtualsKind::Persistent, VirtualsMaterializationPlace::All))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot add column {}: this column name is reserved for persistent virtual column", backQuote(column_name));

            if (command.default_kind == ColumnDefaultKind::Ephemeral
                && virtuals.tryGet(column_name, VirtualsKind::Ephemeral, VirtualsMaterializationPlace::All))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot add ephemeral column {}: it conflicts with a virtual column of the same name",
                    backQuote(column_name));

            if (command.codec)
            {
                CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                    command.codec,
                    command.data_type,
                    codec_validation_settings);
            }

            /// Advance the working snapshot with the exact columns apply() would materialize
            /// (flatten_nested expansion), not a synthetic top-level `n`, so a later command in the
            /// same ALTER that targets a real flattened child (e.g. RENAME COLUMN `n.b`) sees it.
            for (auto & col : columnsAddedByAlter(all_columns, ColumnDescription(column_name, command.data_type),
                                                  context, command.if_not_exists, share_nested))
                all_columns.add(std::move(col));
        }
        else if (command.type == AlterCommand::MODIFY_COLUMN)
        {
            if (!all_columns.has(column_name))
            {
                if (!command.if_exists)
                {
                    throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Wrong column. Cannot find column {} to modify{}",
                                    backQuote(column_name), all_columns.getHintsMessage(column_name));
                }
                continue;
            }

            if (renamed_columns.contains(column_name))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot rename and modify the same column {} "
                                                             "in a single ALTER query", backQuote(column_name));

            if (command.default_kind == ColumnDefaultKind::Ephemeral
                && virtuals.tryGet(column_name, VirtualsKind::Ephemeral, VirtualsMaterializationPlace::All))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot modify column {} to ephemeral: it conflicts with a virtual column of the same name",
                    backQuote(column_name));

            if (command.codec)
            {
                /// `default_kind` holds its enumerator's zero value unless `default_expression` is set.
                const bool becomes_physical = command.default_expression
                    && (command.default_kind == ColumnDefaultKind::Default || command.default_kind == ColumnDefaultKind::Materialized);
                if (all_columns.hasAlias(column_name) && !becomes_physical)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot specify codec for column type ALIAS");
                /// The type is optional here, and a codec can resolve differently per type, so
                /// validate against the type the column will have, as `apply` does.
                CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                    command.codec,
                    command.data_type ? command.data_type : all_columns.get(column_name).type,
                    codec_validation_settings);
            }
            auto column_default = all_columns.getDefault(column_name);
            if (column_default)
            {
                if (command.to_remove == AlterCommand::RemoveProperty::DEFAULT && column_default->kind != ColumnDefaultKind::Default)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot remove DEFAULT from column {}, because column default type is {}. Use REMOVE {} to delete it",
                            backQuote(column_name), toString(column_default->kind), toString(column_default->kind));
                }
                if (command.to_remove == AlterCommand::RemoveProperty::MATERIALIZED && column_default->kind != ColumnDefaultKind::Materialized)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot remove MATERIALIZED from column {}, because column default type is {}. Use REMOVE {} to delete it",
                        backQuote(column_name), toString(column_default->kind), toString(column_default->kind));
                }
                if (command.to_remove == AlterCommand::RemoveProperty::ALIAS && column_default->kind != ColumnDefaultKind::Alias)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot remove ALIAS from column {}, because column default type is {}. Use REMOVE {} to delete it",
                        backQuote(column_name), toString(column_default->kind), toString(column_default->kind));
                }
            }

            /// FIXME: Modifying the column to/from Object(JSON) is broken.
            /// Looks like there is something around default expression for this column (method `getDefault` is not implemented for the data type Object).
            /// But after ALTER TABLE MODIFY COLUMN we need to fill existing rows with something (exactly the default value) or calculate the common type for it.
            /// So we don't allow to do it for now.
            if (command.data_type)
            {
                validateDataType(command.data_type, DataTypeValidationSettings(context->getSettingsRef()));
                checkAllTypesAreAllowedInTable(NamesAndTypesList{{command.column_name, command.data_type}});

                const GetColumnsOptions options(GetColumnsOptions::All);
                const auto old_data_type = all_columns.getColumn(options, column_name).type;
            }

            if (command.isRemovingProperty())
            {
                if (!column_default && command.to_remove == AlterCommand::RemoveProperty::DEFAULT)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have DEFAULT, cannot remove it",
                        backQuote(column_name));

                if (!column_default && command.to_remove == AlterCommand::RemoveProperty::ALIAS)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have ALIAS, cannot remove it",
                        backQuote(column_name));

                if (!column_default && command.to_remove == AlterCommand::RemoveProperty::MATERIALIZED)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have MATERIALIZED, cannot remove it",
                        backQuote(column_name));

                const auto & column_from_table = all_columns.get(column_name);
                if (command.to_remove == AlterCommand::RemoveProperty::TTL && column_from_table.ttl == nullptr)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have TTL, cannot remove it",
                        backQuote(column_name));
                if (command.to_remove == AlterCommand::RemoveProperty::CODEC && column_from_table.codec == nullptr)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have CODEC, cannot remove it",
                        backQuote(column_name));
                if (command.to_remove == AlterCommand::RemoveProperty::COMMENT && column_from_table.comment.empty())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have COMMENT, cannot remove it",
                        backQuote(column_name));
            }

            /// Later commands in this ALTER are applied after this type change. Validate them
            /// against the same ordered column snapshot that apply() will see.
            if (command.data_type)
                all_columns.modify(column_name, [&](ColumnDescription & column) { column.type = command.data_type; });

            modified_columns.emplace(column_name);
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            if (all_columns.has(command.column_name) || (share_nested && all_columns.hasNested(command.column_name)))
            {
                if (!command.clear) /// CLEAR column is Ok even if there are dependencies.
                {
                    /// Check if we are going to DROP a column that some other columns depend on.
                    {
                        auto execution_context = Context::createCopy(context);
                        auto dummy_storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, all_columns);
                        auto fake_table_expression = std::make_shared<TableNode>(dummy_storage, execution_context);
                        for (const ColumnDescription & column : all_columns)
                        {
                            if (const auto & default_expression = column.default_desc.expression)
                            {
                                auto expression = buildQueryTree(default_expression->clone(), execution_context);
                                QueryAnalyzer analyzer(true);
                                analyzer.resolve(expression, fake_table_expression, execution_context);
                                GlobalPlannerContextPtr global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
                                auto planner_context = std::make_shared<PlannerContext>(execution_context, global_planner_context, SelectQueryOptions{});
                                collectSetsAndSourceColumns(expression, planner_context);
                                if (const auto * table_expression = planner_context->getTableExpressionDataOrNull(fake_table_expression))
                                {
                                    for (const auto & selected_column : table_expression->getSelectedColumnsNames())
                                    {
                                        auto column_name_and_type = all_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, selected_column);
                                        if (column_name_and_type && column_name_and_type->getNameInStorage() == command.column_name)
                                            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot drop column {}, because column {} depends on it", backQuote(command.column_name), backQuote(column.name));
                                    }
                                }
                            }
                        }
                    }
                }
                all_columns.remove(command.column_name);
            }
            else if (!command.if_exists)
            {
                 auto message = PreformattedMessage::create(
                    "Wrong column name. Cannot find column {} to drop", backQuote(command.column_name));
                all_columns.appendHintsMessage(message.text, command.column_name);
                throw Exception(std::move(message), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
            }
        }
        else if (command.type == AlterCommand::COMMENT_COLUMN)
        {
            if (!all_columns.has(command.column_name))
            {
                if (!command.if_exists)
                {
                    auto message = PreformattedMessage::create(
                        "Wrong column name. Cannot find column {} to comment", backQuote(command.column_name));
                    all_columns.appendHintsMessage(message.text, command.column_name);
                    throw Exception(std::move(message), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
                }
            }
        }
        else if (command.type == AlterCommand::RESET_SETTING)
        {
            if (metadata->settings_changes == nullptr)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot alter settings, because table engine doesn't support settings changes");
        }
        else if (command.type == AlterCommand::RENAME_COLUMN)
        {
           for (size_t j = i + 1; j < size(); ++j)
           {
               auto next_command = (*this)[j];
               if (next_command.type == AlterCommand::RENAME_COLUMN)
               {
                   if (next_command.column_name == command.rename_to)
                       throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transitive renames in a single ALTER query are not allowed (don't make sense)");
                   if (next_command.column_name == command.column_name)
                       throw Exception(
                           ErrorCodes::BAD_ARGUMENTS,
                           "Cannot rename column '{}' to two different names in a single ALTER query",
                           backQuote(command.column_name));
               }
           }

            /// TODO Implement nested rename
            if (all_columns.hasNested(command.column_name))
            {
                bool skip = false;
                if (auto * merge_tree = dynamic_cast<MergeTreeData *>(table.get()))
                    skip = !(*merge_tree->getSettings())[MergeTreeSetting::share_nested_offsets];
                if (!skip)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot rename whole Nested struct");
            }

            if (!all_columns.has(command.column_name))
            {
                if (!command.if_exists)
                {
                    auto message = PreformattedMessage::create(
                       "Wrong column name. Cannot find column {} to rename", backQuote(command.column_name));
                    all_columns.appendHintsMessage(message.text, command.column_name);
                    throw Exception(std::move(message), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
                }
                continue;
            }

            if (all_columns.has(command.rename_to))
                throw Exception(ErrorCodes::DUPLICATE_COLUMN,
                    "Cannot rename to {}: column with this name already exists", backQuote(command.rename_to));

            if (virtuals.tryGet(command.rename_to, VirtualsKind::Persistent, VirtualsMaterializationPlace::All))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot rename to {}: this column name is reserved for persistent virtual column", backQuote(command.rename_to));

            if (all_columns.get(command.column_name).default_desc.kind == ColumnDefaultKind::Ephemeral
                && virtuals.tryGet(command.rename_to, VirtualsKind::Ephemeral, VirtualsMaterializationPlace::All))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot rename ephemeral column to {}: it conflicts with a virtual column of the same name",
                    backQuote(command.rename_to));

            if (modified_columns.contains(column_name))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot rename and modify the same column {} "
                                                             "in a single ALTER query", backQuote(column_name));

            String from_nested_table_name = Nested::extractTableName(command.column_name);
            String to_nested_table_name = Nested::extractTableName(command.rename_to);
            bool from_nested = from_nested_table_name != command.column_name;
            bool to_nested = to_nested_table_name != command.rename_to;

            /// When share_nested_offsets is disabled, dotted-name columns are independent
            /// and not part of a Nested group, so they can be freely renamed.
            if (auto * merge_tree = dynamic_cast<MergeTreeData *>(table.get()))
            {
                if (!(*merge_tree->getSettings())[MergeTreeSetting::share_nested_offsets])
                {
                    from_nested = false;
                    to_nested = false;
                }
            }

            if (from_nested && to_nested)
            {
                if (from_nested_table_name != to_nested_table_name)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot rename column from one nested name to another");
                all_columns.rename(command.column_name, command.rename_to);
                renamed_columns.emplace(command.column_name);
                renamed_columns.emplace(command.rename_to);
            }
            else if (!from_nested && !to_nested)
            {
                all_columns.rename(command.column_name, command.rename_to);
                renamed_columns.emplace(command.column_name);
                renamed_columns.emplace(command.rename_to);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot rename column from nested struct to normal column and vice versa");
            }
        }
        else if (command.type == AlterCommand::REMOVE_TTL && !metadata->hasAnyTableTTL())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table doesn't have any table TTL expression, cannot remove");
        }
        else if (command.type == AlterCommand::REMOVE_SAMPLE_BY && !metadata->hasSamplingKey())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table doesn't have SAMPLE BY, cannot remove");
        }
        else if (command.type == AlterCommand::ADD_PROJECTION)
        {
            /// Building the projection here would otherwise move failures for every other
            /// `ADD PROJECTION` from `apply` to this point.
            if (validate_projection_codecs
                && !projection_names.contains(command.projection_name)
                && command.projection_decl->as<const ASTProjectionDeclaration &>().columns)
            {
                auto projection = ProjectionDescription::getProjectionFromAST(
                    command.projection_decl,
                    all_columns,
                    &metadata->partition_key,
                    context,
                    LoadingStrictnessLevel::CREATE);
                ProjectionDescription::validateDeclaredColumnCodecs(projection, context, LoadingStrictnessLevel::CREATE);
            }
            projection_names.insert(command.projection_name);
        }
        else if (command.type == AlterCommand::DROP_PROJECTION && !command.partition && !command.clear)
            projection_names.erase(command.projection_name);

        /// Collect default expressions for MODIFY and ADD commands
        if (command.type == AlterCommand::MODIFY_COLUMN || command.type == AlterCommand::ADD_COLUMN)
        {
            if (command.default_expression)
            {
                DataTypePtr data_type_ptr;
                /// If we modify default, but not type.
                if (!command.data_type) /// it's not ADD COLUMN, because we cannot add column without type
                    data_type_ptr = all_columns.get(column_name).type;
                else
                    data_type_ptr = command.data_type;

                const auto & final_column_name = column_name;
                const auto tmp_column_name = final_column_name + "_tmp_alter" + toString(randomSeed());

                default_expr_list->children.emplace_back(setAlias(
                    addTypeConversionToAST(make_intrusive<ASTIdentifier>(tmp_column_name), data_type_ptr->getName()),
                    final_column_name));

                default_expr_list->children.emplace_back(setAlias(command.default_expression->clone(), tmp_column_name));

                if (defaults_evaluated_at_insert_time
                    && (command.default_kind == ColumnDefaultKind::Default || command.default_kind == ColumnDefaultKind::Materialized))
                    insert_time_default_columns.insert(final_column_name);
            } /// if we change data type for column with default
            else if (all_columns.has(column_name) && command.data_type)
            {
                const auto & column_in_table = all_columns.get(column_name);
                /// Column doesn't have a default, nothing to check
                if (!column_in_table.default_desc.expression)
                    continue;

                const auto & final_column_name = column_name;
                const auto tmp_column_name = final_column_name + "_tmp_alter" + toString(randomSeed());
                const auto data_type_ptr = command.data_type;

                default_expr_list->children.emplace_back(setAlias(
                    addTypeConversionToAST(make_intrusive<ASTIdentifier>(tmp_column_name), data_type_ptr->getName()), final_column_name));

                default_expr_list->children.emplace_back(setAlias(column_in_table.default_desc.expression->clone(), tmp_column_name));

                if (defaults_evaluated_at_insert_time
                    && (column_in_table.default_desc.kind == ColumnDefaultKind::Default || column_in_table.default_desc.kind == ColumnDefaultKind::Materialized))
                    insert_time_default_columns.insert(final_column_name);
            }
        }
    }

    /// Parameterized views do not have 'columns' in their metadata
    bool is_parameterized_view = table->as<StorageView>() && table->as<StorageView>()->isParameterizedView();

    if (!is_parameterized_view && all_columns.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot DROP or CLEAR all columns");

    validateColumnsDefaultsAndGetSampleBlock(default_expr_list, all_columns.getAll(), context, insert_time_default_columns);
}

bool AlterCommands::hasNonReplicatedAlterCommand() const
{
    return std::any_of(begin(), end(), [](const AlterCommand & c) { return c.isSettingsAlter() || c.isCommentAlter(); });
}

bool AlterCommands::areNonReplicatedAlterCommands() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isSettingsAlter() || c.isCommentAlter(); });
}

bool AlterCommands::isSettingsAlter() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isSettingsAlter(); });
}

bool AlterCommands::isCommentAlter() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isCommentAlter(); });
}

static MutationCommand createMaterializeTTLCommand()
{
    MutationCommand command;
    auto ast = make_intrusive<ASTAlterCommand>();
    ast->type = ASTAlterCommand::MATERIALIZE_TTL;
    command.type = MutationCommand::MATERIALIZE_TTL;
    command.ast_text = ast->formatWithSecretsOneLine();
    return command;
}

MutationCommands AlterCommands::getMutationCommands(StorageInMemoryMetadata metadata, bool materialize_ttl, ContextPtr context, bool with_alters, bool share_nested_offsets) const
{
    /// Save a copy of the original metadata before applying commands.
    /// We need it for isTTLAlter check below, because apply() updates TTL in metadata,
    /// making it impossible to detect TTL changes afterwards.
    const StorageInMemoryMetadata original_metadata = metadata;

    /// Remove implicit statistics before applying commands to the metadata copy.
    /// This is needed because `getMutationCommands` may be called before `removeImplicitStatistics`
    /// in the ALTER flow (e.g. in StorageMergeTree::alter), and applying ADD_STATISTICS
    /// to metadata that already contains auto-added statistics would throw a duplicate error.
    removeImplicitStatistics(metadata.columns);

    const auto & settings = context->getSettingsRef();
    const UInt64 max_parser_depth = settings[Setting::max_parser_depth];
    const UInt64 max_parser_backtracks = settings[Setting::max_parser_backtracks];

    MutationCommands result;
    for (const auto & alter_cmd : *this)
    {
        if (auto mutation_cmd = alter_cmd.tryConvertToMutationCommand(metadata, context, share_nested_offsets); mutation_cmd)
        {
            result.push_back(*mutation_cmd);
        }
        else if (with_alters)
        {
            result.push_back(MutationCommand{
                .ast_text = alter_cmd.ast->formatWithSecretsOneLine(),
                .max_parser_depth = max_parser_depth,
                .max_parser_backtracks = max_parser_backtracks,
                .type = MutationCommand::Type::ALTER_WITHOUT_MUTATION,
            });
        }
    }

    if (materialize_ttl)
    {
        for (const auto & alter_cmd : *this)
        {
            if (alter_cmd.isTTLAlter(original_metadata))
            {
                result.push_back(createMaterializeTTLCommand());
                break;
            }
        }
    }

    return result;
}

}
