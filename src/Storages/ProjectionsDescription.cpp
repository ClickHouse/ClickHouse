#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/ProjectionsDescription.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>

#include <Core/Defines.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/SquashingChunksTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <base/range.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int NO_SUCH_PROJECTION_IN_TABLE;
    extern const int ILLEGAL_PROJECTION;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

bool ProjectionDescription::isPrimaryKeyColumnPossiblyWrappedInFunctions(const ASTPtr & node) const
{
    const String column_name = node->getColumnName();

    for (const auto & key_name : metadata->getPrimaryKeyColumns())
        if (column_name == key_name)
            return true;

    if (const auto * func = node->as<ASTFunction>())
        if (func->arguments->children.size() == 1)
            return isPrimaryKeyColumnPossiblyWrappedInFunctions(func->arguments->children.front());

    return false;
}


ProjectionDescription ProjectionDescription::clone() const
{
    ProjectionDescription other;
    if (definition_ast)
        other.definition_ast = definition_ast->clone();
    if (query_ast)
        other.query_ast = query_ast->clone();

    other.name = name;
    other.type = type;
    other.required_columns = required_columns;
    other.sample_block = sample_block;
    other.sample_block_for_keys = sample_block_for_keys;
    other.metadata = metadata;
    other.key_size = key_size;
    other.is_minmax_count_projection = is_minmax_count_projection;
    other.primary_key_max_column_name = primary_key_max_column_name;
    other.partition_value_indices = partition_value_indices;

    return other;
}

ProjectionsDescription ProjectionsDescription::clone() const
{
    ProjectionsDescription other;
    for (const auto & projection : projections)
        other.add(projection.clone());

    return other;
}

bool ProjectionDescription::operator==(const ProjectionDescription & other) const
{
    return name == other.name && queryToString(definition_ast) == queryToString(other.definition_ast);
}

ProjectionDescription
ProjectionDescription::getProjectionFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr query_context)
{
    const auto * projection_definition = definition_ast->as<ASTProjectionDeclaration>();

    if (!projection_definition)
        throw Exception("Cannot create projection from non ASTProjectionDeclaration AST", ErrorCodes::INCORRECT_QUERY);

    if (projection_definition->name.empty())
        throw Exception("Projection must have name in definition.", ErrorCodes::INCORRECT_QUERY);

    if (!projection_definition->query)
        throw Exception("QUERY is required for projection", ErrorCodes::INCORRECT_QUERY);

    ProjectionDescription result;
    result.definition_ast = projection_definition->clone();
    result.name = projection_definition->name;

    auto query = projection_definition->query->as<ASTProjectionSelectQuery &>();
    result.query_ast = query.cloneToASTSelect();

    auto external_storage_holder = std::make_shared<TemporaryTableHolder>(query_context, columns, ConstraintsDescription{});
    StoragePtr storage = external_storage_holder->getTable();
    InterpreterSelectQuery select(
        result.query_ast, query_context, storage, {},
        /// Here we ignore ast optimizations because otherwise aggregation keys may be removed from result header as constants.
        SelectQueryOptions{QueryProcessingStage::WithMergeableState}.modify().ignoreAlias().ignoreASTOptimizations());

    result.required_columns = select.getRequiredColumns();
    result.sample_block = select.getSampleBlock();

    StorageInMemoryMetadata metadata;
    metadata.partition_key = KeyDescription::buildEmptyKey();

    const auto & query_select = result.query_ast->as<const ASTSelectQuery &>();
    if (select.hasAggregation())
    {
        if (query.orderBy())
            throw Exception(
                "When aggregation is used in projection, ORDER BY cannot be specified", ErrorCodes::ILLEGAL_PROJECTION);

        result.type = ProjectionDescription::Type::Aggregate;
        if (const auto & group_expression_list = query_select.groupBy())
        {
            ASTPtr order_expression;
            if (group_expression_list->children.size() == 1)
            {
                result.key_size = 1;
                order_expression = std::make_shared<ASTIdentifier>(group_expression_list->children.front()->getColumnName());
            }
            else
            {
                auto function_node = std::make_shared<ASTFunction>();
                function_node->name = "tuple";
                function_node->arguments = group_expression_list->clone();
                result.key_size = function_node->arguments->children.size();
                for (auto & child : function_node->arguments->children)
                    child = std::make_shared<ASTIdentifier>(child->getColumnName());
                function_node->children.push_back(function_node->arguments);
                order_expression = function_node;
            }
            auto columns_with_state = ColumnsDescription(result.sample_block.getNamesAndTypesList());
            metadata.sorting_key = KeyDescription::getSortingKeyFromAST(order_expression, columns_with_state, query_context, {});
            metadata.primary_key = KeyDescription::getKeyFromAST(order_expression, columns_with_state, query_context);
            metadata.primary_key.definition_ast = nullptr;
        }
        else
        {
            metadata.sorting_key = KeyDescription::buildEmptyKey();
            metadata.primary_key = KeyDescription::buildEmptyKey();
        }
        for (const auto & key : select.getQueryAnalyzer()->aggregationKeys())
            result.sample_block_for_keys.insert({nullptr, key.type, key.name});
    }
    else
    {
        result.type = ProjectionDescription::Type::Normal;
        metadata.sorting_key = KeyDescription::getSortingKeyFromAST(query.orderBy(), columns, query_context, {});
        metadata.primary_key = KeyDescription::getKeyFromAST(query.orderBy(), columns, query_context);
        metadata.primary_key.definition_ast = nullptr;
    }

    auto block = result.sample_block;
    for (const auto & [name, type] : metadata.sorting_key.expression->getRequiredColumnsWithTypes())
        block.insertUnique({nullptr, type, name});
    for (const auto & column_with_type_name : block)
    {
        if (column_with_type_name.column && isColumnConst(*column_with_type_name.column))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Projections cannot contain constant columns: {}", column_with_type_name.name);
    }

    metadata.setColumns(ColumnsDescription(block.getNamesAndTypesList()));
    result.metadata = std::make_shared<StorageInMemoryMetadata>(metadata);
    return result;
}

ProjectionDescription ProjectionDescription::getMinMaxCountProjection(
    const ColumnsDescription & columns,
    ASTPtr partition_columns,
    const Names & minmax_columns,
    const ASTs & primary_key_asts,
    ContextPtr query_context)
{
    ProjectionDescription result;
    result.is_minmax_count_projection = true;

    auto select_query = std::make_shared<ASTProjectionSelectQuery>();
    ASTPtr select_expression_list = std::make_shared<ASTExpressionList>();
    for (const auto & column : minmax_columns)
    {
        select_expression_list->children.push_back(makeASTFunction("min", std::make_shared<ASTIdentifier>(column)));
        select_expression_list->children.push_back(makeASTFunction("max", std::make_shared<ASTIdentifier>(column)));
    }
    if (!primary_key_asts.empty())
    {
        select_expression_list->children.push_back(makeASTFunction("min", primary_key_asts.front()->clone()));
        select_expression_list->children.push_back(makeASTFunction("max", primary_key_asts.front()->clone()));
    }
    select_expression_list->children.push_back(makeASTFunction("count"));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::SELECT, std::move(select_expression_list));

    if (partition_columns && !partition_columns->children.empty())
    {
        partition_columns = partition_columns->clone();
        for (const auto & partition_column : partition_columns->children)
            KeyDescription::moduloToModuloLegacyRecursive(partition_column);
        select_query->setExpression(ASTProjectionSelectQuery::Expression::GROUP_BY, partition_columns->clone());
    }

    result.definition_ast = select_query;
    result.name = MINMAX_COUNT_PROJECTION_NAME;
    result.query_ast = select_query->cloneToASTSelect();

    auto external_storage_holder = std::make_shared<TemporaryTableHolder>(query_context, columns, ConstraintsDescription{});
    StoragePtr storage = external_storage_holder->getTable();
    InterpreterSelectQuery select(
        result.query_ast, query_context, storage, {},
        /// Here we ignore ast optimizations because otherwise aggregation keys may be removed from result header as constants.
        SelectQueryOptions{QueryProcessingStage::WithMergeableState}.modify().ignoreAlias().ignoreASTOptimizations());
    result.required_columns = select.getRequiredColumns();
    result.sample_block = select.getSampleBlock();

    std::map<String, size_t> partition_column_name_to_value_index;
    if (partition_columns)
    {
        for (auto i : collections::range(partition_columns->children.size()))
            partition_column_name_to_value_index[partition_columns->children[i]->getColumnNameWithoutAlias()] = i;
    }

    const auto & analysis_result = select.getAnalysisResult();
    if (analysis_result.need_aggregate)
    {
        for (const auto & key : select.getQueryAnalyzer()->aggregationKeys())
        {
            result.sample_block_for_keys.insert({nullptr, key.type, key.name});
            auto it = partition_column_name_to_value_index.find(key.name);
            if (it == partition_column_name_to_value_index.end())
                throw Exception("minmax_count projection can only have keys about partition columns. It's a bug", ErrorCodes::LOGICAL_ERROR);
            result.partition_value_indices.push_back(it->second);
        }
    }

    /// If we have primary key and it's not in minmax_columns, it will be used as one additional minmax columns.
    if (!primary_key_asts.empty()
        && result.sample_block.columns()
            == 2 * (minmax_columns.size() + 1) /* minmax columns */ + 1 /* count() */
                + result.partition_value_indices.size() /* partition_columns */)
    {
        /// partition_expr1, partition_expr2, ..., min(p1), max(p1), min(p2), max(p2), ..., min(k1), max(k1), count()
        ///                                                                                              ^
        ///                                                                                           size - 2
        result.primary_key_max_column_name = *(result.sample_block.getNames().cend() - 2);
    }
    result.type = ProjectionDescription::Type::Aggregate;
    StorageInMemoryMetadata metadata;
    metadata.setColumns(ColumnsDescription(result.sample_block.getNamesAndTypesList()));
    metadata.partition_key = KeyDescription::buildEmptyKey();
    metadata.sorting_key = KeyDescription::buildEmptyKey();
    metadata.primary_key = KeyDescription::buildEmptyKey();
    result.metadata = std::make_shared<StorageInMemoryMetadata>(metadata);
    return result;
}


void ProjectionDescription::recalculateWithNewColumns(const ColumnsDescription & new_columns, ContextPtr query_context)
{
    *this = getProjectionFromAST(definition_ast, new_columns, query_context);
}


Block ProjectionDescription::calculate(const Block & block, ContextPtr context) const
{
    auto builder = InterpreterSelectQuery(
                       query_ast,
                       context,
                       Pipe(std::make_shared<SourceFromSingleChunk>(block)),
                       SelectQueryOptions{
                           type == ProjectionDescription::Type::Normal ? QueryProcessingStage::FetchColumns
                                                                       : QueryProcessingStage::WithMergeableState})
                       .buildQueryPipeline();
    builder.resize(1);
    builder.addTransform(std::make_shared<SquashingChunksTransform>(builder.getHeader(), block.rows(), block.bytes()));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingPipelineExecutor executor(pipeline);
    Block ret;
    executor.pull(ret);
    if (executor.pull(ret))
        throw Exception("Projection cannot increase the number of rows in a block. It's a bug", ErrorCodes::LOGICAL_ERROR);
    return ret;
}


String ProjectionsDescription::toString() const
{
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & projection : projections)
        list.children.push_back(projection.definition_ast);

    return serializeAST(list, true);
}

ProjectionsDescription ProjectionsDescription::parse(const String & str, const ColumnsDescription & columns, ContextPtr query_context)
{
    ProjectionsDescription result;
    if (str.empty())
        return result;

    ParserProjectionDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    for (const auto & projection_ast : list->children)
    {
        auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, columns, query_context);
        result.add(std::move(projection));
    }

    return result;
}

bool ProjectionsDescription::has(const String & projection_name) const
{
    return map.contains(projection_name);
}

const ProjectionDescription & ProjectionsDescription::get(const String & projection_name) const
{
    auto it = map.find(projection_name);
    if (it == map.end())
    {
        String exception_message = fmt::format("There is no projection {} in table", projection_name);
        appendHintsMessage(exception_message, projection_name);
        throw Exception(exception_message, ErrorCodes::NO_SUCH_PROJECTION_IN_TABLE);
    }

    return *(it->second);
}

void ProjectionsDescription::add(ProjectionDescription && projection, const String & after_projection, bool first, bool if_not_exists)
{
    if (has(projection.name))
    {
        if (if_not_exists)
            return;
        throw Exception(
            "Cannot add projection " + projection.name + ": projection with this name already exists", ErrorCodes::ILLEGAL_PROJECTION);
    }

    auto insert_it = projections.cend();

    if (first)
        insert_it = projections.cbegin();
    else if (!after_projection.empty())
    {
        auto it = std::find_if(projections.cbegin(), projections.cend(), [&after_projection](const auto & projection_)
        {
            return projection_.name == after_projection;
        });
        if (it != projections.cend())
            ++it;
        insert_it = it;
    }

    auto it = projections.insert(insert_it, std::move(projection));
    map[it->name] = it;
}

void ProjectionsDescription::remove(const String & projection_name, bool if_exists)
{
    auto it = map.find(projection_name);
    if (it == map.end())
    {
        if (if_exists)
            return;

        String exception_message = fmt::format("There is no projection {} in table", projection_name);
        appendHintsMessage(exception_message, projection_name);
        throw Exception(exception_message, ErrorCodes::NO_SUCH_PROJECTION_IN_TABLE);
    }

    projections.erase(it->second);
    map.erase(it);
}

std::vector<String> ProjectionsDescription::getAllRegisteredNames() const
{
    std::vector<String> names;
    names.reserve(map.size());
    for (const auto & pair : map)
        names.push_back(pair.first);
    return names;
}

ExpressionActionsPtr
ProjectionsDescription::getSingleExpressionForProjections(const ColumnsDescription & columns, ContextPtr query_context) const
{
    ASTPtr combined_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & projection : projections)
        for (const auto & projection_expr : projection.query_ast->children)
            combined_expr_list->children.push_back(projection_expr->clone());

    auto syntax_result = TreeRewriter(query_context).analyze(combined_expr_list, columns.getAllPhysical());
    return ExpressionAnalyzer(combined_expr_list, syntax_result, query_context).getActions(false);
}

}
