#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/ProjectionsDescription.h>

#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>

#include <Core/Defines.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int NO_SUCH_PROJECTION_IN_TABLE;
    extern const int ILLEGAL_PROJECTION;
    extern const int NOT_IMPLEMENTED;
};

const char * ProjectionDescription::typeToString(Type type)
{
    switch (type)
    {
        case Type::Normal:
            return "normal";
        case Type::Aggregate:
            return "aggregate";
    }

    __builtin_unreachable();
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
    other.column_names = column_names;
    other.data_types = data_types;
    other.sample_block = sample_block;
    other.sample_block_for_keys = sample_block_for_keys;
    other.metadata = metadata;
    other.key_size = key_size;

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
        result.query_ast, query_context, storage, {}, SelectQueryOptions{QueryProcessingStage::WithMergeableState}.modify().ignoreAlias());

    result.required_columns = select.getRequiredColumns();
    result.sample_block = select.getSampleBlock();

    const auto & analysis_result = select.getAnalysisResult();
    if (analysis_result.need_aggregate)
    {
        for (const auto & key : select.getQueryAnalyzer()->aggregationKeys())
            result.sample_block_for_keys.insert({nullptr, key.type, key.name});
    }

    for (size_t i = 0; i < result.sample_block.columns(); ++i)
    {
        const auto & column_with_type_name = result.sample_block.getByPosition(i);

        if (column_with_type_name.column && isColumnConst(*column_with_type_name.column))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Projections cannot contain constant columns: {}", column_with_type_name.name);

        result.column_names.emplace_back(column_with_type_name.name);
        result.data_types.emplace_back(column_with_type_name.type);
    }

    StorageInMemoryMetadata metadata;
    metadata.setColumns(ColumnsDescription(result.sample_block.getNamesAndTypesList()));
    metadata.partition_key = KeyDescription::getSortingKeyFromAST({}, metadata.columns, query_context, {});

    const auto & query_select = result.query_ast->as<const ASTSelectQuery &>();
    if (select.hasAggregation())
    {
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
            metadata.sorting_key = KeyDescription::getSortingKeyFromAST(order_expression, metadata.columns, query_context, {});
            metadata.primary_key = KeyDescription::getKeyFromAST(order_expression, metadata.columns, query_context);
        }
        else
        {
            metadata.sorting_key = KeyDescription::getSortingKeyFromAST({}, metadata.columns, query_context, {});
            metadata.primary_key = KeyDescription::getKeyFromAST({}, metadata.columns, query_context);
        }
        if (query_select.orderBy())
            throw Exception(
                "When aggregation is used in projection, ORDER BY cannot be specified", ErrorCodes::ILLEGAL_PROJECTION);
    }
    else
    {
        result.type = ProjectionDescription::Type::Normal;
        metadata.sorting_key = KeyDescription::getSortingKeyFromAST(query_select.orderBy(), metadata.columns, query_context, {});
        metadata.primary_key = KeyDescription::getKeyFromAST(query_select.orderBy(), metadata.columns, query_context);
    }
    metadata.primary_key.definition_ast = nullptr;
    result.metadata = std::make_shared<StorageInMemoryMetadata>(metadata);
    return result;
}

void ProjectionDescription::recalculateWithNewColumns(const ColumnsDescription & new_columns, ContextPtr query_context)
{
    *this = getProjectionFromAST(definition_ast, new_columns, query_context);
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
    return map.count(projection_name) > 0;
}

const ProjectionDescription & ProjectionsDescription::get(const String & projection_name) const
{
    auto it = map.find(projection_name);
    if (it == map.end())
        throw Exception("There is no projection " + projection_name + " in table", ErrorCodes::NO_SUCH_PROJECTION_IN_TABLE);

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

void ProjectionsDescription::remove(const String & projection_name)
{
    auto it = map.find(projection_name);
    if (it == map.end())
        throw Exception("There is no projection " + projection_name + " in table.", ErrorCodes::NO_SUCH_PROJECTION_IN_TABLE);

    projections.erase(it->second);
    map.erase(it);
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
