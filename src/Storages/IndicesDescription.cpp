#include <Common/DateLUT.h>
#include <Common/SipHash.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/IndicesDescription.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/extractKeyExpressionList.h>

#include <Storages/ReplaceAliasByExpressionVisitor.h>

#include <Core/Defines.h>
#include <unordered_set>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Only the `preprocessor` and `postprocessor` arguments contain column expressions.
void expandTextIndexTransformAliases(const ASTPtr & arguments, const ColumnsDescription & columns)
{
    using ReplaceAliasToExprVisitor = InDepthNodeVisitor<ReplaceAliasByExpressionMatcher, true>;
    for (const auto & child : arguments->children)
    {
        const auto * func = child->as<ASTFunction>();
        if (!func || func->name != "equals" || !func->arguments || func->arguments->children.size() != 2)
            continue;

        const auto * key = func->arguments->children[0]->as<ASTIdentifier>();
        if (key && (key->name() == "preprocessor" || key->name() == "postprocessor"))
        {
            ReplaceAliasToExprVisitor::Data data{columns, {}, /*reject_lambda_capture=*/ true};
            ReplaceAliasToExprVisitor{data}.visit(func->arguments->children[1]);
        }
    }
}

}

IndexDescription::IndexDescription(const IndexDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
    , name(other.name)
    , type(other.type)
    , arguments(other.arguments ? other.arguments->clone() : nullptr)
    , column_names(other.column_names)
    , data_types(other.data_types)
    , sample_block(other.sample_block)
    , granularity(other.granularity)
    , where_ast(other.where_ast ? other.where_ast->clone() : nullptr)
    , where_column_name(other.where_column_name)
    , where_settings_salt(other.where_settings_salt)
    , where_identity(other.where_identity)
    , where_timezone(other.where_timezone)
    , is_implicitly_created(other.is_implicitly_created)
    , escape_filenames(other.escape_filenames)
{
    if (other.expression)
        expression = other.expression->clone();
    if (other.where_expression)
        where_expression = other.where_expression->clone();
    if (other.selected_rows_expression)
        selected_rows_expression = other.selected_rows_expression->clone();
}


IndexDescription & IndexDescription::operator=(const IndexDescription & other)
{
    if (&other == this)
        return *this;

    if (other.definition_ast)
        definition_ast = other.definition_ast->clone();
    else
        definition_ast.reset();

    if (other.expression_list_ast)
        expression_list_ast = other.expression_list_ast->clone();
    else
        expression_list_ast.reset();

    name = other.name;
    type = other.type;

    if (other.expression)
        expression = other.expression->clone();
    else
        expression.reset();

    if (other.arguments)
        arguments = other.arguments->clone();
    else
        arguments.reset();

    column_names = other.column_names;
    data_types = other.data_types;
    sample_block = other.sample_block;
    granularity = other.granularity;

    if (other.where_ast)
        where_ast = other.where_ast->clone();
    else
        where_ast.reset();

    if (other.where_expression)
        where_expression = other.where_expression->clone();
    else
        where_expression.reset();

    if (other.selected_rows_expression)
        selected_rows_expression = other.selected_rows_expression->clone();
    else
        selected_rows_expression.reset();

    where_column_name = other.where_column_name;
    where_settings_salt = other.where_settings_salt;
    where_identity = other.where_identity;
    where_timezone = other.where_timezone;
    is_implicitly_created = other.is_implicitly_created;
    escape_filenames = other.escape_filenames;
    return *this;
}

IndexDescription IndexDescription::getIndexFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    bool is_implicitly_created,
    bool escape_filenames,
    ContextPtr context)
{
    const auto * index_definition = definition_ast->as<ASTIndexDeclaration>();
    if (!index_definition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create skip index from non ASTIndexDeclaration AST");

    if (index_definition->name.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Skip index must have name in definition.");

    /// Without escaping the name becomes a part of the index file name as is, see `getIndexFileName`.
    if (!escape_filenames && index_definition->name.contains('/'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Skip index name ({}) cannot contain '/' with `escape_index_filenames` disabled", index_definition->name);

    auto index_type = index_definition->getType();
    if (!index_type)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "TYPE is required for index");

    if (index_type->parameters && !index_type->parameters->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Index type cannot have parameters");

    if (index_definition->granularity == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Index GRANULARITY must be a positive integer");

    IndexDescription result;
    result.definition_ast = index_definition->clone();
    result.name = index_definition->name;
    result.type = Poco::toLower(index_type->name);
    result.granularity = index_definition->granularity;
    result.is_implicitly_created = is_implicitly_created;
    result.escape_filenames = escape_filenames;

    checkExpressionDoesntContainSubqueries(*index_definition->getExpression());
    result.initExpressionInfo(index_definition->getExpression(), columns, context);

    for (auto & elem : result.sample_block)
    {
        if (!elem.column)
            elem.column = elem.type->createColumn();

        result.column_names.push_back(elem.name);
        result.data_types.push_back(elem.type);
    }

    if (result.column_names.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Skip index '{}' must have at least one column in its expression", result.name);

    if (auto where = index_definition->getWhere())
    {
        /// Text and vector similarity indexes address rows by their position in the index granule.
        static const std::unordered_set<String> types_with_where = {"minmax", "set", "bloom_filter", "tokenbf_v1", "ngrambf_v1", "sparse_grams"};
        if (!types_with_where.contains(result.type))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Skip index of type '{}' does not support WHERE", result.type);

        checkExpressionDoesntContainSubqueries(*where);

        using ReplaceAliasToExprVisitor = InDepthNodeVisitor<ReplaceAliasByExpressionMatcher, true>;
        result.where_ast = where->clone();
        /// Without its own alias the condition column is named the same in the combined index expressions.
        if (auto * where_with_alias = dynamic_cast<ASTWithAlias *>(result.where_ast.get()))
            where_with_alias->setAlias("");
        ReplaceAliasToExprVisitor::Data data{columns, {}};
        ReplaceAliasToExprVisitor{data}.visit(result.where_ast);

        /// The part writer runs the condition analysed here, with the settings of the server (global) context.
        /// Its constants are folded in the current time zone, which is recorded.
        const ContextPtr analysis_context = context->getGlobalContext();
        result.where_timezone = DateLUT::instance().getTimeZone();

        ASTPtr where_ast = result.where_ast->clone();
        auto syntax = TreeRewriter(analysis_context).analyze(
            where_ast, columns.get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns()));
        result.where_expression = ExpressionAnalyzer(where_ast, syntax, analysis_context).getActions(true);

        const auto & where_column = result.where_expression->getSampleBlock().getByPosition(0);
        if (!isUInt8(removeNullable(removeLowCardinality(where_column.type))))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "WHERE condition of skip index '{}' must be of type UInt8 or Bool, got {}", result.name, where_column.type->getName());
        result.where_column_name = where_column.name;

        ASTPtr expression_ast = result.expression_list_ast->clone();
        auto expression_syntax = TreeRewriter(analysis_context).analyze(
            expression_ast, columns.get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns()));
        result.selected_rows_expression = ExpressionAnalyzer(expression_ast, expression_syntax, analysis_context).getActions(true);

        /// The functions of the condition capture the global settings, not the ones of the session that defines the index.
        result.where_settings_salt = queryConditionCacheSettingsSalt(context->getGlobalContext()->getSettingsRef());
    }

    if (index_type && index_type->arguments)
    {
        result.arguments = index_type->arguments->clone();

        if (result.type == TEXT_INDEX_NAME)
            expandTextIndexTransformAliases(result.arguments, columns);
    }

    if (result.where_expression)
    {
        /// What the granules of a partial index mean: the condition, the settings it was built with, and how they
        /// are encoded. A partial index of every type is written to the same kind of file, so a part written for
        /// another type, parameters, indexed expression or granularity must not pass for this index.
        SipHash identity;
        const auto where_hash = result.where_ast->getTreeHash(/*ignore_aliases=*/ true);
        identity.update(where_hash.low64);
        identity.update(where_hash.high64);
        identity.update(result.where_settings_salt);
        /// The analysed condition the writer runs: each step with its result type and the state its function captured.
        result.where_expression->getActionsDAG().updateHash(identity);
        identity.update(result.type);
        if (result.arguments)
        {
            const auto arguments_hash = result.arguments->getTreeHash(/*ignore_aliases=*/ true);
            identity.update(arguments_hash.low64);
            identity.update(arguments_hash.high64);
        }
        const auto expression_hash = result.expression_list_ast->getTreeHash(/*ignore_aliases=*/ true);
        identity.update(expression_hash.low64);
        identity.update(expression_hash.high64);
        result.selected_rows_expression->getActionsDAG().updateHash(identity);
        identity.update(result.granularity);
        result.where_identity = identity.get128();
    }

    return result;
}

void IndexDescription::recalculateWithNewColumns(const ColumnsDescription & new_columns, ContextPtr context)
{
    *this = getIndexFromAST(definition_ast, new_columns, is_implicitly_created, escape_filenames, context);
}

void IndexDescription::initExpressionInfo(ASTPtr index_expression, const ColumnsDescription & columns, ContextPtr context)
{
    chassert(index_expression != nullptr);

    using ReplaceAliasToExprVisitor = InDepthNodeVisitor<ReplaceAliasByExpressionMatcher, true>;

    ASTPtr expr_list = extractKeyExpressionList(index_expression);
    if (expr_list == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression is not set");

    ReplaceAliasToExprVisitor::Data data{columns, {}};
    ReplaceAliasToExprVisitor{data}.visit(expr_list);

    expression_list_ast = expr_list->clone();

    TreeRewriterResultPtr syntax = TreeRewriter(context).analyze(
        expr_list,
        columns.get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns())
    );

    expression = ExpressionAnalyzer(expr_list, syntax, context).getActions(true);

    sample_block = expression->getSampleBlock();
}

Field getFieldFromIndexArgumentAST(const ASTPtr & ast)
{
    /// E.g. INDEX index_name column_name TYPE vector_similarity('hnsw', 'f32')
    if (const auto * ast_literal = ast->as<ASTLiteral>())
        return ast_literal->value;
    /// E.g. INDEX index_name column_name TYPE vector_similarity(index_name, column_name)
    if (const auto * ast_identifier = ast->as<ASTIdentifier>())
        return Field(ast_identifier->name());
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Only literals and identifiers can be skip index arguments");
}

FieldVector getFieldsFromIndexArgumentsAST(const ASTPtr & arguments)
{
    FieldVector result;
    if (!arguments)
        return result;

    result.reserve(arguments->children.size());
    for (const auto & child : arguments->children)
        result.emplace_back(getFieldFromIndexArgumentAST(child));
    return result;
}

bool IndexDescription::isSimpleSingleColumnIndex() const
{
    return expression_list_ast && expression_list_ast->children.size() == 1 && expression_list_ast->children[0]->as<ASTIdentifier>();
}

Names IndexDescription::getRequiredColumns() const
{
    Names result = expression->getRequiredColumns();
    if (where_expression)
    {
        for (const auto & actions : {where_expression, selected_rows_expression})
            for (const auto & column : actions->getRequiredColumns())
                if (std::find(result.begin(), result.end(), column) == result.end())
                    result.push_back(column);
    }
    return result;
}


bool IndicesDescription::has(const String & name) const
{
    for (const auto & index : *this)
        if (index.name == name)
            return true;
    return false;
}

const IndexDescription & IndicesDescription::getByName(const String & name) const
{
    for (const auto & index : *this)
        if (index.name == name)
            return index;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no index with name '{}'", name);
}

bool IndicesDescription::hasType(const String & type) const
{
    for (const auto & index : *this)
        if (index.type == type)
            return true;
    return false;
}

String IndicesDescription::explicitToString() const
{
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & index : *this)
    {
        if (!index.isImplicitlyCreated())
            list.children.push_back(index.definition_ast);
    }

    return list.formatIgnoringRedundantParentheses();
}

String IndicesDescription::allToString() const
{
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & index : *this)
        list.children.push_back(index.definition_ast);

    return list.formatIgnoringRedundantParentheses();
}


IndicesDescription
IndicesDescription::parse(const String & str, const ColumnsDescription & columns, bool escape_index_filenames, ContextPtr context)
{
    IndicesDescription result;
    if (str.empty())
        return result;

    ParserIndexDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    for (const auto & index : list->children)
        result.emplace_back(
            IndexDescription::getIndexFromAST(index, columns, /* is_implicitly_created */ false, escape_index_filenames, context));

    return result;
}


ExpressionActionsPtr IndicesDescription::getSingleExpressionForIndices(const ColumnsDescription & columns, ContextPtr context) const
{
    ASTPtr combined_expr_list = make_intrusive<ASTExpressionList>();
    for (const auto & index : *this)
    {
        /// The writer calculates a partial index from its columns, for the rows its condition selects only.
        if (index.where_expression)
        {
            for (const auto & column : index.getRequiredColumns())
                combined_expr_list->children.push_back(make_intrusive<ASTIdentifier>(column));
            continue;
        }
        for (const auto & index_expr : index.expression_list_ast->children)
            combined_expr_list->children.push_back(index_expr->clone());
    }

    auto syntax_result = TreeRewriter(context).analyze(combined_expr_list, columns.get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns()));
    return ExpressionAnalyzer(combined_expr_list, syntax_result, context).getActions(false);
}

VectorWithMemoryTracking<String> IndicesDescription::getAllRegisteredNames() const
{
    VectorWithMemoryTracking<String> result;
    for (const auto & index : *this)
    {
        result.emplace_back(index.name);
    }
    return result;
}

ASTPtr createImplicitMinMaxIndexAST(const String & column_name)
{
    auto index_type = makeASTFunction("minmax");
    auto index_ast = make_intrusive<ASTIndexDeclaration>(
        make_intrusive<ASTIdentifier>(column_name), index_type,
        IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX + column_name);

    index_ast->granularity = ASTIndexDeclaration::DEFAULT_INDEX_GRANULARITY;
    return index_ast;
}

IndexDescription createImplicitMinMaxIndexDescription(
    const String & column_name, const ColumnsDescription & columns, bool escape_index_filenames, ContextPtr context)
{
    auto index_ast = createImplicitMinMaxIndexAST(column_name);
    return IndexDescription::getIndexFromAST(index_ast, columns, /* is_implicitly_created */ true, escape_index_filenames, context);
}

}
