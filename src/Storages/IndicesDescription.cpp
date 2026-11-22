#include <Interpreters/DDLTask.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/misc.h>
#include <Storages/IndicesDescription.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/extractKeyExpressionList.h>

#include <Storages/ReplaceAliasByExpressionVisitor.h>

#include <Core/Defines.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Calls `fn` with the value of each `preprocessor` / `postprocessor` argument of a text index.
template <typename F>
void forEachTransformArgument(const ASTPtr & arguments, F && fn)
{
    for (const auto & child : arguments->children)
    {
        auto * func = child->as<ASTFunction>();
        if (!func || func->name != "equals" || !func->arguments || func->arguments->children.size() != 2)
            continue;

        const auto * key = func->arguments->children[0]->as<ASTIdentifier>();
        if (key && (key->name() == "preprocessor" || key->name() == "postprocessor"))
            fn(func->arguments->children[1]);
    }
}

void expandTextIndexTransformAliases(const ASTPtr & arguments, const ColumnsDescription & columns)
{
    using ReplaceAliasToExprVisitor = InDepthNodeVisitor<ReplaceAliasByExpressionMatcher, true>;
    forEachTransformArgument(arguments, [&](ASTPtr & expression)
    {
        ReplaceAliasToExprVisitor::Data data{columns, {}, /*reject_lambda_capture=*/ true};
        ReplaceAliasToExprVisitor{data}.visit(expression);
    });
}

/// Resolving a name gives the function's resolver, and `checkAccessOnBuild` checks its privileges with no
/// arguments. Names only: building the expression would report its type errors from here, not from the validator.
///
/// A transform is analysed under the global context, so reading a table or a dictionary during analysis
/// would happen with full access. Such access depends on the arguments, not the function name, so
/// authorizing names cannot cover it, and these constructs are refused. Only here, where a user declares
/// the index: metadata stored by an older version must still load and stay alterable.
void checkFunctionsAsUser(const ASTPtr & ast, const ContextPtr & context)
{
    if (ast->as<ASTSubquery>() || ast->as<ASTSelectWithUnionQuery>() || ast->as<ASTSelectQuery>() || ast->as<ASTTableIdentifier>())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "A text index transform expression must not contain subqueries or table references");

    if (const auto * function = ast->as<ASTFunction>())
    {
        if (functionIsInOrGlobalInOperator(function->name) && function->arguments && function->arguments->children.size() == 2)
        {
            const auto & set = function->arguments->children[1];
            if (set->as<ASTIdentifier>() || set->as<ASTTableIdentifier>())
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "A text index transform expression must not use a table or a set as the right side of `{}`", function->name);
        }

        /// Only the name, which for `dictGet` and the like does not touch the dictionary; analysis would.
        if (auto resolver = FunctionFactory::instance().tryGet(function->name, context))
        {
            resolver->checkAccessOnBuild();

            if (!resolver->isDeterministic())
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "A text index transform expression must not contain non-deterministic functions. Got `{}`", function->name);
        }
    }

    for (const auto & child : ast->children)
        checkFunctionsAsUser(child, context);
}

void checkTransformPermissions(const ASTPtr & arguments, const ContextPtr & context)
{
    forEachTransformArgument(arguments, [&](ASTPtr & argument)
    {
        auto expression = argument->clone();
        /// Inlines SQL UDF bodies, as `TreeRewriter` does, so a function used only inside one is seen.
        if (!UserDefinedSQLFunctionFactory::instance().empty())
            UserDefinedSQLFunctionVisitor::visit(expression, context);

        checkFunctionsAsUser(expression, context);
    });
}

}

void checkTextIndexPreAndPostProcessorPermissions(const IndexDescription & index, const ContextPtr & context)
{
    if (index.type == TEXT_INDEX_NAME && index.arguments)
        checkTransformPermissions(index.arguments, context);
}

bool hasTextIndexPreOrPostProcessor(const IndexDescription & index)
{
    if (index.type != TEXT_INDEX_NAME || !index.arguments)
        return false;

    bool found = false;
    forEachTransformArgument(index.arguments, [&](ASTPtr &) { found = true; });
    return found;
}

[[noreturn]] static void throwTransformNeedsInitiatorUser()
{
    throw Exception(
        ErrorCodes::INCORRECT_QUERY,
        "A text index preprocessor or postprocessor is authorized on the host the query is submitted to, and the "
        "other hosts resolve it against their own schema without that user. Let every host run the query as the "
        "initiating user instead: enable the server setting distributed_ddl_use_initial_user_and_roles on every "
        "host, and set distributed_ddl_entry_format_version to at least {}",
        DDLLogEntry::INITIATOR_USER_VERSION);
}

void rejectTextIndexTransformWithoutInitiatorUser(const ASTIndexDeclaration & declaration)
{
    if (hasTextIndexPreOrPostProcessor(declaration))
        throwTransformNeedsInitiatorUser();
}

void rejectTextIndexTransformWithoutInitiatorUser(const IndexDescription & index)
{
    if (hasTextIndexPreOrPostProcessor(index))
        throwTransformNeedsInitiatorUser();
}

bool hasTextIndexPreOrPostProcessor(const ASTIndexDeclaration & declaration)
{
    const auto * type = declaration.getType() ? declaration.getType()->as<ASTFunction>() : nullptr;
    if (!type || Poco::toLower(type->name) != TEXT_INDEX_NAME || !type->arguments)
        return false;

    bool found = false;
    forEachTransformArgument(type->arguments, [&](ASTPtr &) { found = true; });
    return found;
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
    , is_implicitly_created(other.is_implicitly_created)
    , escape_filenames(other.escape_filenames)
{
    if (other.expression)
        expression = other.expression->clone();
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

    if (index_type && index_type->arguments)
    {
        result.arguments = index_type->arguments->clone();

        if (result.type == TEXT_INDEX_NAME)
            expandTextIndexTransformAliases(result.arguments, columns);
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
        for (const auto & index_expr : index.expression_list_ast->children)
            combined_expr_list->children.push_back(index_expr->clone());

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
