#include <TableFunctions/TableFunctionSparql.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/SPARQL/SparqlQueryParser.h>
#include <Parsers/SPARQL/SparqlToSqlTranslator.h>
#include <Storages/StorageView.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

void TableFunctionSparql::parseArguments(const ASTPtr & ast_function, ContextPtr /*context*/)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires arguments", getName());

    ASTs & args = function->arguments->children;
    if (args.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires exactly 1 argument: a SPARQL query string",
            getName());

    const auto * literal = args[0]->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' argument must be a string literal", getName());

    String sparql_query = literal->value.safeGet<String>();

    SPARQL::SparqlQueryParser parser;
    auto ast = parser.parse(sparql_query);

    SPARQL::SparqlToSqlTranslator translator;
    String sql = translator.translate(*ast);

    ParserSelectWithUnionQuery sql_parser;
    auto select_ast = parseQuery(sql_parser, sql, "SPARQL-generated SQL", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    NormalizeSelectWithUnionQueryVisitor::Data data{SetOperationMode::Unspecified};
    NormalizeSelectWithUnionQueryVisitor{data}.visit(select_ast);

    create.set(create.select, select_ast);
}

ColumnsDescription TableFunctionSparql::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    assert(create.select);
    assert(!create.children.empty());
    assert(create.children[0]->as<ASTSelectWithUnionQuery>());

    SharedHeader sample_block;

    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
        sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(create.children[0], context);
    else
        sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(create.children[0], context);

    return ColumnsDescription(sample_block->getNamesAndTypesList());
}

StoragePtr TableFunctionSparql::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageView>(StorageID(getDatabaseName(), table_name), create, columns, "");
    res->startup();
    return res;
}

void registerTableFunctionSparql(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionSparql>(
        FunctionDocumentation{
            .description = "Translates a SPARQL 1.1 SELECT query to SQL and executes it against an rdf_triples table in the current database.",
            .examples = {{"basic", "SELECT * FROM sparql('SELECT ?name WHERE { ?p <rdf:type> <:Person> . ?p <:name> ?name }')", ""}},
            .category = FunctionDocumentation::Category::Other},
        TableFunctionProperties{.allow_readonly = true});
}

}
