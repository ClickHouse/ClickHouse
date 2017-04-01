#include <DB/Analyzers/AnalyzeResultOfQuery.h>
#include <DB/Analyzers/CollectAliases.h>
#include <DB/Analyzers/CollectTables.h>
#include <DB/Analyzers/AnalyzeLambdas.h>
#include <DB/Analyzers/AnalyzeColumns.h>
#include <DB/Analyzers/TypeAndConstantInference.h>
#include <DB/Interpreters/Context.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Parsers/ASTSelectQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_AST_STRUCTURE;
}


void AnalyzeResultOfQuery::process(ASTPtr & ast, Context & context)
{
    const ASTSelectQuery * select = typeid_cast<const ASTSelectQuery *>(ast.get());
    if (!select)
        throw Exception("AnalyzeResultOfQuery::process was called for not a SELECT query", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    if (!select->select_expression_list)
        throw Exception("SELECT query doesn't have select_expression_list", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    AnalyzeLambdas analyze_lambdas;
    analyze_lambdas.process(ast);

    CollectAliases collect_aliases;
    collect_aliases.process(ast);

    CollectTables collect_tables;
    collect_tables.process(ast, context, collect_aliases);

    AnalyzeColumns analyze_columns;
    analyze_columns.process(ast, collect_aliases, collect_tables);

    TypeAndConstantInference inference;
    inference.process(ast, context, collect_aliases, analyze_columns, analyze_lambdas);

    for (const ASTPtr & child : select->select_expression_list->children)
    {
        auto it = inference.info.find(child->getColumnName());
        if (it == inference.info.end())
            throw Exception("Logical error: type information for result column of SELECT query was not inferred", ErrorCodes::LOGICAL_ERROR);

        String name = child->getAliasOrColumnName();
        const TypeAndConstantInference::ExpressionInfo & info = it->second;

        result.insert(ColumnWithTypeAndName(
            info.is_constant_expression ? info.data_type->createConstColumn(1, info.value) : nullptr,
            info.data_type,
            std::move(name)));
    }
}


void AnalyzeResultOfQuery::dump(WriteBuffer & out) const
{
    writeString(result.dumpStructure(), out);
}


}
