#include <Storages/addColumnsStructureToQueryWithClusterEngine.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>
#include <Formats/FormatFactory.h>
#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query)
{
    auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query || !select_query->tables())
        return nullptr;

    auto * tables = select_query->tables()->as<ASTTablesInSelectQuery>();
    auto * table_expression = tables->children[0]->as<ASTTablesInSelectQueryElement>()->table_expression->as<ASTTableExpression>();
    if (!table_expression->table_function)
        return nullptr;

    auto * table_function = table_expression->table_function->as<ASTFunction>();
    return table_function->arguments->as<ASTExpressionList>();
}

static ASTExpressionList * getExpressionListAndCheckArguments(ASTPtr & query, size_t max_arguments, const String & function_name)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query from table function {}, got '{}'", function_name, queryToString(query));

    if (expression_list->children.size() < 2 || expression_list->children.size() > max_arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 2 to {} arguments in {} table functions, got {}",
                        max_arguments, function_name, expression_list->children.size());

    return expression_list;
}

static void addColumnsStructureToQueryWithHDFSOrURLClusterEngine(ASTPtr & query, const String & structure, const String & function_name)
{
    ASTExpressionList * expression_list = getExpressionListAndCheckArguments(query, 5, function_name);
    auto structure_literal = std::make_shared<ASTLiteral>(structure);
    ASTs & args = expression_list->children;

    /// XCuster(cluster_name, source)
    if (args.size() == 2)
    {
        args.push_back(std::make_shared<ASTLiteral>("auto"));
        args.push_back(structure_literal);
    }
    /// XCuster(cluster_name, source, format)
    else if (args.size() == 3)
    {
        args.push_back(structure_literal);
    }
    /// XCuster(cluster_name, source, format, 'auto')
    else if (args.size() == 4)
    {
        args.back() = structure_literal;
    }
    /// XCuster(cluster_name, source, format, 'auto', compression)
    else if (args.size() == 5)
    {
        args[args.size() - 2] = structure_literal;
    }
}


void addColumnsStructureToQueryWithHDFSClusterEngine(ASTPtr & query, const String & structure)
{
    addColumnsStructureToQueryWithHDFSOrURLClusterEngine(query, structure, "hdfsCluster");
}

void addColumnsStructureToQueryWithURLClusterEngine(ASTPtr & query, const String & structure)
{
    addColumnsStructureToQueryWithHDFSOrURLClusterEngine(query, structure, "urlCluster");
}

void addColumnsStructureToQueryWithS3ClusterEngine(ASTPtr & query, const String & structure)
{
    ASTExpressionList * expression_list = getExpressionListAndCheckArguments(query, 7, "s3Cluster");
    auto structure_literal = std::make_shared<ASTLiteral>(structure);
    ASTs & args = expression_list->children;

    /// s3Cluster(cluster_name, s3_url)
    if (args.size() == 2)
    {
        args.push_back(std::make_shared<ASTLiteral>("auto"));
        args.push_back(structure_literal);
    }
    /// s3Cluster(cluster_name, s3_url, format) or s3Cluster(cluster_name, s3_url, NOSIGN)
    /// We can distinguish them by looking at the 3-rd argument: check if it's NOSIGN or not.
    else if (args.size() == 3)
    {
        auto third_arg = checkAndGetLiteralArgument<String>(args[2], "format/NOSIGN");
        /// If there is NOSIGN, add format name before structure.
        if (boost::iequals(third_arg, "NOSIGN"))
            args.push_back(std::make_shared<ASTLiteral>("auto"));
        args.push_back(structure_literal);
    }
    /// s3Cluster(cluster_name, source, format, structure) or
    /// s3Cluster(cluster_name, source, access_key_id, access_key_id) or
    /// s3Cluster(cluster_name, source, NOSIGN, format)
    /// We can distinguish them by looking at the 3-nrd argument: check if it's NOSIGN, format name or neither.
    else if (args.size() == 4)
    {
        auto third_arg = checkAndGetLiteralArgument<String>(args[2], "format/NOSIGN");
        if (boost::iequals(third_arg, "NOSIGN"))
        {
            args.push_back(structure_literal);
        }
        else if (third_arg == "auto" || FormatFactory::instance().getAllFormats().contains(third_arg))
        {
            args.back() = structure_literal;
        }
        else
        {
            args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(structure_literal);
        }
    }
    /// s3Cluster(cluster_name, source, format, structure, compression_method) or
    /// s3Cluster(cluster_name, source, access_key_id, access_key_id, format) or
    /// s3Cluster(cluster_name, source, NOSIGN, format, structure)
    /// We can distinguish them by looking at the 3-rd argument: check if it's NOSIGN, format name or neither.
    else if (args.size() == 5)
    {
        auto third_arg = checkAndGetLiteralArgument<String>(args[2], "format/NOSIGN");
        if (boost::iequals(third_arg, "NOSIGN"))
        {
            args.back() = structure_literal;
        }
        else if (third_arg == "auto" || FormatFactory::instance().getAllFormats().contains(third_arg))
        {
            args[args.size() - 2] = structure_literal;
        }
        else
        {
            args.push_back(structure_literal);
        }
    }
    /// s3Cluster(cluster_name, source, access_key_id, access_key_id, format, structure) or
    /// s3Cluster(cluster_name, source, NOSIGN, format, structure, compression_method)
    /// We can distinguish them by looking at the 3-rd argument: check if it's a NOSIGN keyword name or not.
    else if (args.size() == 6)
    {
        auto third_arg = checkAndGetLiteralArgument<String>(args[2], "format/NOSIGN");
        if (boost::iequals(third_arg, "NOSIGN"))
        {
            args[args.size() - 2] = structure_literal;
        }
        else
        {
            args.back() = structure_literal;
        }
    }
    /// s3Cluster(cluster_name, source, access_key_id, access_key_id, format, structure, compression)
    else if (args.size() == 7)
    {
        args[args.size() - 2] = structure_literal;
    }
}


}
