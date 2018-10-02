#include <Poco/String.h>
#include <Analyzers/TypeAndConstantInference.h>
#include <Analyzers/CollectAliases.h>
#include <Analyzers/AnalyzeColumns.h>
#include <Analyzers/AnalyzeLambdas.h>
#include <Analyzers/AnalyzeResultOfQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTSubquery.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFunction.h>
#include <algorithm>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int BAD_LAMBDA;
}


namespace
{

Field getValueFromConstantColumn(const ColumnPtr & column)
{
    if (!column->isColumnConst())
        throw Exception("Logical error: expected that column is constant", ErrorCodes::LOGICAL_ERROR);
    if (column->size() != 1)
        throw Exception("Logical error: expected that column with constant has single element", ErrorCodes::LOGICAL_ERROR);
    return (*column)[0];
}


/// Description of single parameter of lambda expression: name -> type.
/// Note, that after AnalyzeLambda step, names of lambda parameters are unique in single select query.
using LambdaParameters = std::unordered_map<String, DataTypePtr>;


void processImpl(
    ASTPtr & ast, const Context & context,
    CollectAliases & aliases, const AnalyzeColumns & columns,
    TypeAndConstantInference::Info & info,
    const AnalyzeLambdas & lambdas,
    ExecuteTableFunctions & table_functions);


void processLiteral(const String & column_name, const ASTPtr & ast, TypeAndConstantInference::Info & info)
{
    const ASTLiteral * literal = static_cast<const ASTLiteral *>(ast.get());

    TypeAndConstantInference::ExpressionInfo expression_info;
    expression_info.node = ast;
    expression_info.is_constant_expression = true;
    expression_info.data_type = applyVisitor(FieldToDataType(), literal->value);
    expression_info.value = convertFieldToType(literal->value, *expression_info.data_type);
    info.emplace(column_name, std::move(expression_info));
}


void processIdentifier(const String & column_name, const ASTPtr & ast, TypeAndConstantInference::Info & info,
    const Context & context, CollectAliases & aliases, const AnalyzeColumns & columns,
    const AnalyzeLambdas & lambdas, ExecuteTableFunctions & table_functions)
{
    /// Column from table
    auto it = columns.columns.find(column_name);
    if (it != columns.columns.end())
    {
        TypeAndConstantInference::ExpressionInfo expression_info;
        expression_info.node = ast;
        expression_info.data_type = it->second.data_type;

        /// If it comes from subquery and we know, that it is constant expression.
        const Block & structure_of_subquery = it->second.table.structure_of_subquery;
        if (structure_of_subquery)
        {
            const ColumnWithTypeAndName & column_from_subquery = structure_of_subquery.getByName(it->second.name_in_table);
            if (column_from_subquery.column)
            {
                expression_info.is_constant_expression = true;
                expression_info.value = getValueFromConstantColumn(column_from_subquery.column);
            }
        }

        info.emplace(column_name, std::move(expression_info));
    }
    else
    {
        /// Alias
        auto jt = aliases.aliases.find(column_name);
        if (jt != aliases.aliases.end())
        {
            /// TODO Cyclic aliases.

            if (jt->second.kind != CollectAliases::Kind::Expression)
                throw Exception("Logical error: unexpected kind of alias", ErrorCodes::LOGICAL_ERROR);

            processImpl(jt->second.node, context, aliases, columns, info, lambdas, table_functions);
            info[column_name] = info[jt->second.node->getColumnName()];
        }
    }
}


void processFunction(const String & column_name, ASTPtr & ast, TypeAndConstantInference::Info & info,
    const Context & context)
{
    ASTFunction * function = static_cast<ASTFunction *>(ast.get());

    /// Special case for lambda functions. Lambda function has special return type "Function".
    /// We first create info with Function of unspecified arguments, and will specify them later.
    if (function->name == "lambda")
    {
        size_t number_of_lambda_parameters = AnalyzeLambdas::extractLambdaParameters(function->arguments->children.at(0)).size();

        TypeAndConstantInference::ExpressionInfo expression_info;
        expression_info.node = ast;
        expression_info.data_type = std::make_unique<DataTypeFunction>(DataTypes(number_of_lambda_parameters));
        info.emplace(column_name, std::move(expression_info));
        return;
    }

    DataTypes argument_types;
    ColumnsWithTypeAndName argument_columns;

    if (function->arguments)
    {
        for (const auto & child : function->arguments->children)
        {
            auto it = info.find(child->getColumnName());
            if (it == info.end())
                throw Exception("Logical error: type of function argument was not inferred during depth-first search", ErrorCodes::LOGICAL_ERROR);

            argument_types.emplace_back(it->second.data_type);
            argument_columns.emplace_back(ColumnWithTypeAndName(nullptr, it->second.data_type, ""));
            if (it->second.is_constant_expression)
                argument_columns.back().column = it->second.data_type->createColumnConst(1, it->second.value);
        }
    }

    /// Special cases for COUNT(DISTINCT ...) function.
    bool column_name_changed = false;
    String func_name_lowercase = Poco::toLower(function->name);
    if (func_name_lowercase == "countdistinct")    /// It comes in that form from parser.
    {
        /// Select implementation of countDistinct based on settings.
        /// Important that it is done as query rewrite. It means rewritten query
        ///  will be sent to remote servers during distributed query execution,
        ///  and on all remote servers, function implementation will be same.
        function->name = context.getSettingsRef().count_distinct_implementation;
        column_name_changed = true;
    }

    /// Aggregate function.
    Array parameters = (function->parameters) ? getAggregateFunctionParametersArray(function->parameters) : Array();
    if (AggregateFunctionPtr aggregate_function_ptr = AggregateFunctionFactory::instance().tryGet(function->name, argument_types, parameters))
    {
        /// Note that aggregate function could never be constant expression.

        /// (?) Replace function name to canonical one. Because same function could be referenced by different names.
        // function->name = aggregate_function_ptr->getName();

        TypeAndConstantInference::ExpressionInfo expression_info;
        expression_info.node = ast;
        expression_info.data_type = aggregate_function_ptr->getReturnType();
        expression_info.aggregate_function = aggregate_function_ptr;
        info.emplace(column_name_changed ? ast->getColumnName() : column_name, std::move(expression_info));
        return;
    }

    /// Ordinary function.
    if (function->parameters)
        throw Exception("The only parametric functions (functions with two separate parenthesis pairs) are aggregate functions"
            ", and '" + function->name + "' is not an aggregate function.", ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);

    /// IN operator. This is special case, because subqueries in right hand side are not scalar subqueries.
    if (function->name == "in"
        || function->name == "notIn"
        || function->name == "globalIn"
        || function->name == "globalNotIn")
    {
        /// For simplicity reasons, do not consider this as constant expression. We may change it in future.
        TypeAndConstantInference::ExpressionInfo expression_info;
        expression_info.node = ast;
        expression_info.data_type = std::make_shared<DataTypeUInt8>();
        info.emplace(column_name, std::move(expression_info));
        return;
    }

    const auto & function_builder_ptr = FunctionFactory::instance().get(function->name, context);

    /// (?) Replace function name to canonical one. Because same function could be referenced by different names.
    // function->name = function_ptr->getName();

    ColumnsWithTypeAndName columns_for_analysis;
    columns_for_analysis.reserve(argument_types.size());

    bool all_consts = true;
    if (function->arguments)
    {
        for (const auto & child : function->arguments->children)
        {
            String child_name = child->getColumnName();
            const TypeAndConstantInference::ExpressionInfo & child_info = info.at(child_name);
            columns_for_analysis.emplace_back(
                child_info.is_constant_expression ? child_info.data_type->createColumnConst(1, child_info.value) : nullptr,
                child_info.data_type,
                child_name);

            if (!child_info.is_constant_expression)
                all_consts = false;
        }
    }

    auto function_ptr = function_builder_ptr->build(argument_columns);

    TypeAndConstantInference::ExpressionInfo expression_info;
    expression_info.node = ast;
    expression_info.function = function_ptr;
    expression_info.data_type = function_ptr->getReturnType();

    if (all_consts && function_ptr->isSuitableForConstantFolding())
    {
        Block block_with_constants(columns_for_analysis);

        ColumnNumbers argument_numbers(columns_for_analysis.size());
        for (size_t i = 0, size = argument_numbers.size(); i < size; ++i)
            argument_numbers[i] = i;

        size_t result_position = argument_numbers.size();
        block_with_constants.insert({nullptr, expression_info.data_type, column_name});

        function_ptr->execute(block_with_constants, argument_numbers, result_position, 1);

        const auto & result_column = block_with_constants.getByPosition(result_position).column;
        if (result_column->isColumnConst())
        {
            expression_info.is_constant_expression = true;
            expression_info.value = (*result_column)[0];
        }
    }

    info.emplace(column_name, std::move(expression_info));
}


void processScalarSubquery(const String & column_name, ASTPtr & ast, TypeAndConstantInference::Info & info,
    const Context & context, ExecuteTableFunctions & table_functions)
{
    ASTSubquery * subquery = static_cast<ASTSubquery *>(ast.get());

    AnalyzeResultOfQuery analyzer;
    analyzer.process(subquery->children.at(0), context, table_functions);

    if (!analyzer.result)
        throw Exception("Logical error: no columns returned from scalar subquery", ErrorCodes::LOGICAL_ERROR);

    TypeAndConstantInference::ExpressionInfo expression_info;
    expression_info.node = ast;

    if (analyzer.result.columns() == 1)
    {
        const auto & elem = analyzer.result.getByPosition(0);
        expression_info.data_type = elem.type;

        if (elem.column)
        {
            expression_info.is_constant_expression = true;
            expression_info.value = getValueFromConstantColumn(elem.column);
        }
    }
    else
    {
        /// Result of scalar subquery is interpreted as tuple.
        size_t size = analyzer.result.columns();
        DataTypes types;
        types.reserve(size);
        bool all_consts = true;
        for (size_t i = 0; i < size; ++i)
        {
            const auto & elem = analyzer.result.getByPosition(i);
            types.emplace_back(elem.type);
            if (!elem.column)
                all_consts = false;
        }

        expression_info.data_type = std::make_shared<DataTypeTuple>(types);

        if (all_consts)
        {
            TupleBackend value(size);

            for (size_t i = 0; i < size; ++i)
                value[i] = getValueFromConstantColumn(analyzer.result.getByPosition(i).column);

            expression_info.is_constant_expression = true;
            expression_info.value = Tuple(std::move(value));
        }
    }

    info.emplace(column_name, std::move(expression_info));
}


void processHigherOrderFunction(
    ASTPtr & ast, const Context & context,
    CollectAliases & aliases, const AnalyzeColumns & columns,
    TypeAndConstantInference::Info & info,
    const AnalyzeLambdas & lambdas,
    ExecuteTableFunctions & table_functions)
{
    ASTFunction * function = static_cast<ASTFunction *>(ast.get());

    const auto & function_builder_ptr = FunctionFactory::instance().get(function->name, context);

    if (!function->arguments)
        throw Exception("Unexpected AST for higher-order function", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    DataTypes types;
    types.reserve(function->arguments->children.size());
    for (const auto & child : function->arguments->children)
    {
        String child_name = child->getColumnName();
        const TypeAndConstantInference::ExpressionInfo & child_info = info.at(child_name);
        types.emplace_back(child_info.data_type);
    }

    function_builder_ptr->getLambdaArgumentTypes(types);

    /// For every lambda expression, dive into it.

    if (types.size() != function->arguments->children.size())
        throw Exception("Logical error: size of types was changed after call to IFunction::getLambdaArgumentTypes",
            ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0, size = function->arguments->children.size(); i < size; ++i)
    {
        const auto & child = function->arguments->children[i];
        const ASTFunction * lambda = typeid_cast<const ASTFunction *>(child.get());
        if (lambda && lambda->name == "lambda")
        {
            const auto * lambda_type = typeid_cast<const DataTypeFunction *>(types[i].get());

            if (!lambda_type)
                throw Exception("Logical error: IFunction::getLambdaArgumentTypes returned data type for lambda expression,"
                    " that is not DataTypeFunction", ErrorCodes::LOGICAL_ERROR);

            if (!lambda->arguments || lambda->arguments->children.size() != 2)
                throw Exception("Lambda function must have exactly two arguments (sides of arrow)", ErrorCodes::BAD_LAMBDA);

            /// Set types for every lambda parameter.

            AnalyzeLambdas::LambdaParameters parameters = AnalyzeLambdas::extractLambdaParameters(lambda->arguments->children[0]);

            const DataTypes & lambda_argument_types = lambda_type->getArgumentTypes();

            if (parameters.size() != lambda_argument_types.size())
                throw Exception("DataTypeExpression for lambda function has different number of argument types than number of lambda parameters",
                    ErrorCodes::LOGICAL_ERROR);

            for (size_t param_idx = 0, num_params = parameters.size(); param_idx < num_params; ++param_idx)
            {
                TypeAndConstantInference::ExpressionInfo expression_info;
                expression_info.node = typeid_cast<ASTIdentifier *>(lambda->arguments->children[0].get())
                    ? lambda->arguments->children[0]
                    : typeid_cast<ASTFunction &>(*lambda->arguments->children[0]).arguments->children.at(param_idx);

                expression_info.data_type = lambda_argument_types[param_idx];

                info.emplace(parameters[param_idx], std::move(expression_info));
            }

            /// Now dive into.

            processImpl(lambda->arguments->children[1], context, aliases, columns, info, lambdas, table_functions);

            /// Update Expression type (expression signature).

            info.at(lambda->getColumnName()).data_type = std::make_shared<DataTypeFunction>(
                lambda_argument_types, info.at(lambda->arguments->children[1]->getColumnName()).data_type);
        }
    }
}


void processImpl(
    ASTPtr & ast, const Context & context,
    CollectAliases & aliases, const AnalyzeColumns & columns,
    TypeAndConstantInference::Info & info,
    const AnalyzeLambdas & lambdas,
    ExecuteTableFunctions & table_functions)
{
    const ASTFunction * function = typeid_cast<const ASTFunction *>(ast.get());

    /// Bottom-up.

    /// Don't go into components of compound identifiers.
    if (!typeid_cast<const ASTIdentifier *>(ast.get()))
    {
        for (auto & child : ast->children)
        {
            /** Don't go into subqueries and table-like expressions.
              * Also don't go into components of compound identifiers.
              */
            if (typeid_cast<const ASTSelectQuery *>(child.get())
                || typeid_cast<const ASTTableExpression *>(child.get()))
                continue;

            /** Postpone diving into lambda expressions.
              * We must first infer types of other arguments of higher-order function,
              *  and then process lambda expression.
              * Example:
              *  arrayMap((x, y) -> x + y, arr1, arr2)
              * First, infer type of 'arr1' and 'arr2'.
              * Then, ask function arrayMap, what types will have 'x' and 'y'.
              * And then, infer type of 'x + y'.
              */
            if (function && function->name == "lambda")
                continue;

            processImpl(child, context, aliases, columns, info, lambdas, table_functions);
        }
    }

    const ASTLiteral * literal = nullptr;
    const ASTIdentifier * identifier = nullptr;
    const ASTSubquery * subquery = nullptr;

    function
        || (literal = typeid_cast<const ASTLiteral *>(ast.get()))
        || (identifier = typeid_cast<const ASTIdentifier *>(ast.get()))
        || (subquery = typeid_cast<const ASTSubquery *>(ast.get()));

    if (!literal && !identifier && !function && !subquery)
        return;

    /// Same expression is already processed.
    String column_name = ast->getColumnName();
    if (info.count(column_name))
        return;

    if (function)
    {
        /// If this is higher-order function, determine types of lambda arguments and infer types of subexpressions inside lambdas.
        if (lambdas.higher_order_functions.end() != std::find(lambdas.higher_order_functions.begin(), lambdas.higher_order_functions.end(), ast))
            processHigherOrderFunction(ast, context, aliases, columns, info, lambdas, table_functions);

        processFunction(column_name, ast, info, context);
    }
    else if (literal)
        processLiteral(column_name, ast, info);
    else if (identifier)
        processIdentifier(column_name, ast, info, context, aliases, columns, lambdas, table_functions);
    else if (subquery)
        processScalarSubquery(column_name, ast, info, context, table_functions);
}

}


void TypeAndConstantInference::process(ASTPtr & ast, const Context & context,
    CollectAliases & aliases,
    const AnalyzeColumns & columns,
    const AnalyzeLambdas & lambdas,
    ExecuteTableFunctions & table_functions)
{
    processImpl(ast, context, aliases, columns, info, lambdas, table_functions);
}


void TypeAndConstantInference::dump(WriteBuffer & out) const
{
    /// For need of tests, we need to dump result in some fixed order.
    std::vector<Info::const_iterator> vec;
    vec.reserve(info.size());
    for (auto it = info.begin(); it != info.end(); ++it)
        vec.emplace_back(it);

    std::sort(vec.begin(), vec.end(), [](const auto & a, const auto & b) { return a->first < b->first; });

    for (const auto & it : vec)
    {
        writeString(it->first, out);
        writeCString(" -> ", out);
        writeString(it->second.data_type->getName(), out);

        if (it->second.is_constant_expression)
        {
            writeCString(" = ", out);
            String value = applyVisitor(FieldVisitorToString(), it->second.value);
            writeString(value, out);
        }

        writeCString(". AST: ", out);
        if (!it->second.node)
            writeCString("(none)", out);
        else
        {
            std::stringstream formatted_ast;
            formatAST(*it->second.node, formatted_ast, false, true);
            writeString(formatted_ast.str(), out);
        }

        writeChar('\n', out);
    }
}

}
