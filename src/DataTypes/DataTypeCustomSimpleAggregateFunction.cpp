#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <boost/algorithm/string/join.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

void DataTypeCustomSimpleAggregateFunction::checkSupportedFunctions(const AggregateFunctionPtr & function)
{
    /// TODO Make it sane.
    static const std::vector<String> supported_functions{"any", "anyLast", "min",
        "max", "sum", "sumWithOverflow", "groupBitAnd", "groupBitOr", "groupBitXor",
        "sumMap", "minMap", "maxMap", "groupArrayArray", "groupUniqArrayArray",
        "sumMappedArrays", "minMappedArrays", "maxMappedArrays"};

    // check function
    if (std::find(std::begin(supported_functions), std::end(supported_functions), function->getName()) == std::end(supported_functions))
    {
        throw Exception("Unsupported aggregate function " + function->getName() + ", supported functions are " + boost::algorithm::join(supported_functions, ","),
                ErrorCodes::BAD_ARGUMENTS);
    }
}

String DataTypeCustomSimpleAggregateFunction::getName() const
{
    WriteBufferFromOwnString stream;
    stream << "SimpleAggregateFunction(" << function->getName();

    if (!parameters.empty())
    {
        stream << "(";
        for (size_t i = 0; i < parameters.size(); ++i)
        {
            if (i)
                stream << ", ";
            stream << applyVisitor(FieldVisitorToString(), parameters[i]);
        }
        stream << ")";
    }

    for (const auto & argument_type : argument_types)
        stream << ", " << argument_type->getName();

    stream << ")";
    return stream.str();
}


static std::pair<DataTypePtr, DataTypeCustomDescPtr> create(const ASTPtr & arguments)
{
    String function_name;
    AggregateFunctionPtr function;
    DataTypes argument_types;
    Array params_row;

    if (!arguments || arguments->children.empty())
        throw Exception("Data type SimpleAggregateFunction requires parameters: "
                        "name of aggregate function and list of data types for arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (const ASTFunction * parametric = arguments->children[0]->as<ASTFunction>())
    {
        if (parametric->parameters)
            throw Exception("Unexpected level of parameters to aggregate function", ErrorCodes::SYNTAX_ERROR);
        function_name = parametric->name;

        if (parametric->arguments)
        {
            const ASTs & parameters = parametric->arguments->as<ASTExpressionList &>().children;
            params_row.resize(parameters.size());

            for (size_t i = 0; i < parameters.size(); ++i)
            {
                const ASTLiteral * lit = parameters[i]->as<ASTLiteral>();
                if (!lit)
                    throw Exception(
                        ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS,
                        "Parameters to aggregate functions must be literals. "
                        "Got parameter '{}' for function '{}'",
                        parameters[i]->formatForErrorMessage(),
                        function_name);

                params_row[i] = lit->value;
            }
        }
    }
    else if (auto opt_name = tryGetIdentifierName(arguments->children[0]))
    {
        function_name = *opt_name;
    }
    else if (arguments->children[0]->as<ASTLiteral>())
    {
        throw Exception("Aggregate function name for data type SimpleAggregateFunction must be passed as identifier (without quotes) or function",
                        ErrorCodes::BAD_ARGUMENTS);
    }
    else
        throw Exception("Unexpected AST element passed as aggregate function name for data type SimpleAggregateFunction. Must be identifier or function.",
                        ErrorCodes::BAD_ARGUMENTS);

    for (size_t i = 1; i < arguments->children.size(); ++i)
        argument_types.push_back(DataTypeFactory::instance().get(arguments->children[i]));

    if (function_name.empty())
        throw Exception("Logical error: empty name of aggregate function passed", ErrorCodes::LOGICAL_ERROR);

    AggregateFunctionProperties properties;
    function = AggregateFunctionFactory::instance().get(function_name, argument_types, params_row, properties);

    DataTypeCustomSimpleAggregateFunction::checkSupportedFunctions(function);

    DataTypePtr storage_type = DataTypeFactory::instance().get(argument_types[0]->getName());

    if (!function->getReturnType()->equals(*removeLowCardinality(storage_type)))
    {
        throw Exception("Incompatible data types between aggregate function '" + function->getName() + "' which returns " + function->getReturnType()->getName() + " and column storage type " + storage_type->getName(),
                        ErrorCodes::BAD_ARGUMENTS);
    }

    DataTypeCustomNamePtr custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(function, argument_types, params_row);

    return std::make_pair(storage_type, std::make_unique<DataTypeCustomDesc>(std::move(custom_name), nullptr));
}

void registerDataTypeDomainSimpleAggregateFunction(DataTypeFactory & factory)
{
    factory.registerDataTypeCustom("SimpleAggregateFunction", create);
}

}
