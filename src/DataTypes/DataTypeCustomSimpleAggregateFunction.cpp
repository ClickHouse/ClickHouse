#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
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
    static const std::vector<String> supported_functions{
        "any",
        "anyLast",
        "min",
        "max",
        "sum",
        "sumWithOverflow",
        "groupBitAnd",
        "groupBitOr",
        "groupBitXor",
        "sumMap",
        "minMap",
        "maxMap",
        "groupArrayArray",
        "groupArrayLastArray",
        "groupUniqArrayArray",
        "sumMappedArrays",
        "minMappedArrays",
        "maxMappedArrays",
    };

    // check function
    if (std::find(std::begin(supported_functions), std::end(supported_functions), function->getName()) == std::end(supported_functions))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported aggregate function {}, supported functions are {}",
                function->getName(), boost::algorithm::join(supported_functions, ","));
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
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Data type SimpleAggregateFunction requires parameters: "
                        "name of aggregate function and list of data types for arguments");

    if (const ASTFunction * parametric = arguments->children[0]->as<ASTFunction>())
    {
        if (parametric->parameters)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected level of parameters to aggregate function");
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Aggregate function name for data type SimpleAggregateFunction must "
                        "be passed as identifier (without quotes) or function");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unexpected AST element passed as aggregate function name for data type "
                        "SimpleAggregateFunction. Must be identifier or function.");

    for (size_t i = 1; i < arguments->children.size(); ++i)
        argument_types.push_back(DataTypeFactory::instance().get(arguments->children[i]));

    if (function_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty name of aggregate function passed");

    AggregateFunctionProperties properties;
    /// NullsAction is not part of the type definition, instead it will have transformed the function into a different one
    auto action = NullsAction::EMPTY;
    function = AggregateFunctionFactory::instance().get(function_name, action, argument_types, params_row, properties);

    DataTypeCustomSimpleAggregateFunction::checkSupportedFunctions(function);

    DataTypePtr storage_type = DataTypeFactory::instance().get(argument_types[0]->getName());

    if (!function->getResultType()->equals(*removeLowCardinality(storage_type)))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incompatible data types between aggregate function '{}' "
                        "which returns {} and column storage type {}",
                        function->getName(), function->getResultType()->getName(), storage_type->getName());
    }

    DataTypeCustomNamePtr custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(function, argument_types, params_row);

    return std::make_pair(storage_type, std::make_unique<DataTypeCustomDesc>(std::move(custom_name), nullptr));
}

String DataTypeCustomSimpleAggregateFunction::getFunctionName() const
{
    return function->getName();
}

DataTypePtr createSimpleAggregateFunctionType(const AggregateFunctionPtr & function, const DataTypes & argument_types, const Array & parameters)
{
    auto custom_desc = std::make_unique<DataTypeCustomDesc>(
        std::make_unique<DataTypeCustomSimpleAggregateFunction>(function, argument_types, parameters));

    return DataTypeFactory::instance().getCustom(std::move(custom_desc));
}

void registerDataTypeDomainSimpleAggregateFunction(DataTypeFactory & factory)
{
    factory.registerDataTypeCustom("SimpleAggregateFunction", create);
}

}
