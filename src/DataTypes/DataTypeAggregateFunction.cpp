#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnAggregateFunction.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/AlignedBuffer.h>
#include <Common/FieldVisitorToString.h>

#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/Serializations/SerializationAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>


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


std::string DataTypeAggregateFunction::doGetName() const
{
    WriteBufferFromOwnString stream;
    stream << "AggregateFunction(" << function->getName();

    if (!parameters.empty())
    {
        stream << '(';
        for (size_t i = 0; i < parameters.size(); ++i)
        {
            if (i)
                stream << ", ";
            stream << applyVisitor(FieldVisitorToString(), parameters[i]);
        }
        stream << ')';
    }

    for (const auto & argument_type : argument_types)
        stream << ", " << argument_type->getName();

    stream << ')';
    return stream.str();
}

MutableColumnPtr DataTypeAggregateFunction::createColumn() const
{
    return ColumnAggregateFunction::create(function);
}


/// Create empty state
Field DataTypeAggregateFunction::getDefault() const
{
    Field field = AggregateFunctionStateData();
    field.get<AggregateFunctionStateData &>().name = getName();

    AlignedBuffer place_buffer(function->sizeOfData(), function->alignOfData());
    AggregateDataPtr place = place_buffer.data();

    function->create(place);

    try
    {
        WriteBufferFromString buffer_from_field(field.get<AggregateFunctionStateData &>().data);
        function->serialize(place, buffer_from_field);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }

    function->destroy(place);

    return field;
}


bool DataTypeAggregateFunction::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && getName() == rhs.getName();
}

SerializationPtr DataTypeAggregateFunction::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationAggregateFunction>(function, getName());
}


static DataTypePtr create(const ASTPtr & arguments)
{
    String function_name;
    AggregateFunctionPtr function;
    DataTypes argument_types;
    Array params_row;

    if (!arguments || arguments->children.empty())
        throw Exception("Data type AggregateFunction requires parameters: "
            "name of aggregate function and list of data types for arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (const auto * parametric = arguments->children[0]->as<ASTFunction>())
    {
        if (parametric->parameters)
            throw Exception("Unexpected level of parameters to aggregate function", ErrorCodes::SYNTAX_ERROR);
        function_name = parametric->name;

        if (parametric->arguments)
        {
            const ASTs & parameters = parametric->arguments->children;
            params_row.resize(parameters.size());

            for (size_t i = 0; i < parameters.size(); ++i)
            {
                const auto * literal = parameters[i]->as<ASTLiteral>();
                if (!literal)
                    throw Exception(
                        ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS,
                        "Parameters to aggregate functions must be literals. "
                        "Got parameter '{}' for function '{}'",
                        parameters[i]->formatForErrorMessage(), function_name);

                params_row[i] = literal->value;
            }
        }
    }
    else if (auto opt_name = tryGetIdentifierName(arguments->children[0]))
    {
        function_name = *opt_name;
    }
    else if (arguments->children[0]->as<ASTLiteral>())
    {
        throw Exception("Aggregate function name for data type AggregateFunction must be passed as identifier (without quotes) or function",
            ErrorCodes::BAD_ARGUMENTS);
    }
    else
        throw Exception("Unexpected AST element passed as aggregate function name for data type AggregateFunction. Must be identifier or function.",
            ErrorCodes::BAD_ARGUMENTS);

    for (size_t i = 1; i < arguments->children.size(); ++i)
        argument_types.push_back(DataTypeFactory::instance().get(arguments->children[i]));

    if (function_name.empty())
        throw Exception("Logical error: empty name of aggregate function passed", ErrorCodes::LOGICAL_ERROR);

    AggregateFunctionProperties properties;
    function = AggregateFunctionFactory::instance().get(function_name, argument_types, params_row, properties);
    return std::make_shared<DataTypeAggregateFunction>(function, argument_types, params_row);
}

void registerDataTypeAggregateFunction(DataTypeFactory & factory)
{
    factory.registerDataType("AggregateFunction", create);
}


}
