#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnAggregateFunction.h>

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
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>


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


String DataTypeAggregateFunction::doGetName() const
{
    return getNameImpl(true);
}


String DataTypeAggregateFunction::getNameWithoutVersion() const
{
    return getNameImpl(false);
}


size_t DataTypeAggregateFunction::getVersion() const
{
    if (version)
        return *version;
    return function->getDefaultVersion();
}


String DataTypeAggregateFunction::getNameImpl(bool with_version) const
{
    WriteBufferFromOwnString stream;
    stream << "AggregateFunction(";

    /// If aggregate function does not support versioning its version is 0 and is not printed.
    auto data_type_version = getVersion();
    if (with_version && data_type_version)
        stream << data_type_version << ", ";
    stream << function->getName();

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
    return ColumnAggregateFunction::create(function, getVersion());
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
        function->serialize(place, buffer_from_field, version);
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
    if (typeid(rhs) != typeid(*this))
        return false;

    auto lhs_state_type = function->getStateType();
    auto rhs_state_type = typeid_cast<const DataTypeAggregateFunction &>(rhs).function->getStateType();

    if (typeid(lhs_state_type.get()) != typeid(rhs_state_type.get()))
        return false;

    if (const auto * lhs_state = typeid_cast<const DataTypeAggregateFunction *>(lhs_state_type.get()))
        return lhs_state->getNameWithoutVersion()
            == typeid_cast<const DataTypeAggregateFunction &>(*rhs_state_type).getNameWithoutVersion();

    return lhs_state_type->equals(*rhs_state_type);
}


SerializationPtr DataTypeAggregateFunction::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationAggregateFunction>(function, getName(), getVersion());
}


static DataTypePtr create(const ASTPtr & arguments)
{
    String function_name;
    AggregateFunctionPtr function;
    DataTypes argument_types;
    Array params_row;
    std::optional<size_t> version;

    if (!arguments || arguments->children.empty())
        throw Exception("Data type AggregateFunction requires parameters: "
            "version(optionally), name of aggregate function and list of data types for arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTPtr data_type_ast = arguments->children[0];
    size_t argument_types_start_idx = 1;

    /* If aggregate function definition doesn't have version, it will have in AST children args [ASTFunction, types...] - in case
     * it is parametric, or [ASTIdentifier, types...] - otherwise. If aggregate function has version in AST, then it will be:
     * [ASTLiteral, ASTFunction (or ASTIdentifier), types...].
     */
    if (auto * version_ast = arguments->children[0]->as<ASTLiteral>())
    {
        if (arguments->children.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Data type AggregateFunction has version, but it requires at least one more parameter - name of aggregate function");
        version = version_ast->value.safeGet<UInt64>();
        data_type_ast = arguments->children[1];
        argument_types_start_idx = 2;
    }

    if (const auto * parametric = data_type_ast->as<ASTFunction>())
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
    else if (auto opt_name = tryGetIdentifierName(data_type_ast))
    {
        function_name = *opt_name;
    }
    else if (data_type_ast->as<ASTLiteral>())
    {
        throw Exception("Aggregate function name for data type AggregateFunction must be passed as identifier (without quotes) or function",
            ErrorCodes::BAD_ARGUMENTS);
    }
    else
        throw Exception("Unexpected AST element passed as aggregate function name for data type AggregateFunction. Must be identifier or function.",
            ErrorCodes::BAD_ARGUMENTS);

    for (size_t i = argument_types_start_idx; i < arguments->children.size(); ++i)
        argument_types.push_back(DataTypeFactory::instance().get(arguments->children[i]));

    if (function_name.empty())
        throw Exception("Logical error: empty name of aggregate function passed", ErrorCodes::LOGICAL_ERROR);

    AggregateFunctionProperties properties;
    function = AggregateFunctionFactory::instance().get(function_name, argument_types, params_row, properties);
    return std::make_shared<DataTypeAggregateFunction>(function, argument_types, params_row, version);
}


void registerDataTypeAggregateFunction(DataTypeFactory & factory)
{
    factory.registerDataType("AggregateFunction", create);
}

}
