#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnAggregateFunction.h>

#include <Common/SipHash.h>
#include <Common/AlignedBuffer.h>
#include <Common/FieldVisitorToString.h>

#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/Serializations/SerializationAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/transformTypesRecursively.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
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


DataTypeAggregateFunction::DataTypeAggregateFunction(AggregateFunctionPtr function_, const DataTypes & argument_types_,
                            const Array & parameters_, std::optional<size_t> version_)
    : function(std::move(function_))
    , argument_types(argument_types_)
    , parameters(parameters_)
    , version(version_)
{
}

String DataTypeAggregateFunction::getFunctionName() const
{
    return function->getName();
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

DataTypePtr DataTypeAggregateFunction::getReturnType() const
{
    return function->getResultType();
}

DataTypePtr DataTypeAggregateFunction::getReturnTypeToPredict() const
{
    return function->getReturnTypeToPredict();
}

bool DataTypeAggregateFunction::isVersioned() const
{
    return function->isVersioned();
}

void DataTypeAggregateFunction::updateVersionFromRevision(size_t revision, bool if_empty) const
{
    setVersion(function->getVersionFromRevision(revision), if_empty);
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
        for (size_t i = 0, size = parameters.size(); i < size; ++i)
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
    field.safeGet<AggregateFunctionStateData>().name = getName();

    AlignedBuffer place_buffer(function->sizeOfData(), function->alignOfData());
    AggregateDataPtr place = place_buffer.data();

    function->create(place);

    try
    {
        WriteBufferFromString buffer_from_field(field.safeGet<AggregateFunctionStateData>().data);
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

bool DataTypeAggregateFunction::strictEquals(const DataTypePtr & lhs_state_type, const DataTypePtr & rhs_state_type)
{
    const auto * lhs_state = typeid_cast<const DataTypeAggregateFunction *>(lhs_state_type.get());
    const auto * rhs_state = typeid_cast<const DataTypeAggregateFunction *>(rhs_state_type.get());

    if (!lhs_state || !rhs_state)
        return false;

    if (lhs_state->function->getName() != rhs_state->function->getName())
        return false;

    if (lhs_state->parameters.size() != rhs_state->parameters.size())
        return false;

    for (size_t i = 0; i < lhs_state->parameters.size(); ++i)
        if (lhs_state->parameters[i] != rhs_state->parameters[i])
            return false;

    if (lhs_state->argument_types.size() != rhs_state->argument_types.size())
        return false;

    for (size_t i = 0; i < lhs_state->argument_types.size(); ++i)
        if (!lhs_state->argument_types[i]->equals(*rhs_state->argument_types[i]))
            return false;

    return true;
}

void DataTypeAggregateFunction::updateHashImpl(SipHash & hash) const
{
    hash.update(getFunctionName());
    hash.update(parameters.size());
    for (const auto & param : parameters)
        hash.update(param.getType());
    hash.update(argument_types.size());
    for (const auto & arg_type : argument_types)
        arg_type->updateHash(hash);
    if (version)
        hash.update(*version);
}

bool DataTypeAggregateFunction::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    auto lhs_state_type = function->getNormalizedStateType();
    auto rhs_state_type = typeid_cast<const DataTypeAggregateFunction &>(rhs).function->getNormalizedStateType();

    return strictEquals(lhs_state_type, rhs_state_type);
}


SerializationPtr DataTypeAggregateFunction::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationAggregateFunction>(function, getName(), getVersion());
}


static DataTypePtr create(const ASTPtr & arguments)
{
    String function_name;
    DataTypes argument_types;
    Array params_row;
    std::optional<size_t> version;

    if (!arguments || arguments->children.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Data type AggregateFunction requires parameters: "
                        "version(optionally), name of aggregate function and list of data types for arguments");

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

    auto action = NullsAction::EMPTY;
    if (const auto * parametric = data_type_ast->as<ASTFunction>())
    {
        if (parametric->parameters)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected level of parameters to aggregate function");

        function_name = parametric->name;
        action = parametric->nulls_action;

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
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Aggregate function name for data type AggregateFunction must "
                        "be passed as identifier (without quotes) or function");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unexpected AST element {} passed as aggregate function name for data type AggregateFunction. "
                        "Must be identifier or function", data_type_ast->getID());

    for (size_t i = argument_types_start_idx; i < arguments->children.size(); ++i)
        argument_types.push_back(DataTypeFactory::instance().get(arguments->children[i]));

    if (function_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty name of aggregate function passed");

    AggregateFunctionProperties properties;
    AggregateFunctionPtr function = AggregateFunctionFactory::instance().get(function_name, action, argument_types, params_row, properties);
    return std::make_shared<DataTypeAggregateFunction>(function, argument_types, params_row, version);
}

void setVersionToAggregateFunctions(DataTypePtr & type, bool if_empty, std::optional<size_t> revision)
{
    auto callback = [revision, if_empty](DataTypePtr & column_type)
    {
        const auto * aggregate_function_type = typeid_cast<const DataTypeAggregateFunction *>(column_type.get());
        if (aggregate_function_type && aggregate_function_type->isVersioned())
        {
            if (revision)
                aggregate_function_type->updateVersionFromRevision(*revision, if_empty);
            else
                aggregate_function_type->setVersion(0, if_empty);
        }
    };

    callOnNestedSimpleTypes(type, callback);
}


void registerDataTypeAggregateFunction(DataTypeFactory & factory)
{
    factory.registerDataType("AggregateFunction", create);
}

bool hasAggregateFunctionType(const DataTypePtr & type)
{
    auto result = false;
    auto check = [&](const IDataType & t)
    {
        result |= WhichDataType(t).isAggregateFunction();
    };

    check(*type);
    type->forEachChild(check);
    return result;
}

}
