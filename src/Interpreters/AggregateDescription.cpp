#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <IO/Operators.h>
#include <Interpreters/AggregateDescription.h>
#include <Common/FieldVisitorToString.h>
#include <Common/JSONBuilder.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Parsers/NullsAction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void AggregateDescription::explain(WriteBuffer & out, size_t indent) const
{
    String prefix(indent, ' ');

    out << prefix << column_name << '\n';

    auto dump_params = [&](const Array & arr)
    {
        bool first = true;
        for (const auto & param : arr)
        {
            if (!first)
                out << ", ";

            first = false;

            out << applyVisitor(FieldVisitorToString(), param);
        }
    };

    if (function)
    {
        /// Double whitespace is intentional.
        out << prefix << "  Function: " << function->getName();

        const auto & params = function->getParameters();
        if (!params.empty())
        {
            out << "(";
            dump_params(params);
            out << ")";
        }

        out << "(";

        bool first = true;
        for (const auto & type : function->getArgumentTypes())
        {
            if (!first)
                out << ", ";
            first = false;

            out << type->getName();
        }

        out << ") → " << function->getResultType()->getName() << "\n";
    }
    else
        out << prefix << "  Function: nullptr\n";

    if (!parameters.empty())
    {
        out << prefix << "  Parameters: ";
        dump_params(parameters);
        out << '\n';
    }

    out << prefix << "  Arguments: ";

    if (argument_names.empty())
        out << "none\n";
    else
    {
        bool first = true;
        for (const auto & arg : argument_names)
        {
            if (!first)
                out << ", ";
            first = false;

            out << arg;
        }
        out << "\n";
    }
}

void AggregateDescription::explain(JSONBuilder::JSONMap & map) const
{
    map.add("Name", column_name);

    if (function)
    {
        auto function_map = std::make_unique<JSONBuilder::JSONMap>();

        function_map->add("Name", function->getName());

        const auto & params = function->getParameters();
        if (!params.empty())
        {
            auto params_array = std::make_unique<JSONBuilder::JSONArray>();
            for (const auto & param : params)
                params_array->add(applyVisitor(FieldVisitorToString(), param));

            function_map->add("Parameters", std::move(params_array));
        }

        auto args_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & type : function->getArgumentTypes())
            args_array->add(type->getName());

        function_map->add("Argument Types", std::move(args_array));
        function_map->add("Result Type", function->getResultType()->getName());

        map.add("Function", std::move(function_map));
    }

    auto args_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & name : argument_names)
        args_array->add(name);

    map.add("Arguments", std::move(args_array));
}

void serializeAggregateDescriptions(const AggregateDescriptions & aggregates, WriteBuffer & out)
{
    writeVarUInt(aggregates.size(), out);
    for (const auto & aggregate : aggregates)
    {
        writeStringBinary(aggregate.column_name, out);

        UInt64 num_args = aggregate.argument_names.size();
        const auto & argument_types = aggregate.function->getArgumentTypes();

        if (argument_types.size() != num_args)
        {
            WriteBufferFromOwnString buf;
            aggregate.explain(buf, 0);
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Invalid number of for aggregate function. Expected {}, got {}. Description:\n{}",
                argument_types.size(), num_args, buf.str());
        }

        writeVarUInt(num_args, out);
        for (size_t i = 0; i < num_args; ++i)
        {
            writeStringBinary(aggregate.argument_names[i], out);
            encodeDataType(argument_types[i], out);
        }

        writeStringBinary(aggregate.function->getName(), out);

        writeVarUInt(aggregate.parameters.size(), out);
        for (const auto & param : aggregate.parameters)
            writeFieldBinary(param, out);
    }
}

void deserializeAggregateDescriptions(AggregateDescriptions & aggregates, ReadBuffer & in)
{
    UInt64 num_aggregates;
    readVarUInt(num_aggregates, in);
    aggregates.resize(num_aggregates);
    for (auto & aggregate : aggregates)
    {
        readStringBinary(aggregate.column_name, in);

        UInt64 num_args;
        readVarUInt(num_args, in);
        aggregate.argument_names.resize(num_args);

        DataTypes argument_types;
        argument_types.reserve(num_args);

        for (auto & arg_name : aggregate.argument_names)
        {
            readStringBinary(arg_name, in);
            argument_types.emplace_back(decodeDataType(in));
        }

        String function_name;
        readStringBinary(function_name, in);

        UInt64 num_params;
        readVarUInt(num_params, in);
        aggregate.parameters.resize(num_params);
        for (auto & param : aggregate.parameters)
            param = readFieldBinary(in);

        auto action = NullsAction::EMPTY; /// As I understand, it should be resolved to function name.
        AggregateFunctionProperties properties;
        aggregate.function = AggregateFunctionFactory::instance().get(
            function_name, action, argument_types, aggregate.parameters, properties);
    }

}

}
