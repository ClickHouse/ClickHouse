#include <Interpreters/AggregateDescription.h>
#include <Common/FieldVisitorToString.h>
#include <IO/Operators.h>

#include <Common/JSONBuilder.h>


namespace DB
{

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

        out << ") → " << function->getReturnType()->getName() << "\n";
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

    out << prefix << "  Argument positions: ";

    if (arguments.empty())
        out << "none\n";
    else
    {
        bool first = true;
        for (auto arg : arguments)
        {
            if (!first)
                out << ", ";
            first = false;

            out << arg;
        }
        out << '\n';
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
        function_map->add("Result Type", function->getReturnType()->getName());

        map.add("Function", std::move(function_map));
    }

    auto args_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & name : argument_names)
        args_array->add(name);

    map.add("Arguments", std::move(args_array));

    if (!arguments.empty())
    {
        auto args_pos_array = std::make_unique<JSONBuilder::JSONArray>();
        for (auto pos : arguments)
            args_pos_array->add(pos);

        map.add("Argument Positions", std::move(args_pos_array));
    }
}

}
