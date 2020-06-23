#include <Interpreters/AggregateDescription.h>
#include <Common/FieldVisitors.h>

namespace DB
{

Strings AggregateDescription::explain() const
{
    Strings res;
    String arguments_pos_str;
    for (auto arg : arguments)
    {
        if (!arguments_pos_str.empty())
            arguments_pos_str += ", ";

        arguments_pos_str += std::to_string(arg);
    }

    if (arguments_pos_str.empty())
        arguments_pos_str = "none";

    res.emplace_back("argument positions: " + arguments_pos_str);

    String arguments_names_str;
    for (const auto & arg : argument_names)
    {
        if (!arguments_names_str.empty())
            arguments_names_str += ", ";

        arguments_names_str += arg;
    }

    if (arguments_names_str.empty())
        arguments_names_str = "none";

    res.emplace_back("arguments: " + arguments_names_str);
    res.emplace_back("column_name: " + column_name);

    auto get_params_string = [](const Array & arr)
    {
        String params_str;
        for (const auto & param : arr)
        {
            if (!params_str.empty())
                params_str += ", ";

            params_str += applyVisitor(FieldVisitorToString(), param);
        }

        return params_str;
    };

    if (function)
    {
        String types_str;
        for (const auto & type : function->getArgumentTypes())
        {
            if (!types_str.empty())
                types_str += ", ";

            types_str += type->getName();
        }

        auto params_str = get_params_string(function->getParameters());
        if (!params_str.empty())
            params_str = "(" + params_str + ")";

        res.emplace_back("function: " + function->getName() + params_str + '(' + types_str + ") -> " +
                         function->getReturnType()->getName());
    }
    else
        res.emplace_back("function: nullptr");

    if (!parameters.empty())
        res.emplace_back("parameters: " + get_params_string(parameters));

    return res;
}

}
