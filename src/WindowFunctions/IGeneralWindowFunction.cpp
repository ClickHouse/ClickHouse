#include <WindowFunctions/IGeneralWindowFunction.h>

namespace DB
{

IGeneralWindowFunction::IGeneralWindowFunction(String name_, DataTypes argument_types_, Array parameters_, DataTypePtr result_type_)
    : name(std::move(name_))
    , argument_types(std::move(argument_types_))
    , parameters(std::move(parameters_))
    , result_type(std::move(result_type_))
{
}

const String & IGeneralWindowFunction::getName() const
{
    return name;
}

const DataTypes & IGeneralWindowFunction::getArgumentTypes() const
{
    return argument_types;
}

const Array & IGeneralWindowFunction::getParameters() const
{
    return parameters;
}

const DataTypePtr & IGeneralWindowFunction::getResultType() const
{
    return result_type;
}

WindowFunctionRequirements IGeneralWindowFunction::getRequirements(const WindowDescription &) const
{
    return {};
}

std::optional<WindowFrame> IGeneralWindowFunction::getDefaultFrame() const
{
    return {};
}

void IGeneralWindowFunction::checkWindowFrameType(const WindowDescription &) const
{
}

size_t IGeneralWindowFunction::sizeOfState() const
{
    return 0;
}

size_t IGeneralWindowFunction::alignOfState() const
{
    return 1;
}

void IGeneralWindowFunction::createState(char *) const
{
}

void IGeneralWindowFunction::destroyState(char *) const noexcept
{
}

void IGeneralWindowFunction::resetPartition(char *) const
{
}

}
