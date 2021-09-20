
#include <Core/Field.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

enum class ArgumentKind
{
    Optional,
    Mandatory
};

String getExceptionMessage(
    const String & message, size_t argument_index, const char * argument_name,
    const std::string & context_data_type_name, Field::Types::Which field_type)
{
    return std::string("Parameter #") + std::to_string(argument_index) + " '"
           + argument_name + "' for " + context_data_type_name
           + message
           + ", expected: " + Field::Types::toString(field_type) + " literal.";
}

template <typename T, ArgumentKind Kind>
std::conditional_t<Kind == ArgumentKind::Optional, std::optional<T>, T>
getArgument(const ASTPtr & arguments, size_t argument_index, const char * argument_name [[maybe_unused]], const std::string context_data_type_name)
{
    using NearestResultType = NearestFieldType<T>;
    const auto field_type = Field::TypeToEnum<NearestResultType>::value;
    const ASTLiteral * argument = nullptr;

    if (!arguments || arguments->children.size() <= argument_index
        || !(argument = arguments->children[argument_index]->as<ASTLiteral>())
        || argument->value.getType() != field_type)
    {
        if constexpr (Kind == ArgumentKind::Optional)
            return {};
        else
        {
            if (argument && argument->value.getType() != field_type)
                throw Exception(getExceptionMessage(String(" has wrong type: ") + argument->value.getTypeName(),
                    argument_index, argument_name, context_data_type_name, field_type), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            else
                throw Exception(getExceptionMessage(" is missing", argument_index, argument_name, context_data_type_name, field_type),
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
    }

    return argument->value.get<NearestResultType>();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeDateTime>();

    const auto scale = getArgument<UInt64, ArgumentKind::Optional>(arguments, 0, "scale", "DateTime");
    const auto timezone = getArgument<String, ArgumentKind::Optional>(arguments, !!scale, "timezone", "DateTime");

    if (!scale && !timezone)
        throw Exception(getExceptionMessage(" has wrong type: ", 0, "scale", "DateTime", Field::Types::Which::UInt64),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    /// If scale is defined, the data type is DateTime when scale = 0 otherwise the data type is DateTime64
    if (scale && scale.value() != 0)
        return std::make_shared<DataTypeDateTime64>(scale.value(), timezone.value_or(String{}));

    return std::make_shared<DataTypeDateTime>(timezone.value_or(String{}));
}

static DataTypePtr create32(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeDateTime>();

    if (arguments->children.size() != 1)
        throw Exception("DateTime32 data type can optionally have only one argument - time zone name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto timezone = getArgument<String, ArgumentKind::Mandatory>(arguments, 0, "timezone", "DateTime32");

    return std::make_shared<DataTypeDateTime>(timezone);
}

static DataTypePtr create64(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale);

    if (arguments->children.size() > 2)
        throw Exception("DateTime64 data type can optionally have two argument - scale and time zone name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto scale = getArgument<UInt64, ArgumentKind::Mandatory>(arguments, 0, "scale", "DateTime64");
    const auto timezone = getArgument<String, ArgumentKind::Optional>(arguments, 1, "timezone", "DateTime64");

    return std::make_shared<DataTypeDateTime64>(scale, timezone.value_or(String{}));
}

void registerDataTypeDateTime(DataTypeFactory & factory)
{
    factory.registerDataType("DateTime", create, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("DateTime32", create32, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("DateTime64", create64, DataTypeFactory::CaseInsensitive);

    factory.registerAlias("TIMESTAMP", "DateTime", DataTypeFactory::CaseInsensitive);
}

}
