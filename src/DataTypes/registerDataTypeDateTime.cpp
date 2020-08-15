
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

template <typename T, ArgumentKind Kind>
std::conditional_t<Kind == ArgumentKind::Optional, std::optional<T>, T>
getArgument(const ASTPtr & arguments, size_t argument_index, const char * argument_name, const std::string context_data_type_name)
{
    using NearestResultType = NearestFieldType<T>;
    const auto field_type = Field::TypeToEnum<NearestResultType>::value;
    const ASTLiteral * argument = nullptr;

    auto exception_message = [=](const String & message)
    {
        return std::string("Parameter #") + std::to_string(argument_index) + " '"
               + argument_name + "' for " + context_data_type_name
               + message
               + ", expected: " + Field::Types::toString(field_type) + " literal.";
    };

    if (!arguments || arguments->children.size() <= argument_index
        || !(argument = arguments->children[argument_index]->as<ASTLiteral>())
        || argument->value.getType() != field_type)
    {
        if constexpr (Kind == ArgumentKind::Optional)
            return {};
        else
            throw Exception(exception_message(" is missing"),
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    return argument->value.get<NearestResultType>();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->size() == 0)
        return std::make_shared<DataTypeDateTime>();

    const auto scale = getArgument<UInt64, ArgumentKind::Optional>(arguments, 0, "scale", "DateTime");
    const auto timezone = getArgument<String, ArgumentKind::Optional>(arguments, !!scale, "timezone", "DateTime");

    if (scale)
        return std::make_shared<DataTypeDateTime64>(scale.value_or(DataTypeDateTime64::default_scale), timezone.value_or(String{}));

    return std::make_shared<DataTypeDateTime>(timezone.value_or(String{}));
}

static DataTypePtr create32(const ASTPtr & arguments)
{
    if (!arguments || arguments->size() == 0)
        return std::make_shared<DataTypeDateTime>();

    if (arguments->children.size() != 1)
        throw Exception("DateTime32 data type can optionally have only one argument - time zone name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto timezone = getArgument<String, ArgumentKind::Mandatory>(arguments, 0, "timezone", "DateTime32");

    return std::make_shared<DataTypeDateTime>(timezone);
}

static DataTypePtr create64(const ASTPtr & arguments)
{
    if (!arguments || arguments->size() == 0)
        return std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale);

    if (arguments->children.size() > 2)
        throw Exception("DateTime64 data type can optionally have two argument - scale and time zone name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto scale = getArgument<UInt64, ArgumentKind::Optional>(arguments, 0, "scale", "DateTime64");
    const auto timezone = getArgument<String, ArgumentKind::Optional>(arguments, !!scale, "timezone", "DateTime64");

    return std::make_shared<DataTypeDateTime64>(scale.value_or(DataTypeDateTime64::default_scale), timezone.value_or(String{}));
}

void registerDataTypeDateTime(DataTypeFactory & factory)
{
    factory.registerDataType("DateTime", create, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("DateTime32", create32, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("DateTime64", create64, DataTypeFactory::CaseInsensitive);

    factory.registerAlias("TIMESTAMP", "DateTime", DataTypeFactory::CaseInsensitive);
}

}
