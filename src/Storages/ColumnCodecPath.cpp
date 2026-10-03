#include <Storages/ColumnCodecPath.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
String joinPrefix(const std::vector<String> & parts, size_t count)
{
    String result;
    for (size_t i = 0; i < count; ++i)
    {
        if (i)
            result += '.';
        result += parts[i];
    }
    return result;
}

DataTypePtr unwrapTransparentWrapper(DataTypePtr type)
{
    while (true)
    {
        if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()); array && !isNested(type))
            type = array->getNestedType();
        else if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
            type = nullable->getNestedType();
        else
            return type;
    }
}
}

TupleCodecPath canonicalizeTupleCodecPath(const DataTypePtr & owner_type, const TupleCodecPath & input)
{
    DataTypePtr current = owner_type;
    TupleCodecPath result;
    result.reserve(input.size());

    for (const auto & component : input)
    {
        current = unwrapTransparentWrapper(current);
        const auto * tuple = typeid_cast<const DataTypeTuple *>(current.get());
        if (!tuple)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Codec target crosses unsupported type {} before Tuple element {}", current->getName(), component);

        const auto position = tuple->tryGetPositionByName(component);
        if (!position)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Tuple type {} has no element named {}", tuple->getName(), component);
        result.push_back(tuple->getNameByPosition(*position + 1));
        current = tuple->getElements()[*position];
    }

    return result;
}

ResolvedCodecTarget resolveCodecTarget(const ColumnsDescription & columns, const ASTIdentifier & target)
{
    if (target.name_parts.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec target is empty");

    for (size_t prefix_size = target.name_parts.size(); prefix_size > 0; --prefix_size)
    {
        String owner = joinPrefix(target.name_parts, prefix_size);
        const auto * description = columns.tryGet(owner);
        if (!description)
            continue;

        TupleCodecPath suffix(target.name_parts.begin() + prefix_size, target.name_parts.end());
        if (suffix.empty())
            return {.owner_name = std::move(owner), .path = {}};
        return {
            .owner_name = std::move(owner),
            .path = canonicalizeTupleCodecPath(description->type, suffix),
        };
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec target {} has no physical owning column", target.name());
}

}
