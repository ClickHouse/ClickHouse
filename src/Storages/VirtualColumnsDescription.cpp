#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_COLUMN;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

VirtualColumnDescription::VirtualColumnDescription(
    String name_, DataTypePtr type_, ASTPtr codec_, String comment_, VirtualsKind kind_, bool is_common_)
    : ColumnDescription(std::move(name_), std::move(type_), std::move(codec_), std::move(comment_))
    , kind(kind_)
    , is_common(is_common_)
{
}

void VirtualColumnsDescription::add(VirtualColumnDescription desc)
{
    if (container.get<1>().contains(desc.name))
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Virtual column {} already exists", desc.name);

    container.get<0>().push_back(std::move(desc));
}

void VirtualColumnsDescription::addEphemeral(String name, DataTypePtr type, String comment, bool is_common)
{
    add({std::move(name), std::move(type), nullptr, std::move(comment), VirtualsKind::Ephemeral, is_common});
}

void VirtualColumnsDescription::addPersistent(String name, DataTypePtr type, ASTPtr codec, String comment, bool is_common)
{
    add({std::move(name), std::move(type), std::move(codec), std::move(comment), VirtualsKind::Persistent, is_common});
}

std::optional<ColumnDefault> VirtualColumnsDescription::getDefault(const String & column_name) const
{
    auto it = container.get<1>().find(column_name);
    if (it != container.get<1>().end() && it->default_desc.expression)
        return it->default_desc;
    return {};
}

std::optional<NameAndTypePair> VirtualColumnsDescription::tryGet(const String & name, VirtualsKind kind) const
{
    auto it = container.get<1>().find(name);
    if (it != container.get<1>().end() && (static_cast<UInt8>(it->kind) & static_cast<UInt8>(kind)))
        return NameAndTypePair{it->name, it->type};
    return {};
}

NameAndTypePair VirtualColumnsDescription::get(const String & name, VirtualsKind kind) const
{
    auto column = tryGet(name, kind);
    if (!column)
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no virtual column {}", name);
    return *column;
}

const VirtualColumnDescription * VirtualColumnsDescription::tryGetDescription(const String & name, VirtualsKind kind) const
{
    auto it = container.get<1>().find(name);
    if (it != container.get<1>().end() && (static_cast<UInt8>(it->kind) & static_cast<UInt8>(kind)))
        return &(*it);
    return nullptr;
}

const VirtualColumnDescription & VirtualColumnsDescription::getDescription(const String & name, VirtualsKind kind) const
{
    const auto * column = tryGetDescription(name, kind);
    if (!column)
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no virtual column {}", name);
    return *column;
}

Block VirtualColumnsDescription::getSampleBlock(VirtualsKind kind, bool exclude_common) const
{
    Block result;
    for (const auto & column : getNamesAndTypesList(kind, exclude_common))
        result.insert({column.type->createColumn(), column.type, column.name});
    return result;
}

NamesAndTypesList VirtualColumnsDescription::getNamesAndTypesList(VirtualsKind kind, bool exclude_common) const
{
    NamesAndTypesList result;
    for (const auto & column : container)
    {
        if (!(static_cast<UInt8>(column.kind) & static_cast<UInt8>(kind)))
            continue;
        if (exclude_common && column.is_common)
            continue;
        result.emplace_back(column.name, column.type);
    }
    return result;
}

}
