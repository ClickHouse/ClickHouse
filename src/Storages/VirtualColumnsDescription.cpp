#include <Storages/ColumnsDescription.h>
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_COLUMN;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

VirtualColumnDescription::VirtualColumnDescription(
    String name_, DataTypePtr type_, ASTPtr codec_, String comment_, VirtualsKind kind_, VirtualsMaterializationPlace place_)
    : ColumnDescription(std::move(name_), std::move(type_), std::move(codec_), std::move(comment_))
    , kind(kind_)
    , place(place_)
{
}

void VirtualColumnsDescription::add(VirtualColumnDescription desc)
{
    chassert(!desc.name.empty());

    if (container.get<1>().contains(desc.name))
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Virtual column {} already exists", desc.name);

    container.get<0>().push_back(std::move(desc));
}

void VirtualColumnsDescription::addEphemeral(String name, DataTypePtr type, String comment, VirtualsMaterializationPlace place)
{
    add({std::move(name), std::move(type), nullptr, std::move(comment), VirtualsKind::Ephemeral, place});
}

void VirtualColumnsDescription::addPersistent(String name, DataTypePtr type, ASTPtr codec, String comment)
{
    add({std::move(name), std::move(type), std::move(codec), std::move(comment), VirtualsKind::Persistent, VirtualsMaterializationPlace::Reader});
}

std::optional<ColumnDefault> VirtualColumnsDescription::getDefault(const String & column_name) const
{
    auto it = container.get<1>().find(column_name);
    if (it != container.get<1>().end() && it->default_desc.expression)
        return it->default_desc;
    return {};
}

std::optional<NameAndTypePair> VirtualColumnsDescription::tryGet(const String & name, VirtualsKind kind, VirtualsMaterializationPlace place) const
{
    if (auto it = container.get<1>().find(name); it != container.get<1>().end())
        if (static_cast<UInt8>(it->kind) & static_cast<UInt8>(kind))
            if (static_cast<UInt8>(it->place) & static_cast<UInt8>(place))
                return NameAndTypePair{it->name, it->type};

    return {};
}

NameAndTypePair VirtualColumnsDescription::get(const String & name, VirtualsKind kind, VirtualsMaterializationPlace place) const
{
    if (auto column = tryGet(name, kind, place))
        return std::move(*column);

    throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no virtual column {}", name);
}

const VirtualColumnDescription * VirtualColumnsDescription::tryGetDescription(const String & name, VirtualsKind kind, VirtualsMaterializationPlace place) const
{
    if (auto it = container.get<1>().find(name); it != container.get<1>().end())
        if (static_cast<UInt8>(it->kind) & static_cast<UInt8>(kind))
            if (static_cast<UInt8>(it->place) & static_cast<UInt8>(place))
                return &(*it);

    return nullptr;
}

const VirtualColumnDescription & VirtualColumnsDescription::getDescription(const String & name, VirtualsKind kind, VirtualsMaterializationPlace place) const
{
    if (const auto * column = tryGetDescription(name, kind, place))
        return *column;

    throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no virtual column {}", name);
}

Block VirtualColumnsDescription::getSampleBlock(VirtualsKind kind, VirtualsMaterializationPlace place) const
{
    Block result;
    for (const auto & desc : container)
        if (static_cast<UInt8>(desc.kind) & static_cast<UInt8>(kind))
            if (static_cast<UInt8>(desc.place) & static_cast<UInt8>(place))
                result.insert({desc.type->createColumn(), desc.type, desc.name});

    return result;
}

}
