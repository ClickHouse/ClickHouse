#include <Storages/MergeTree/StreamBaseManifest.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_FILE_NAME;
}

namespace
{

const char * kindToString(StreamBaseManifest::Kind kind)
{
    return kind == StreamBaseManifest::Kind::Column ? "column" : "skip index";
}

}

void StreamBaseManifest::registerStreamBase(const String & base, Owner owner)
{
    auto [it, inserted] = owners.emplace(base, owner);
    if (inserted)
        return;

    const auto & existing = it->second;

    /// Only a column meeting a skip index is this check's concern: columns-vs-columns is owned by
    /// `MergeTreeDataPartWriterWide::addStreams`. Keep the first claim so a later cross-kind
    /// conflict still names a real owner.
    if (existing.kind == owner.kind)
        return;

    throw Exception(ErrorCodes::INCORRECT_FILE_NAME,
        "Stream base name `{}` is claimed by both {} `{}` and {} `{}`. They would share the same "
        "data and marks files in one part directory. Rename one of them",
        base, kindToString(existing.kind), existing.name, kindToString(owner.kind), owner.name);
}

}
