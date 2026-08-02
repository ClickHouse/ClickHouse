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
    if (existing.kind == owner.kind && existing.name == owner.name)
        return;

    /// Two columns may legitimately share one base (Nested array sizes); that direction is owned by
    /// `MergeTreeDataPartWriterWide::addStreams`. Keep the first claim so a later index conflict
    /// still names a real column.
    if (existing.kind == Kind::Column && owner.kind == Kind::Column)
        return;

    throw Exception(ErrorCodes::INCORRECT_FILE_NAME,
        "Stream base name `{}` is claimed by both {} `{}` and {} `{}`. They would share the same "
        "data and marks files in one part directory. Rename one of them",
        base, kindToString(existing.kind), existing.name, kindToString(owner.kind), owner.name);
}

}
