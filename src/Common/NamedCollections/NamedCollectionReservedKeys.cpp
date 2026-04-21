#include <Common/NamedCollections/NamedCollectionReservedKeys.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void assertNoReservedKeys(const SettingsChanges & changes)
{
    for (const auto & change : changes)
    {
        if (isReservedNamedCollectionKey(change.name))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Named collection key `{}` starts with the reserved prefix `{}`, which is internal to ClickHouse "
                "and cannot be set from user DDL. Use the dedicated DDL (e.g. `CREATE REPLICA`) to create "
                "specialised named collections instead.",
                change.name,
                NAMED_COLLECTION_RESERVED_KEY_PREFIX);
        }
    }
}

void assertNoReservedKeys(const std::vector<String> & delete_keys)
{
    for (const auto & key : delete_keys)
    {
        if (isReservedNamedCollectionKey(key))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Named collection key `{}` starts with the reserved prefix `{}`, which is internal to ClickHouse "
                "and cannot be deleted from user DDL.",
                key,
                NAMED_COLLECTION_RESERVED_KEY_PREFIX);
        }
    }
}

}
