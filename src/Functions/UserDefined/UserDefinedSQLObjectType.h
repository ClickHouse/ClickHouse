#pragma once


namespace DB
{

enum class UserDefinedSQLObjectType : uint8_t
{
    Function,

    /// Not a user-defined SQL object: the backup coordination for replicated SQL objects is
    /// generic over this enum, and the backup of named collections reuses it.
    NamedCollection,
};

}
