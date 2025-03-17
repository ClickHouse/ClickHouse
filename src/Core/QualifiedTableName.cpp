#include <Core/QualifiedTableName.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

UInt64 QualifiedTableName::hash() const
{
    SipHash hash_state;
    hash_state.update(database.data(), database.size());
    hash_state.update(table.data(), table.size());
    return hash_state.get64();
}

QualifiedTableName QualifiedTableName::parseFromString(const String & maybe_qualified_name)
{
    auto name = tryParseFromString(maybe_qualified_name);
    if (!name)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid qualified name: {}", maybe_qualified_name);
    return *name;
}

}
