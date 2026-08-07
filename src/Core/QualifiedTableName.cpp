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

std::string QualifiedTableName::getFullName() const
{
    if (database.empty())
        return table;
    return database + '.' + table;
}

std::optional<QualifiedTableName> QualifiedTableName::tryParseFromString(const String & maybe_qualified_name)
{
    if (maybe_qualified_name.empty())
        return {};

    /// Do not allow dot at the beginning and at the end
    auto pos = maybe_qualified_name.find('.');
    if (pos == 0 || pos == (maybe_qualified_name.size() - 1))
        return {};

    QualifiedTableName name;
    if (pos == std::string::npos)
    {
        name.table = maybe_qualified_name;
    }
    else if (maybe_qualified_name.find('.', pos + 1) != std::string::npos)
    {
        /// Do not allow multiple dots
        return {};
    }
    else
    {
        name.database = maybe_qualified_name.substr(0, pos);
        name.table = maybe_qualified_name.substr(pos + 1);
    }

    return name;
}

}
