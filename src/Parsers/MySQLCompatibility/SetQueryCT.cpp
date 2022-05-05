#include <Parsers/MySQLCompatibility/SetQueryCT.h>
#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/util.h>

#include <Parsers/ASTSetQuery.h>
#include <Common/SettingsChanges.h>

namespace MySQLCompatibility
{

// TODO: multiple key-value pairs in SET query
bool SetQueryCT::setup(String &)
{
    MySQLPtr key_node = TreePath({"internalVariableName", "pureIdentifier"}).evaluate(_source);

    if (key_node == nullptr)
        return false;

    const String & key = key_node->terminals[0];

    MySQLPtr value_node = TreePath({"setExprOrDefault", "textStringLiteral"}).evaluate(_source);

    if (value_node == nullptr)
        return false;

    String value = removeQuotes(value_node->terminals[0]);

    _key_value_list.push_back({key, value});

    return true;
}

void SetQueryCT::convert(CHPtr & ch_tree) const
{
    auto query = std::make_shared<DB::ASTSetQuery>();

    DB::SettingsChanges changes;
    for (const auto & key_value : _key_value_list)
    {
        changes.push_back(DB::SettingChange{});

        changes.back().name = key_value.first;
        changes.back().value = key_value.second;
    }
    query->is_standalone = false;
    query->changes = std::move(changes);

    ch_tree = query;
}
}
