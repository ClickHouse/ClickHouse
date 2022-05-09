#include <Parsers/MySQLCompatibility/SetQueryCT.h>
#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/util.h>

#include <Parsers/ASTSetQuery.h>
#include <Common/SettingsChanges.h>

namespace MySQLCompatibility
{

static String convertArg(String query)
{
    // MySQL argument is case insensetive
    // ClickHouse argument is in lowercase
    std::transform(query.begin(), query.end(), query.begin(), [](unsigned char c) { return std::tolower(c); });

    const String prefix = "sql_select_limit";
    if (query.starts_with(prefix))
        return "limit" + std::string(query.data() + prefix.length());

    return query;
}

// TODO: support all types of sets
static bool tryExtractSetting(MySQLPtr node, String & key, DB::Field & value)
{
    // FIXME: use tryExtractIdentifier?
    MySQLPtr key_node = TreePath({"internalVariableName", "pureIdentifier"}).find(node);

    if (key_node == nullptr)
        return false;

    key = key_node->terminals[0];
    key = convertArg(key);

    MySQLPtr value_node = TreePath({"setExprOrDefault", "literal"}).find(node);

    return (value_node != nullptr) && tryExtractLiteral(value_node, value);
}

// TODO: multiple key-value pairs in SET query, other complex syntax
bool SetQueryCT::setup(String & error)
{
    const MySQLPtr & set_statement_node = getSourceNode();

    MySQLPtr first_option_node = TreePath({"optionValueNoOptionType"}).find(set_statement_node);

    if (first_option_node == nullptr)
    {
        error = "unsupported set query";
        return false;
    }

    String key;
    DB::Field value;
    if (!tryExtractSetting(first_option_node, key, value))
    {
        error = "invalid set options";
        return false;
    }

    _key_value_list.push_back({key, value});

    MySQLPtr rest_options_node = TreePath({"optionValueListContinued"}).find(set_statement_node);

    if (rest_options_node != nullptr)
    {
        for (const auto & child : rest_options_node->children)
        {
            if (!tryExtractSetting(child, key, value))
            {
                error = "invalid set options";
                return false;
            }
            _key_value_list.push_back({key, value});
        }
    }

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
