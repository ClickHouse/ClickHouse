#include <RoutingTable.h>

#include <Common/Exception.h>

#include <Poco/String.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <fstream>
#include <string_view>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int CANNOT_OPEN_FILE;
    extern const int INVALID_CONFIG_PARAMETER;
}
}

namespace DB::Proxy
{

namespace
{

std::vector<String> splitList(const String & list)
{
    std::vector<String> res;
    if (list.empty())
        return res;
    boost::split(res, list, boost::is_any_of(","));
    for (auto & value : res)
        boost::trim(value);
    return res;
}

}

String substituteCaptures(const String & pattern, const std::vector<String> & captures)
{
    String res;
    res.reserve(pattern.size());
    for (size_t i = 0; i < pattern.size(); ++i)
    {
        if (pattern[i] == '$' && i + 1 < pattern.size())
        {
            char next = pattern[i + 1];
            if (next >= '1' && next <= '9')
            {
                size_t index = static_cast<size_t>(next - '1');
                if (index >= captures.size())
                    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "Backend template references capture ${} but the matched rule has only {} capture groups",
                        next, captures.size());
                res += captures[index];
                ++i;
                continue;
            }
            if (next == '$')
            {
                res += '$';
                ++i;
                continue;
            }
        }
        res += pattern[i];
    }
    return res;
}

ConfigRoutingTable::Matcher ConfigRoutingTable::makeMatcher(const String & exact, const String & regexp, bool case_insensitive)
{
    Matcher matcher;
    matcher.values = splitList(exact);
    if (case_insensitive)
        for (auto & value : matcher.values)
            value = Poco::toLower(value);
    if (!regexp.empty())
    {
        re2::RE2::Options options(re2::RE2::Quiet);
        options.set_case_sensitive(!case_insensitive);
        matcher.regexp = std::make_shared<re2::RE2>(regexp, options);
        if (!matcher.regexp->ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                "Cannot compile regexp '{}' in a routing rule: {}", regexp, matcher.regexp->error());
    }
    return matcher;
}

namespace
{

/// Whether a token is a public key algorithm name, as it appears in an `authorized_keys` entry:
/// `ssh-rsa`, `ssh-ed25519`, `ssh-dss`, `ecdsa-sha2-nistp256`, `sk-ssh-ed25519@openssh.com`, ...
bool isKeyType(std::string_view token)
{
    return token.starts_with("ssh-") || token.starts_with("ecdsa-sha2-")
        || token.starts_with("sk-ssh-") || token.starts_with("sk-ecdsa-");
}

/// Split an `authorized_keys` entry into whitespace-separated tokens, keeping a quoted options
/// value such as `command="echo hello"` in a single token, as `sshd` does.
std::vector<std::string_view> tokenize(std::string_view line)
{
    std::vector<std::string_view> tokens;
    bool in_quotes = false;
    size_t token_begin = String::npos;

    for (size_t i = 0; i < line.size(); ++i)
    {
        if (line[i] == '\\' && in_quotes && i + 1 < line.size())
        {
            ++i;
            continue;
        }
        if (line[i] == '"')
            in_quotes = !in_quotes;

        const bool is_separator = !in_quotes && (line[i] == ' ' || line[i] == '\t');
        if (is_separator)
        {
            if (token_begin != String::npos)
                tokens.push_back(line.substr(token_begin, i - token_begin));
            token_begin = String::npos;
        }
        else if (token_begin == String::npos)
            token_begin = i;
    }

    if (token_begin != String::npos)
        tokens.push_back(line.substr(token_begin));
    return tokens;
}

/// Turn an "authorized_keys"-style line ("[options] <type> <base64> [comment]") into the
/// canonical "<type> <base64>". The optional leading options field is skipped.
void collectKey(const String & line, std::unordered_set<String> & keys)
{
    String trimmed = line;
    boost::trim(trimmed);
    if (trimmed.empty() || trimmed[0] == '#')
        return;

    const auto tokens = tokenize(trimmed);
    for (size_t i = 0; i + 1 < tokens.size(); ++i)
        if (isKeyType(tokens[i]))
        {
            keys.insert(String{tokens[i]} + " " + String{tokens[i + 1]});
            return;
        }
}

}

std::unordered_set<String> ConfigRoutingTable::loadAuthorizedKeys(const String & inline_keys, const String & file)
{
    std::unordered_set<String> keys;

    std::vector<String> lines;
    if (!inline_keys.empty())
        boost::split(lines, inline_keys, boost::is_any_of("\n,"));
    for (const auto & line : lines)
        collectKey(line, keys);

    if (!file.empty())
    {
        std::ifstream stream(file);
        if (!stream)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open authorized_key_file '{}'", file);
        String line;
        while (std::getline(stream, line))
            collectKey(line, keys);
    }

    /// An allowlist that parsed to nothing must not silently degrade into "no key restriction".
    if (keys.empty() && !(inline_keys.empty() && file.empty()))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "A routing rule specifies an SSH public key allowlist, but no key could be parsed from it. "
            "Every entry must be an `authorized_keys` line: '[options] <type> <base64> [comment]'");

    return keys;
}

bool ConfigRoutingTable::Matcher::matches(const String & value, std::vector<String> & captures) const
{
    if (!specified())
        return true;

    /// A specified matcher never matches an attribute that could not be extracted.
    if (value.empty())
        return false;

    if (!values.empty())
        return std::find(values.begin(), values.end(), value) != values.end();

    size_t groups = regexp->NumberOfCapturingGroups();
    std::vector<std::string> matched(groups);
    std::vector<re2::RE2::Arg> args(groups);
    std::vector<const re2::RE2::Arg *> arg_pointers(groups);
    for (size_t i = 0; i < groups; ++i)
    {
        args[i] = re2::RE2::Arg(&matched[i]);
        arg_pointers[i] = &args[i];
    }

    if (!re2::RE2::FullMatchN(value, *regexp, arg_pointers.data(), static_cast<int>(groups)))
        return false;

    for (auto & capture : matched)
        captures.push_back(std::move(capture));
    return true;
}

ConfigRoutingTable::ConfigRoutingTable(const std::vector<RuleConfig> & rules_)
{
    for (const auto & rule_config : rules_)
    {
        Rule rule;
        rule.host = makeMatcher(rule_config.host, rule_config.host_regexp, /*case_insensitive=*/ true);
        rule.user = makeMatcher(rule_config.user, rule_config.user_regexp);
        rule.database = makeMatcher(rule_config.database, rule_config.database_regexp);
        rule.query_types = splitList(rule_config.query_type);

        for (const auto & name : rule.query_types)
            if (name != "select" && name != "insert" && name != "other")
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "Unknown query type '{}' in a routing rule. Supported types: select, insert, other", name);

        for (const auto & name : splitList(rule_config.protocol))
            rule.protocols.push_back(parseListenerProtocol(name));

        rule.authorized_keys = loadAuthorizedKeys(rule_config.authorized_key, rule_config.authorized_key_file);

        rule.target.pool_name = rule_config.pool;
        rule.target.backend = rule_config.backend_template;

        rules.push_back(std::move(rule));
    }
}

bool ConfigRoutingTable::appliesToProtocol(const Rule & rule, ListenerProtocol protocol)
{
    if (rule.protocols.empty())
        return true;
    return std::find(rule.protocols.begin(), rule.protocols.end(), protocol) != rule.protocols.end();
}

std::optional<IRoutingTable::Target> ConfigRoutingTable::resolve(const RouteAttributes & attributes) const
{
    for (const auto & rule : rules)
    {
        if (!appliesToProtocol(rule, attributes.protocol))
            continue;

        if (!rule.query_types.empty()
            && std::find(rule.query_types.begin(), rule.query_types.end(), attributes.query_type) == rule.query_types.end())
            continue;

        if (!rule.authorized_keys.empty()
            && (attributes.authorized_key.empty() || !rule.authorized_keys.contains(attributes.authorized_key)))
            continue;

        /// Captures are numbered across the host, user and database matchers, in this order.
        std::vector<String> captures;
        if (!rule.host.matches(attributes.host, captures))
            continue;
        if (!rule.user.matches(attributes.user, captures))
            continue;
        if (!rule.database.matches(attributes.database, captures))
            continue;

        Target target = rule.target;
        if (target.backend)
        {
            target.backend->name = substituteCaptures(target.backend->name, captures);
            target.backend->host = substituteCaptures(target.backend->host, captures);
        }
        return target;
    }
    return {};
}

bool ConfigRoutingTable::needsCredentials(ListenerProtocol protocol) const
{
    for (const auto & rule : rules)
        if (appliesToProtocol(rule, protocol) && (rule.user.specified() || rule.database.specified()))
            return true;
    return false;
}

bool ConfigRoutingTable::needsQueryType(ListenerProtocol protocol) const
{
    for (const auto & rule : rules)
        if (appliesToProtocol(rule, protocol) && !rule.query_types.empty())
            return true;
    return false;
}

}
