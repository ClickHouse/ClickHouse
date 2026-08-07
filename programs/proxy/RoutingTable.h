#pragma once

#include <BackendPool.h>

#include <unordered_set>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wms-bitfield-padding"
#include <re2/re2.h>
#pragma clang diagnostic pop

namespace DB::Proxy
{

/// The routing table maps connection attributes to a target: a pool name or a concrete backend address.
/// It is abstract so that other sources of routing information can be plugged in
/// (e.g. a table synchronized from an external service).
class IRoutingTable
{
public:
    virtual ~IRoutingTable() = default;

    struct Target
    {
        String pool_name;                       /// A reference to a configured pool, or
        std::optional<BackendConfig> backend;   /// a concrete backend produced by the rule.
    };

    /// Returns the target of the first matching rule, or nothing if no rule matches.
    virtual std::optional<Target> resolve(const RouteAttributes & attributes) const = 0;

    /// Whether any rule applicable to this protocol needs the user name or the database
    /// (and therefore the proxy has to parse the first packets of the protocol).
    virtual bool needsCredentials(ListenerProtocol protocol) const = 0;

    /// Whether any rule applicable to this protocol needs the query type
    /// (and therefore the proxy has to peek into the first query).
    virtual bool needsQueryType(ListenerProtocol protocol) const = 0;
};

/// A routing table defined by the <rules> section of the configuration.
class ConfigRoutingTable : public IRoutingTable
{
public:
    explicit ConfigRoutingTable(const std::vector<RuleConfig> & rules_);

    std::optional<Target> resolve(const RouteAttributes & attributes) const override;
    bool needsCredentials(ListenerProtocol protocol) const override;
    bool needsQueryType(ListenerProtocol protocol) const override;

private:
    struct Matcher
    {
        std::vector<String> values;             /// Exact values; match if the attribute equals any of them.
        std::shared_ptr<re2::RE2> regexp;       /// Alternatively, a regexp the whole attribute must match.

        bool specified() const { return !values.empty() || regexp; }

        /// On a regexp match, the values of the capture groups are appended to captures.
        bool matches(const String & value, std::vector<String> & captures) const;
    };

    struct Rule
    {
        Matcher host;
        Matcher user;
        Matcher database;
        std::vector<String> query_types;
        std::vector<ListenerProtocol> protocols;
        std::unordered_set<String> authorized_keys;   /// Canonical "<type> <base64>" keys; empty means unspecified.
        Target target;
    };

    std::vector<Rule> rules;

    /// DNS hostnames are case-insensitive, so the host matcher lowercases its exact values
    /// and compiles its regexp as case-insensitive; the attribute itself is lowercased
    /// by the frontends at extraction.
    static Matcher makeMatcher(const String & exact, const String & regexp, bool case_insensitive = false);
    static std::unordered_set<String> loadAuthorizedKeys(const String & inline_keys, const String & file);
    static bool appliesToProtocol(const Rule & rule, ListenerProtocol protocol);
};

/// Replace $1..$9 with the corresponding captures.
String substituteCaptures(const String & pattern, const std::vector<String> & captures);

}
