#pragma once

#include <base/types.h>
#include <optional>
#include <vector>


namespace DB
{

/// Names of the settings that limit the number of generated addresses. They are only used to make the
/// error message actionable: `remote` has a dedicated setting, everything else shares the glob one.
inline constexpr auto TABLE_FUNCTION_REMOTE_MAX_ADDRESSES_SETTING = "table_function_remote_max_addresses";
inline constexpr auto GLOB_EXPANSION_MAX_ELEMENTS_SETTING = "glob_expansion_max_elements";

/// The same pattern parser serves table functions (`remote`, `url`, `urlCluster`, ...), table engines
/// (`URL`, `MySQL`, `PostgreSQL`), database engines and dictionary sources. The error messages have to
/// name the surface the user actually invoked, not the one that happens to share the parser.
struct RemoteDescriptionCaller
{
    /// How the surface is named in error messages, e.g. `Table function 'url'` or `Table engine 'URL'`.
    String description = "Table function 'remote'";
    /// The setting that raises the limit on the number of generated addresses.
    String max_addresses_setting = TABLE_FUNCTION_REMOTE_MAX_ADDRESSES_SETTING;
    /// Whether to explain that, unlike object storage, this surface cannot list the existing files.
    bool cannot_list_files = false;
};

/// A surface that expands the pattern into addresses it has to request one by one, without any listing.
inline RemoteDescriptionCaller globCaller(String description)
{
    return {std::move(description), GLOB_EXPANSION_MAX_ELEMENTS_SETTING, /*cannot_list_files=*/false};
}

/// The same, for the `url` family: HTTP provides no listing at all, which is worth explaining.
inline RemoteDescriptionCaller urlCaller(String description)
{
    return {std::move(description), GLOB_EXPANSION_MAX_ELEMENTS_SETTING, /*cannot_list_files=*/true};
}

/// `generated` is the number of addresses the pattern produces; it is not always known, because the
/// product of the already known factors can overflow before it is compared with the limit.
[[noreturn]] void throwTooManyAddresses(
    const RemoteDescriptionCaller & caller, size_t max_addresses, std::optional<size_t> generated);

/* Parse a string that generates shards and replicas. Separator - one of two characters '|' or ','
 *  depending on whether shards or replicas are generated.
 * For example:
 * host1,host2,...      - generates set of shards from host1, host2, ...
 * host1|host2|...      - generates set of replicas from host1, host2, ...
 * abc{8..10}def        - generates set of shards abc8def, abc9def, abc10def.
 * abc{08..10}def       - generates set of shards abc08def, abc09def, abc10def.
 * abc{x,yy,z}def       - generates set of shards abcxdef, abcyydef, abczdef.
 * abc{x|yy|z} def      - generates set of replicas abcxdef, abcyydef, abczdef.
 * abc{1..9}de{f,g,h}   - is a direct product, 27 shards.
 * abc{1..9}de{0|1}     - is a direct product, 9 shards, in each 2 replicas.
 *
 * Every address of the direct product is materialized, so the result is limited by `max_addresses`.
 * `caller` is only used to report which surface was invoked and which setting has to be raised when
 * the limit is hit.
 */
std::vector<String> parseRemoteDescription(
    const String & description,
    size_t l,
    size_t r,
    char separator,
    size_t max_addresses,
    const RemoteDescriptionCaller & caller = {});

/// Parse remote description for external database (MySQL or PostgreSQL).
std::vector<std::pair<String, uint16_t>> parseRemoteDescriptionForExternalDatabase(
    const String & description, size_t max_addresses, UInt16 default_port, const RemoteDescriptionCaller & caller);

}
