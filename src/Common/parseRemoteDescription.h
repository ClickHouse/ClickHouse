#pragma once

#include <base/types.h>

#include <cstddef>
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
    /// Set for the surfaces that cannot list the existing files (the `url` family: HTTP has no
    /// listing) to the object storage surface recommended instead, phrased for the message, e.g.
    /// `'s3' (or another object storage table function)`. The recommendation has to match the kind
    /// of the surface that was invoked: a table engine cannot be replaced by a table function, and
    /// a clustered table function needs a clustered replacement. Empty when listing is possible.
    String listing_alternative;
};

/// A surface that expands the pattern into addresses it has to request one by one, without any listing.
inline RemoteDescriptionCaller globCaller(String description)
{
    return {std::move(description), GLOB_EXPANSION_MAX_ELEMENTS_SETTING, /*listing_alternative=*/ {}};
}

/// The same, for the `url` family: HTTP provides no listing at all, which is worth explaining.
/// `listing_alternative` names the object storage surface to use instead - see the field.
inline RemoteDescriptionCaller urlCaller(String description, String listing_alternative)
{
    return {std::move(description), GLOB_EXPANSION_MAX_ELEMENTS_SETTING, std::move(listing_alternative)};
}

/// `generated` is the number of addresses the pattern produces; it is not always known, because the
/// product of the already known factors can overflow before it is compared with the limit.
[[noreturn]] void throwTooManyAddresses(
    const RemoteDescriptionCaller & caller, size_t max_addresses, std::optional<size_t> generated);

/// The same, reporting how many addresses the whole `description` generates, so that the number is
/// the cardinality of the entire first argument rather than of the part that hit the limit. When
/// `replica_separator` is set, every address generated with `separator` is expanded once more into
/// replicas separated by it, and the number covers both stages.
[[noreturn]] void throwTooManyAddressesForDescription(
    const String & description,
    char separator,
    std::optional<char> replica_separator,
    const RemoteDescriptionCaller & caller,
    size_t max_addresses);

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
 * `caller` is only used to report which surface was invoked and which setting has to be raised when
 * the limit is hit.
 */

struct RemoteDescriptionShard;

/// Generates the addresses of a pattern one by one, without materializing the direct product.
///
/// `remote` needs every address at once - it has to build a cluster out of them - and uses
/// `parseRemoteDescription` below, which drains the generator into a vector. Readers that can stop
/// early, such as the `url` table function under a `LIMIT`, iterate the generator instead, so that a
/// pattern describing a huge address space costs only as much as the query actually consumes.
///
/// Not thread safe; a shared generator has to be guarded by the caller.
class RemoteDescriptionGenerator
{
public:
    /// Parses `description[l, r)`. Throws on a malformed pattern, naming `caller` in the message.
    ///
    /// `max_addresses` bounds the number of addresses this generator is allowed to produce: `next`
    /// throws once the pattern turns out to have more of them. It also bounds the groups that cannot
    /// be generated lazily - a group with a separator inside, such as `{a,b}` in `{a,b}{c,d}`, is
    /// expanded eagerly, because the direct product has to know its alternatives up front. Such a
    /// group can only hold literal text (a `..` anywhere inside braces makes the whole group a
    /// numeric interval, which is kept symbolic), so in practice it is tiny.
    ///
    /// `replica_separator` is only used to report the number of addresses when the limit is hit: it is
    /// set when every generated address is expanded once more into replicas separated by it, as `url`
    /// does with `|`, so that the reported number covers both stages.
    RemoteDescriptionGenerator(
        const String & description,
        size_t l,
        size_t r,
        char separator,
        size_t max_addresses,
        const RemoteDescriptionCaller & caller = {},
        std::optional<char> replica_separator = {});

    /// How many addresses the pattern generates in total, ignoring `max_addresses`.
    /// `std::nullopt` when that number does not fit into `UInt64`.
    std::optional<UInt64> totalCount() const { return total_count; }

    /// Writes the next address into `out` and returns true, or returns false when the pattern is
    /// exhausted. Throws when the pattern generates more than `max_addresses` addresses.
    bool next(String & out);

    /// Whether the last address has already been generated. Lets a caller that generates addresses in
    /// portions tell "the pattern ended" from "the portion ended" without asking for one more address,
    /// which would throw once `max_addresses` of them have been generated.
    bool isExhausted() const { return finished; }

private:
    friend std::vector<RemoteDescriptionShard> parseRemoteDescriptionWithFailover(
        const String & description, size_t max_addresses, const RemoteDescriptionCaller & caller);

    /// One position of the direct product: either a set of alternatives, or a numeric interval, which
    /// is kept symbolic so that `{0..1000000000}` does not cost a billion strings.
    struct Factor
    {
        std::vector<String> alternatives;
        UInt64 range_begin = 0;
        UInt64 range_end = 0; /// Inclusive.
        size_t pad_width = 0; /// Left-pad the number with zeroes up to this width, 0 - do not pad.
        bool is_range = false;

        UInt64 size() const;
        void appendElementTo(String & out, UInt64 index) const;
    };

    /// One separator-delimited part of the description. Its addresses are the direct product of its
    /// factors, and a part without factors generates nothing at all (as in `host1,,host2`).
    struct Segment
    {
        std::vector<Factor> factors;
    };

    /// A group with the separator inside is parsed by a nested generator, which reports the number of
    /// addresses of the `outer` description, the one the user wrote, when the limit is hit.
    RemoteDescriptionGenerator(
        const String & description,
        size_t l,
        size_t r,
        char separator,
        size_t max_addresses,
        const RemoteDescriptionCaller & caller,
        std::optional<char> replica_separator,
        const RemoteDescriptionGenerator * outer);

    /// Moves to the first segment that generates anything, starting from `segment_index`.
    void startSegment();

    [[noreturn]] void throwTooManyAddresses() const;

    const size_t max_addresses;
    const RemoteDescriptionCaller caller;

    /// The description the user wrote, for the error message - see `throwTooManyAddressesForDescription`.
    String origin_description;
    char origin_separator;
    std::optional<char> origin_replica_separator;

    std::vector<Segment> segments;
    std::optional<UInt64> total_count;

    /// Position of the next address: the current segment, and the odometer over its factors. The last
    /// factor is the least significant digit, which is the order `parseRemoteDescription` produced.
    size_t segment_index = 0;
    std::vector<UInt64> digits;
    UInt64 generated = 0;
    bool finished = false;
};

std::vector<String> parseRemoteDescription(
    const String & description,
    size_t l,
    size_t r,
    char separator,
    size_t max_addresses,
    const RemoteDescriptionCaller & caller = {});

/// A shard of a `shards,separated,by,commas` description together with its `replicas|separated|by|bars`.
struct RemoteDescriptionShard
{
    /// The shard as produced by the first stage, with the replica pattern still unexpanded, e.g.
    /// `example01-1-{1|2}`. This is the form that is handed over to the workers of the cluster
    /// table functions, which expand the replicas on their own.
    String description;
    std::vector<String> replicas;
};

/// Parse a description that generates both shards and replicas: the shards are separated by `,` and
/// every shard is then expanded into its replicas separated by `|`, e.g. `example01-0{1,2}-{1|2}` is
/// two shards with two replicas each. `max_addresses` bounds the total number of generated addresses,
/// not the number generated at each of the two stages, since that is what the setting promises.
std::vector<RemoteDescriptionShard> parseRemoteDescriptionWithFailover(
    const String & description, size_t max_addresses, const RemoteDescriptionCaller & caller = {});

/// Parse remote description for external database (MySQL or PostgreSQL).
std::vector<std::pair<String, uint16_t>> parseRemoteDescriptionForExternalDatabase(
    const String & description, size_t max_addresses, UInt16 default_port, const RemoteDescriptionCaller & caller);

}
