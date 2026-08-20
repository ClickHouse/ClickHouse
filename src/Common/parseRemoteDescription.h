#pragma once

#include <base/types.h>

#include <cstddef>
#include <optional>
#include <vector>


namespace DB
{

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
 */

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
    /// Parses `description[l, r)`. Throws on a malformed pattern, naming `func_name` in the message.
    ///
    /// `max_addresses` bounds the number of addresses this generator is allowed to produce: `next`
    /// throws once the pattern turns out to have more of them. It also bounds the groups that cannot
    /// be generated lazily - a group with a separator inside, such as `{a,b}` in `{a,b}{c,d}`, is
    /// expanded eagerly, because the direct product has to know its alternatives up front. Such a
    /// group can only hold literal text (a `..` anywhere inside braces makes the whole group a
    /// numeric interval, which is kept symbolic), so in practice it is tiny.
    RemoteDescriptionGenerator(
        const String & description, size_t l, size_t r, char separator, size_t max_addresses, const String & func_name = "remote");

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

    /// Moves to the first segment that generates anything, starting from `segment_index`.
    void startSegment();

    const size_t max_addresses;
    const String func_name;

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
    const String & description, size_t l, size_t r, char separator, size_t max_addresses, const String & func_name = "remote");

/// Parse remote description for external database (MySQL or PostgreSQL).
std::vector<std::pair<String, uint16_t>> parseRemoteDescriptionForExternalDatabase(const String & description, size_t max_addresses, UInt16 default_port);

}
