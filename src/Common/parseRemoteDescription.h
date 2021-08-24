#pragma once
#include <common/types.h>
#include <vector>
namespace DB
{
/* Parse a string that generates shards and replicas. Separator - one of two characters | or ,
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
std::vector<String> parseRemoteDescription(const String & description, size_t l, size_t r, char separator, size_t max_addresses);

/// Parse remote description for external database (MySQL or PostgreSQL).
std::vector<std::pair<String, uint16_t>> parseRemoteDescriptionForExternalDatabase(const String & description, size_t max_addresses, UInt16 default_port);

}
