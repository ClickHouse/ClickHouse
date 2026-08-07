#pragma once

namespace DB
{

class IAST;

/** Clear the purely cosmetic `parenthesized` flag everywhere in the tree.
  *
  * Redundant parentheses a user wrote around a definition expression (`PRIMARY KEY (col)`,
  * `PARTITION BY (a)`, `TTL (d + INTERVAL 1 DAY)`) carry no meaning, but since
  * https://github.com/ClickHouse/ClickHouse/pull/92340 the formatter preserves them.
  *
  * Stored table metadata is compared as text, and the text on the other side of the comparison
  * may have been written by a server version that did not preserve the parentheses. Clear the
  * flag before a definition is serialized or compared, so that two identical definitions do not
  * differ as strings. Parentheses required by operator precedence are re-emitted while
  * formatting and are unaffected.
  */
void stripArtificialParens(IAST & ast);

}
