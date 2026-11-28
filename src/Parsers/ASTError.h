#pragma once

#include <Parsers/IAST.h>

#include <string>
#include <vector>


namespace DB
{

/** The slot of a `partial_ast` (see `Parsers/PartialASTCapture.h`) that the parser failed in.
  *
  * Serialize-only: it has no SQL formatting, and it is deliberately not registered with
  * `IAST::createFromJSON`, so `ch_format_json` rejects a document that contains one.
  */
class ASTError : public IAST
{
public:
    /// Where the parser stopped: `max_parsed_pos`, and the end of the token that starts there.
    /// `ch_parse` turns them into byte offsets with `setOffsets` once the query start is known.
    const char * begin_pos = nullptr;
    const char * end_pos = nullptr;

    UInt64 begin_offset = 0;
    UInt64 end_offset = 0;

    /// What the parser would have accepted at `begin_pos`.
    std::vector<std::string> expected;

    String getID(char) const override { return "Error"; }
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;

    void setOffsets(const char * query_begin);
};

/** A captured `INSERT`, `CREATE` or `ALTER`, or a captured part of one (a storage definition, an
  * `ALTER` command), with the failed slot kept beside the node rather than inside it.
  *
  * The slot that failed is often a typed member (`ASTCreateQuery::columns_list` is an `ASTColumns`)
  * or a string (`ASTInsertQuery::format`), so it cannot hold an `Error`, and these nodes do not
  * serialize `children`. `writeJSON` therefore writes the node and adds `"<failed_key>": <failed_value>`
  * to its object, which is how a consumer sees it: as the value of the key that failed. `failed_value`
  * is the `Error`, or the captured part that failed in that slot, which holds the `Error` in turn. Only
  * keys that the node leaves out when the slot is empty are accepted, so the key never appears twice.
  */
class ASTPartialNode : public IAST
{
public:
    ASTPtr node;
    ASTPtr failed_value;
    /// The JSON key of the failed slot in `node`; statically allocated.
    const char * failed_key = nullptr;

    String getID(char) const override { return "PartialNode"; }
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
};

}
