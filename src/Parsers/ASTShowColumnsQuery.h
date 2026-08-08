#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// Query SHOW COLUMNS
class ASTShowColumnsQuery : public ASTQueryWithOutput
{
public:
    bool extended = false;
    bool full = false;
    /// Whether a `[NOT] [I]LIKE '<pattern>'` clause was present at all. A separate presence bit is
    /// required because `LIKE ''` is a valid clause with an empty pattern: without it, the query
    /// would hash and format exactly like one without the clause, so the rewrite-rule matcher's
    /// "equal tree hash means exact match" invariant would not hold for the two queries.
    bool has_like = false;
    bool not_like = false;
    bool case_insensitive_like = false;

    ASTPtr where_expression;
    ASTPtr limit_length;

    String database;
    String table;

    String like;

    String getID(char) const override { return "ShowColumns"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    /// `getID` is the constant `"ShowColumns"` and the fields that actually distinguish two
    /// `SHOW COLUMNS` queries — `extended` / `full`, the `like` pattern with its `not_like` /
    /// `case_insensitive_like` modifiers, the `database` / `table` and the `where_expression` /
    /// `limit_length` clauses — are plain members, not part of `children`. Fold them into the hash
    /// so the rewrite-rule matcher (which treats an equal tree hash as semantic equality) does not
    /// let a rule template for one `SHOW COLUMNS` over-match an unrelated one.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
