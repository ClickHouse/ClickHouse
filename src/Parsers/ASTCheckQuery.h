#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>

namespace Poco::JSON { class Object; }

namespace DB
{

struct ASTCheckTableQuery : public ASTQueryWithTableAndOutput
{
    ASTPtr partition;
    String part_name;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "CheckQuery" + (delim + getDatabase()) + delim + getTable(); }

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTCheckTableQuery>(*this);
        res->children.clear();
        /// `partition` is not a child: the parser puts it into the member only. Do not leave it
        /// shared with the source.
        if (partition)
            res->partition = partition->clone();
        /// The parser adds the database/table children first and `ParserQueryWithOutput` appends
        /// the output options last; reproduce that order so the clone has the same tree hash.
        cloneTableOptions(*res);
        cloneOutputOptions(*res);
        return res;
    }

    QueryKind getQueryKind() const override { return QueryKind::Check; }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override
    {
        /// Neither `partition` nor `part_name` is a child, so without hashing them
        /// `CHECK TABLE t PARTITION 1` and `CHECK TABLE t PART 'all_1_1_0'` would both hash the same
        /// as a plain `CHECK TABLE t`.
        hash_state.update(part_name.size());
        hash_state.update(part_name);
        hash_state.update(partition != nullptr);
        if (partition)
            partition->updateTreeHash(hash_state, ignore_aliases);
        ASTQueryWithTableAndOutput::updateTreeHashImpl(hash_state, ignore_aliases);
    }

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    std::variant<std::monostate, ASTPtr, String> getPartitionOrPartitionID() const
    {
        if (partition)
            return partition;
        if (!part_name.empty())
            return part_name;
        return std::monostate{};
    }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        ostr << indent_str << "CHECK TABLE ";

        if (table)
        {
            if (database)
            {
                database->format(ostr, settings, state, frame);
                ostr << '.';
            }

            chassert(table);
            table->format(ostr, settings, state, frame);
        }

        if (partition)
        {
            ostr << indent_str << " PARTITION ";
            partition->format(ostr, settings, state, frame);
        }

        if (!part_name.empty())
        {
            ostr << indent_str << " PART "
                << quoteString(part_name);
        }
    }
};


struct ASTCheckAllTablesQuery : public ASTQueryWithOutput
{
    String getID(char /* delim */) const override { return "CheckAllQuery"; }

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTCheckAllTablesQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

    QueryKind getQueryKind() const override { return QueryKind::Check; }

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & /* state */, FormatStateStacked frame) const override
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        ostr << indent_str << "CHECK ALL TABLES";
    }
};

}
