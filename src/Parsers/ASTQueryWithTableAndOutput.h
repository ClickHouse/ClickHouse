#pragma once

#include <base/defines.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Core/UUID.h>


namespace DB
{


/** Query specifying table name and, possibly, the database and the FORMAT section.
  */
class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
{
public:
    UUID uuid = UUIDHelpers::Nil;

    ASTPtr getDatabaseAst() const { return database_index != INVALID_INDEX ? children[database_index] : nullptr; }
    ASTPtr getTableAst() const { return table_index != INVALID_INDEX ? children[table_index] : nullptr; }

    void setDatabaseAst(ASTPtr node)
    {
        if (node)
        {
            if (database_index != INVALID_INDEX)
                children[database_index] = std::move(node);
            else
                database_index = addChildAndGetIndex(std::move(node));
        }
        else if (database_index != INVALID_INDEX)
        {
            /// We don't remove from children for simplicity, just invalidate the index
            database_index = INVALID_INDEX;
        }
    }

    void setTableAst(ASTPtr node)
    {
        if (node)
        {
            if (table_index != INVALID_INDEX)
                children[table_index] = std::move(node);
            else
                table_index = addChildAndGetIndex(std::move(node));
        }
        else if (table_index != INVALID_INDEX)
        {
            /// We don't remove from children for simplicity, just invalidate the index
            table_index = INVALID_INDEX;
        }
    }

    bool isTemporary() const { return FLAGS & IS_TEMPORARY; }
    void setIsTemporary(bool value) { FLAGS = value ? (FLAGS | IS_TEMPORARY) : (FLAGS & ~IS_TEMPORARY); }

    /// Returns database/table name as String (for compatibility with old API)
    String getDatabase() const;
    String getTable() const;

    // Once database or table are set they cannot be assigned with empty value
    void setDatabase(const String & name);
    void setTable(const String & name);

    void cloneTableOptions(ASTQueryWithTableAndOutput & cloned) const;

    void resetOutputAST() override;

protected:
    UInt8 database_index = INVALID_INDEX;
    UInt8 table_index = INVALID_INDEX;

    /// Bit flags for ASTQueryWithTableAndOutput (bits 0-2 used by ASTQueryWithOutput)
    static constexpr UInt32 IS_TEMPORARY = 1u << 3;

    void resetTableIndices()
    {
        database_index = INVALID_INDEX;
        table_index = INVALID_INDEX;
    }
};


template <typename AstIDAndQueryNames>
class ASTQueryWithTableAndOutputImpl : public ASTQueryWithTableAndOutput
{
public:
    String getID(char delim) const override { return AstIDAndQueryNames::ID + (delim + getDatabase()) + delim + getTable(); }

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTQueryWithTableAndOutputImpl<AstIDAndQueryNames>>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        cloneTableOptions(*res);
        return res;
    }

    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        ostr
            << (isTemporary() ? AstIDAndQueryNames::QueryTemporary : AstIDAndQueryNames::Query)
            << " ";

        if (auto database = getDatabaseAst())
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        auto table = getTableAst();
        chassert(table != nullptr, "Table is empty for the ASTQueryWithTableAndOutputImpl.");
        table->format(ostr, settings, state, frame);
    }
};

}
