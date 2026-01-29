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
    struct ASTQueryWithTableAndOutputFlags
    {
        using ParentFlags = ASTQueryWithOutput::ASTQueryWithOutputFlags;
        static constexpr UInt32 RESERVED_BITS = 20;

        UInt32 _parent_reserved : ParentFlags::RESERVED_BITS;
        UInt32 is_temporary : 1;
        UInt32 database_index : 8;
        UInt32 table_index : 8;
        UInt32 unused : 12;
    };

    UInt8 getDatabaseIndex() const { return flags<ASTQueryWithTableAndOutputFlags>().database_index; }
    void setDatabaseIndex(UInt8 value) { flags<ASTQueryWithTableAndOutputFlags>().database_index = value; }

    UInt8 getTableIndex() const { return flags<ASTQueryWithTableAndOutputFlags>().table_index; }
    void setTableIndex(UInt8 value) { flags<ASTQueryWithTableAndOutputFlags>().table_index = value; }

public:
    ASTQueryWithTableAndOutput()
    {
        setDatabaseIndex(INVALID_INDEX);
        setTableIndex(INVALID_INDEX);
    }

    UUID uuid = UUIDHelpers::Nil;

    ASTPtr getDatabaseAst() const { return getDatabaseIndex() != INVALID_INDEX ? children[getDatabaseIndex()] : nullptr; }
    ASTPtr getTableAst() const { return getTableIndex() != INVALID_INDEX ? children[getTableIndex()] : nullptr; }

    void setDatabaseAst(ASTPtr node)
    {
        if (node)
        {
            if (getDatabaseIndex() != INVALID_INDEX)
                children[getDatabaseIndex()] = std::move(node);
            else
                setDatabaseIndex(addChildAndGetIndex(std::move(node)));
        }
        else if (getDatabaseIndex() != INVALID_INDEX)
        {
            /// We don't remove from children for simplicity, just invalidate the index
            setDatabaseIndex(INVALID_INDEX);
        }
    }

    void setTableAst(ASTPtr node)
    {
        if (node)
        {
            if (getTableIndex() != INVALID_INDEX)
                children[getTableIndex()] = std::move(node);
            else
                setTableIndex(addChildAndGetIndex(std::move(node)));
        }
        else if (getTableIndex() != INVALID_INDEX)
        {
            /// We don't remove from children for simplicity, just invalidate the index
            setTableIndex(INVALID_INDEX);
        }
    }

    bool isTemporary() const { return flags<ASTQueryWithTableAndOutputFlags>().is_temporary; }
    void setIsTemporary(bool value) { flags<ASTQueryWithTableAndOutputFlags>().is_temporary = value; }

    /// Returns database/table name as String (for compatibility with old API)
    String getDatabase() const;
    String getTable() const;

    // Once database or table are set they cannot be assigned with empty value
    void setDatabase(const String & name);
    void setTable(const String & name);

    void cloneTableOptions(ASTQueryWithTableAndOutput & cloned) const;

    void resetOutputAST() override;

protected:
    void resetTableIndices()
    {
        setDatabaseIndex(INVALID_INDEX);
        setTableIndex(INVALID_INDEX);
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
