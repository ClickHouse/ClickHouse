#pragma once

#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{
class ASTFunction;
using DatabaseAndTableName = std::pair<String, String>;

class ASTSnapshotQuery : public ASTQueryWithOutput
{
public:
    enum ElementType
    {
        TABLE,
        ALL,
    };

    struct Element
    {
        ElementType type;
        String table_name;
        String database_name;
        std::set<DatabaseAndTableName> except_tables;
        std::set<String> except_databases;
    };

    Element element;
    ASTFunction * snapshot_destination = nullptr;

    String getID(char) const override;
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override;
    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(reinterpret_cast<IAST **>(&snapshot_destination), nullptr);
    }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & fs, FormatState &, FormatStateStacked) const override;
};

}
