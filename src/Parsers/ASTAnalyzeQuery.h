#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>


namespace DB
{

class ASTAnalyzeQuery : public IAST/// , public ASTQueryWithOnCluster
{
public:
    bool is_full;
    bool is_async;
    ASTPtr database;
    ASTPtr table;
    ASTPtr column_list;
    ASTPtr settings;
    String cluster;

    String getID(char) const override { return "Analyze"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&database));
        f(reinterpret_cast<void **>(&table));
        f(reinterpret_cast<void **>(&column_list));
        f(reinterpret_cast<void **>(&settings));
    }
};


}
