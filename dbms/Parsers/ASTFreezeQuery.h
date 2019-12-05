#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>

namespace DB
{
/** FREEZE query
  */
class ASTFreezeQuery : public ASTQueryWithTableAndOutput
{
public:
    /// The partition to be frozen can be specified.
    ASTPtr partition;

    String with_name;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "FreezeQuery" + (delim + database) + delim + table; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTFreezeQuery>(*this);
        res->children.clear();

        if (partition)
        {
            res->partition = partition->clone();
            res->children.push_back(res->partition);
        }

        return res;
    }

    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
