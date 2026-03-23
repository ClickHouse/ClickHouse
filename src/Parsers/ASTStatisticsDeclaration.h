#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

class ASTFunction;

/** name BY columns TYPE typename(args) in create query
  */
class ASTStatisticsDeclaration : public IAST
{
public:
    IAST * columns;
    IAST * types;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Stat"; }

    std::vector<String> getColumnNames() const;
    std::vector<String> getTypeNames() const;

    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
