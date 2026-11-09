#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/** The list of columns in `CLEAR STATISTICS` and `MATERIALIZE STATISTICS`.
  * `types` is set only in the obsolete `ADD/MODIFY STATISTICS columns TYPE types`, which is parsed only to replay
  * the entries stored by older versions.
  */
class ASTStatisticsDeclaration : public IAST
{
public:
    IAST * columns{};
    IAST * types{};

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Stat"; }

    std::vector<String> getColumnNames() const;

    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
