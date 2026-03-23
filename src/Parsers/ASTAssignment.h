#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// Part of the ALTER UPDATE statement of the form: column = expr
class ASTAssignment : public IAST
{
public:
    String column_name;

    ASTPtr expression() const
    {
        return children.at(0);
    }

    String getID(char delim) const override { return "Assignment" + (delim + column_name); }

    void writeJSON(WriteBuffer & out) const override
    {
        JSONObjectWriter w(out, "Assignment");
        w.writeString("column_name", column_name);
        w.writeChildren(children);
    }

    void readJSON(const Poco::JSON::Object & json) override
    {
        JSONObjectReader r(json);
        column_name = r.getString("column_name");
        children = r.readChildren();
    }

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTAssignment>(*this);
        res->children = { expression()->clone() };
        return res;
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.writeIdentifier(ostr, column_name, /*ambiguous=*/false);
        ostr << " = ";

        if (auto ast = boost::dynamic_pointer_cast<ASTWithAlias>(expression()); ast && !ast->alias.empty())
        {
            frame.need_parens = true;
        }

        expression()->format(ostr, settings, state, frame);
    }
};

}
