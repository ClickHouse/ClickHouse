#pragma once

#include <Parsers/IAST.h>

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

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTAssignment>(*this);
        res->children = { expression()->clone() };
        return res;
    }

protected:
    void formatImpl(const FormattingBuffer & out) const override
    {
        out.writeIdentifier(column_name);
        out.writeOperator(" = ");
        expression()->formatImpl(out);
    }
};

}
