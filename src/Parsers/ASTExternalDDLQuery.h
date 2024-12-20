#pragma once

#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>


namespace DB
{

class ASTExternalDDLQuery : public IAST
{
public:
    ASTFunction * from;
    ASTPtr external_ddl;

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTExternalDDLQuery>(*this);
        res->children.clear();

        if (from)
            res->set(res->from, from->clone());

        if (external_ddl)
        {
            res->external_ddl = external_ddl->clone();
            res->children.emplace_back(res->external_ddl);
        }

        return res;
    }

    String getID(char) const override { return "external ddl query"; }

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked stacked) const override
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "EXTERNAL DDL FROM " << (settings.hilite ? hilite_none : "");
        from->formatImpl(ostr, settings, state, stacked);
        external_ddl->formatImpl(ostr, settings, state, stacked);
    }

    QueryKind getQueryKind() const override { return QueryKind::ExternalDDL; }

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&from));
    }
};

}
