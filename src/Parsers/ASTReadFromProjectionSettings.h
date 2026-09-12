#pragma once

#include <Parsers/IAST.h>

namespace DB
{

struct ASTReadFromProjectionSettings : public IAST
{
    ASTPtr name;

    String getID(char) const override { return "ReadFromProjectionSettings"; }
    ASTPtr clone() const override;

    void setName(ASTPtr name_);

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;
};

}
