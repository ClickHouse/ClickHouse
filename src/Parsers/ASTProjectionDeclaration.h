#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

class ASTExpressionList;
class ASTFunction;
class ASTSetQuery;

class ASTProjectionDeclaration : public IAST
{
public:
    String name;
    IAST * query = nullptr;
    IAST * index = nullptr;
    ASTFunction * type = nullptr;
    ASTSetQuery * with_settings = nullptr;

    /// Optional `ASTColumnDeclaration` list, holding only the columns whose codec is overridden.
    /// Mutually exclusive with `index`.
    ASTExpressionList * columns = nullptr;

    String getID(char) const override { return "Projection"; }

    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(&query, nullptr);
        f(&index, nullptr);
        f(reinterpret_cast<IAST **>(&type), nullptr);
        f(reinterpret_cast<IAST **>(&with_settings), nullptr);
        f(reinterpret_cast<IAST **>(&columns), nullptr);
    }

    /// everything after the name, so a statement that prints the name itself can reuse it
    void formatBody(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
