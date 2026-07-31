#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/** name CHECK logical_expr
 */
class ASTConstraintDeclaration : public IAST
{
public:
    enum class Type : UInt8
    {
        CHECK,
        ASSUME,
    };

    String name;
    Type type{};
    IAST * expr{};

    String getID(char) const override { return "Constraint"; }

    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(&expr, nullptr);
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
