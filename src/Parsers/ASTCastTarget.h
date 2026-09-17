#pragma once

#include <Parsers/IAST.h>

#include <utility>


namespace DB
{

/// Marks the second argument of a CAST whose target must remain a structured
/// type expression until analysis. It is deliberately not an expression node.
class ASTCastTarget final : public IAST
{
public:
    explicit ASTCastTarget(ASTPtr type_)
        : type(std::move(type_))
    {
        children.push_back(type);
    }

    const ASTPtr & getType() const { return type; }

    String getID(char) const override { return "CastTarget"; }
    ASTPtr clone() const override;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override { f(nullptr, &type); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    ASTPtr type;
};

}
