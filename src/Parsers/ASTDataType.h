#pragma once

#include <Parsers/ASTExpressionList.h>


namespace DB
{

/// AST for data types, e.g. UInt8 or Tuple(x UInt8, y Enum(a = 1))
class ASTDataType : public IAST
{
public:
    String name;
    ASTPtr arguments;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

template <typename... Args>
std::shared_ptr<ASTDataType> makeASTDataType(const String & name, Args &&... args)
{
    auto function = std::make_shared<ASTDataType>();

    function->name = name;
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);

    function->arguments->children = { std::forward<Args>(args)... };

    return function;
}

}
