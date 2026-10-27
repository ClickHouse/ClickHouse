#pragma once

#include <Parsers/ASTExpressionList.h>


namespace DB
{

/// AST for data types, e.g. UInt8 or Tuple(x UInt8, y Enum(a = 1))
class ASTDataType : public IAST
{
public:
    String name;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getArguments() const;
    void resetArguments();

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

template <typename... Args>
boost::intrusive_ptr<ASTDataType> makeASTDataType(const String & name, Args &&... args)
{
    auto data_type = make_intrusive<ASTDataType>();
    data_type->name = name;

    if constexpr (sizeof...(args))
    {
        auto arguments = make_intrusive<ASTExpressionList>();
        data_type->children.push_back(arguments);
        arguments->children = {std::forward<Args>(args)...};
    }

    return data_type;
}

}
