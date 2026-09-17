#pragma once

#include <Parsers/ASTExpressionList.h>

#include <utility>


namespace DB
{

/// A qualified user-defined type reference, for example app.UserId or ids.Raw(16).
/// The database and type components remain separate so quoted identifiers containing
/// dots cannot be confused with a built-in data-type family name.
class ASTUDTReference final : public IAST
{
public:
    String database_name;
    String type_name;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getArguments() const;
    void resetArguments();

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

template <typename... Args>
boost::intrusive_ptr<ASTUDTReference>
makeASTUDTReference(const String & database_name, const String & type_name, Args &&... args)
{
    auto reference = make_intrusive<ASTUDTReference>();
    reference->database_name = database_name;
    reference->type_name = type_name;

    if constexpr (sizeof...(args))
    {
        auto arguments = make_intrusive<ASTExpressionList>();
        reference->children.push_back(arguments);
        arguments->children = {std::forward<Args>(args)...};
    }

    return reference;
}

}
