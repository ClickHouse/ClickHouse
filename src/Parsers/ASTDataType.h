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

/** Materialize the `uuid_type_version` setting into a data-type AST.
  *
  * When `uuid_type_version == 2`, every occurrence of the bare type name `UUID` (case-insensitive), including nested
  * ones such as `Array(UUID)` or `Nullable(UUID)`, is replaced by `UUID2`. The explicit names `UUID1` and `UUID2` are
  * never touched. For any other version value the AST is returned unchanged.
  *
  * The input AST is not modified: a clone is returned when a substitution is performed, otherwise the original pointer.
  */
ASTPtr applyUUIDTypeVersion(const ASTPtr & type_ast, UInt64 uuid_type_version);

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
