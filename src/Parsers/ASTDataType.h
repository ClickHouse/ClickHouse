#pragma once

#include <Parsers/ASTExpressionList.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// AST for data types, e.g. UInt8 or Tuple(x UInt8, y Enum(a = 1))
class ASTDataType : public IAST
{
public:
    String name;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getArguments() const;
    void resetArguments();

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

/** Materialize the `uuid_type_version` setting into a data-type or expression AST.
  *
  * When `uuid_type_version == 2`, every occurrence of the bare type name `UUID` (case-insensitive), including nested
  * ones such as `Array(UUID)` or `Nullable(UUID)`, is replaced by `UUID2`. The explicit names `UUID1` and `UUID2` are
  * never touched. For any other version value the AST is returned unchanged.
  *
  * The AST may also be an expression: type names inside a string-literal argument of the functions that take one
  * (`CAST(x, 'UUID')` - the canonical form the parser produces for `CAST(x AS UUID)` and `x::UUID` - as well as
  * `reinterpret`, `defaultValueOfTypeName`, `JSONExtract`, `variantElement` and friends) are rewritten too, so the
  * setting reaches column types inferred from `DEFAULT` expressions and `AS SELECT`, and every other persisted
  * expression that names a type.
  *
  * The input AST is not modified: a clone is returned when a substitution is performed, otherwise the original pointer.
  */
ASTPtr applyUUIDTypeVersion(const ASTPtr & type_ast, UInt64 uuid_type_version);

/** The same substitution as `applyUUIDTypeVersion`, but performed in place over a whole AST subtree.
  *
  * Returns true if anything was substituted. Mutating in place keeps the AST members that alias their own children
  * (such as `ASTColumnDeclaration::type`) consistent, which makes it suitable for normalizing an entire persisted
  * `CREATE` / `ALTER` query - including storage expressions (`ORDER BY`, `PARTITION BY`, `SAMPLE BY`, TTL), indices,
  * constraints, projections and mutation commands - in one pass.
  */
bool applyUUIDTypeVersionInPlace(IAST & ast, UInt64 uuid_type_version);

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
