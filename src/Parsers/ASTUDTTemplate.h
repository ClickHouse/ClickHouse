#pragma once

#include <Parsers/IAST.h>

#include <optional>
#include <string_view>


namespace DB
{

/// Parser-layer spelling of a user-defined type formal kind.  This deliberately
/// does not depend on the semantic UDT module: lowering maps every value
/// explicitly across that boundary.
enum class UDTParameterKind : UInt8
{
    Type = 1,
    Bool = 2,
    UInt8 = 3,
    UInt16 = 4,
    UInt32 = 5,
    UInt64 = 6,
    Int8 = 7,
    Int16 = 8,
    Int32 = 9,
    Int64 = 10,
    String = 11,
};

std::string_view getUDTParameterKindName(UDTParameterKind kind);
std::optional<UDTParameterKind> tryParseUDTParameterKind(std::string_view name);
bool isUDTValueParameterKind(UDTParameterKind kind) noexcept;
bool isUDTIntegerParameterKind(UDTParameterKind kind) noexcept;
bool isUDTUnsignedParameterKind(UDTParameterKind kind) noexcept;


/// One entry in the declaration-order ASTExpressionList owned by CREATE TYPE.
class ASTUDTParameterDeclaration final : public IAST
{
public:
    ASTUDTParameterDeclaration() = default;
    ASTUDTParameterDeclaration(String name_, UInt16 ordinal_, UDTParameterKind kind_)
        : name(std::move(name_))
        , ordinal(ordinal_)
        , kind(kind_)
    {
    }

    String name;
    UInt16 ordinal = 0;
    UDTParameterKind kind = UDTParameterKind::Type;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// Common state for the two syntactically distinct formal-reference leaves.
/// The decoded name is retained for lossless identifier formatting; the ordinal
/// and kind are the parser's unambiguous binding to the ordered declaration list.
class ASTUDTParameterReference : public IAST
{
public:
    String name;
    UInt16 ordinal = 0;
    UDTParameterKind kind = UDTParameterKind::Type;

protected:
    ASTUDTParameterReference() = default;
    ASTUDTParameterReference(String name_, UInt16 ordinal_, UDTParameterKind kind_)
        : name(std::move(name_))
        , ordinal(ordinal_)
        , kind(kind_)
    {
    }

    void updateReferenceHash(SipHash & hash_state, std::string_view domain) const;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


class ASTUDTTypeParameterReference final : public ASTUDTParameterReference
{
public:
    ASTUDTTypeParameterReference() = default;
    ASTUDTTypeParameterReference(String name_, UInt16 ordinal_)
        : ASTUDTParameterReference(std::move(name_), ordinal_, UDTParameterKind::Type)
    {
    }

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
};


class ASTUDTValueParameterReference final : public ASTUDTParameterReference
{
public:
    ASTUDTValueParameterReference() { kind = UDTParameterKind::Bool; }

    ASTUDTValueParameterReference(String name_, UInt16 ordinal_, UDTParameterKind kind_)
        : ASTUDTParameterReference(std::move(name_), ordinal_, kind_)
    {
    }

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
};


/// Compile-time predicate: an integral value formal equals zero.
class ASTUDTIsZero final : public IAST
{
public:
    ASTUDTIsZero() = default;
    explicit ASTUDTIsZero(ASTPtr parameter_reference_);

    ASTPtr parameter_reference;

    void setParameterReference(ASTPtr parameter_reference_);

    String getID(char) const override { return "UDTIsZero"; }
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override { f(nullptr, &parameter_reference); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// Compile-time value expression used by a decreasing self-reference.
class ASTUDTDecrement final : public IAST
{
public:
    ASTUDTDecrement() = default;
    ASTUDTDecrement(ASTPtr parameter_reference_, UInt64 amount_);

    ASTPtr parameter_reference;
    /// Parser construction and semantic lowering require this value to be positive.
    UInt64 amount = 1;

    void setParameterReference(ASTPtr parameter_reference_);

    String getID(char) const override { return "UDTDecrement"; }
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override { f(nullptr, &parameter_reference); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// Restricted compile-time type selection.  Its children are always ordered as
/// predicate, zero branch, nonzero branch.
class ASTUDTTypeIf final : public IAST
{
public:
    ASTUDTTypeIf() = default;
    ASTUDTTypeIf(ASTPtr predicate_, ASTPtr then_type_, ASTPtr else_type_);

    ASTPtr predicate;
    ASTPtr then_type;
    ASTPtr else_type;

    void setComponents(ASTPtr predicate_, ASTPtr then_type_, ASTPtr else_type_);

    String getID(char) const override { return "UDTTypeIf"; }
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(nullptr, &predicate);
        f(nullptr, &then_type);
        f(nullptr, &else_type);
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
