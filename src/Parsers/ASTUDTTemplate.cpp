#include <Parsers/ASTUDTTemplate.h>

#include <IO/Operators.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>

#include <algorithm>
#include <array>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

using Kind = UDTParameterKind;

constexpr std::array<std::pair<std::string_view, Kind>, 11> kind_names{{
    {"TYPE", Kind::Type},
    {"Bool", Kind::Bool},
    {"UInt8", Kind::UInt8},
    {"UInt16", Kind::UInt16},
    {"UInt32", Kind::UInt32},
    {"UInt64", Kind::UInt64},
    {"Int8", Kind::Int8},
    {"Int16", Kind::Int16},
    {"Int32", Kind::Int32},
    {"Int64", Kind::Int64},
    {"String", Kind::String},
}};

void updateDomain(SipHash & hash_state, std::string_view domain)
{
    hash_state.update(domain.size());
    hash_state.update(domain.data(), domain.size());
}

void updateString(SipHash & hash_state, const String & value)
{
    hash_state.update(value.size());
    hash_state.update(value);
}

}


std::string_view getUDTParameterKindName(UDTParameterKind kind)
{
    for (const auto & [name, candidate] : kind_names)
        if (candidate == kind)
            return name;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown user-defined type parameter kind {}", static_cast<UInt16>(kind));
}

std::optional<UDTParameterKind> tryParseUDTParameterKind(std::string_view name)
{
    for (const auto & [candidate_name, kind] : kind_names)
        if (name.size() == candidate_name.size()
            && std::equal(
                name.begin(), name.end(), candidate_name.begin(), [](char lhs, char rhs) { return equalsCaseInsensitive(lhs, rhs); }))
            return kind;
    return std::nullopt;
}

bool isUDTValueParameterKind(UDTParameterKind kind) noexcept
{
    return kind != UDTParameterKind::Type;
}

bool isUDTIntegerParameterKind(UDTParameterKind kind) noexcept
{
    return kind >= UDTParameterKind::UInt8 && kind <= UDTParameterKind::Int64;
}

bool isUDTUnsignedParameterKind(UDTParameterKind kind) noexcept
{
    return kind >= UDTParameterKind::UInt8 && kind <= UDTParameterKind::UInt64;
}


String ASTUDTParameterDeclaration::getID(char delim) const
{
    return "UDTParameterDeclaration" + (delim + name) + (delim + std::to_string(ordinal));
}

ASTPtr ASTUDTParameterDeclaration::clone() const
{
    return make_intrusive<ASTUDTParameterDeclaration>(*this);
}

void ASTUDTParameterDeclaration::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    updateDomain(hash_state, "ASTUDTParameterDeclaration");
    updateString(hash_state, name);
    hash_state.update(ordinal);
    hash_state.update(static_cast<UInt8>(kind));
}

void ASTUDTParameterDeclaration::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
    ostr << ' ' << getUDTParameterKindName(kind);
}


void ASTUDTParameterReference::updateReferenceHash(SipHash & hash_state, std::string_view domain) const
{
    updateDomain(hash_state, domain);
    updateString(hash_state, name);
    hash_state.update(ordinal);
    hash_state.update(static_cast<UInt8>(kind));
}

void ASTUDTParameterReference::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
}


String ASTUDTTypeParameterReference::getID(char delim) const
{
    return "UDTTypeParameterReference" + (delim + name);
}

ASTPtr ASTUDTTypeParameterReference::clone() const
{
    return make_intrusive<ASTUDTTypeParameterReference>(*this);
}

void ASTUDTTypeParameterReference::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    updateReferenceHash(hash_state, "ASTUDTTypeParameterReference");
}


String ASTUDTValueParameterReference::getID(char delim) const
{
    return "UDTValueParameterReference" + (delim + name);
}

ASTPtr ASTUDTValueParameterReference::clone() const
{
    return make_intrusive<ASTUDTValueParameterReference>(*this);
}

void ASTUDTValueParameterReference::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    updateReferenceHash(hash_state, "ASTUDTValueParameterReference");
}


ASTUDTIsZero::ASTUDTIsZero(ASTPtr parameter_reference_)
{
    setParameterReference(std::move(parameter_reference_));
}

void ASTUDTIsZero::setParameterReference(ASTPtr parameter_reference_)
{
    children.clear();
    parameter_reference = std::move(parameter_reference_);
    if (parameter_reference)
        children.push_back(parameter_reference);
}

ASTPtr ASTUDTIsZero::clone() const
{
    auto result = make_intrusive<ASTUDTIsZero>(*this);
    result->children.clear();
    result->parameter_reference.reset();
    if (parameter_reference)
        result->setParameterReference(parameter_reference->clone());
    return result;
}

void ASTUDTIsZero::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    updateDomain(hash_state, "ASTUDTIsZero");
    /// The parameter-reference child is hashed automatically.
}

void ASTUDTIsZero::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    chassert(parameter_reference);
    parameter_reference->format(ostr, settings, state, frame);
    ostr << " = 0";
}


ASTUDTDecrement::ASTUDTDecrement(ASTPtr parameter_reference_, UInt64 amount_)
    : amount(amount_)
{
    setParameterReference(std::move(parameter_reference_));
}

void ASTUDTDecrement::setParameterReference(ASTPtr parameter_reference_)
{
    children.clear();
    parameter_reference = std::move(parameter_reference_);
    if (parameter_reference)
        children.push_back(parameter_reference);
}

ASTPtr ASTUDTDecrement::clone() const
{
    auto result = make_intrusive<ASTUDTDecrement>(*this);
    result->children.clear();
    result->parameter_reference.reset();
    if (parameter_reference)
        result->setParameterReference(parameter_reference->clone());
    return result;
}

void ASTUDTDecrement::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    updateDomain(hash_state, "ASTUDTDecrement");
    hash_state.update(amount);
    /// The parameter-reference child is hashed automatically.
}

void ASTUDTDecrement::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    chassert(parameter_reference);
    chassert(amount > 0);
    parameter_reference->format(ostr, settings, state, frame);
    ostr << " - " << amount;
}


ASTUDTTypeIf::ASTUDTTypeIf(ASTPtr predicate_, ASTPtr then_type_, ASTPtr else_type_)
{
    setComponents(std::move(predicate_), std::move(then_type_), std::move(else_type_));
}

void ASTUDTTypeIf::setComponents(ASTPtr predicate_, ASTPtr then_type_, ASTPtr else_type_)
{
    children.clear();
    predicate = std::move(predicate_);
    then_type = std::move(then_type_);
    else_type = std::move(else_type_);

    if (predicate)
        children.push_back(predicate);
    if (then_type)
        children.push_back(then_type);
    if (else_type)
        children.push_back(else_type);
}

ASTPtr ASTUDTTypeIf::clone() const
{
    auto result = make_intrusive<ASTUDTTypeIf>(*this);
    result->children.clear();
    result->predicate.reset();
    result->then_type.reset();
    result->else_type.reset();
    result->setComponents(
        predicate ? predicate->clone() : nullptr, then_type ? then_type->clone() : nullptr, else_type ? else_type->clone() : nullptr);
    return result;
}

void ASTUDTTypeIf::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    updateDomain(hash_state, "ASTUDTTypeIf");
    /// Predicate and branches are hashed automatically in canonical order.
}

void ASTUDTTypeIf::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    chassert(predicate);
    chassert(then_type);
    chassert(else_type);

    ostr << "TYPE_IF(";
    predicate->format(ostr, settings, state, frame);
    ostr << ", ";
    then_type->format(ostr, settings, state, frame);
    ostr << ", ";
    else_type->format(ostr, settings, state, frame);
    ostr << ')';
}

}
