#include <Common/FieldVisitorMul.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


FieldVisitorMul::FieldVisitorMul(const Field & rhs_) : rhs(rhs_) {}

// We can add all ints as unsigned regardless of their actual signedness.
bool FieldVisitorMul::operator() (Int64 & x) const { return this->operator()(reinterpret_cast<UInt64 &>(x)); }
bool FieldVisitorMul::operator() (UInt64 & x) const
{
    x *= applyVisitor(FieldVisitorConvertToNumber<UInt64>(), rhs);
    return x != 0;
}

bool FieldVisitorMul::operator() (Float64 & x) const {
    x *= rhs.safeGet<Float64>();
    return x != 0;
}

bool FieldVisitorMul::operator() (Null &) const
{
    /// Do not add anything
    return false;
}

bool FieldVisitorMul::operator() (String &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Strings"); }
bool FieldVisitorMul::operator() (Array &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Arrays"); }
bool FieldVisitorMul::operator() (Tuple &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Tuples"); }
bool FieldVisitorMul::operator() (Map &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Maps"); }
bool FieldVisitorMul::operator() (Object &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Objects"); }
bool FieldVisitorMul::operator() (UUID &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply UUIDs"); }
bool FieldVisitorMul::operator() (IPv4 &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply IPv4s"); }
bool FieldVisitorMul::operator() (IPv6 &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply IPv6s"); }
bool FieldVisitorMul::operator() (CustomType & x) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply custom type {}", x.getTypeName()); }

bool FieldVisitorMul::operator() (AggregateFunctionStateData &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply AggregateFunctionStates");
}

bool FieldVisitorMul::operator() (bool &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Bools"); }

}
