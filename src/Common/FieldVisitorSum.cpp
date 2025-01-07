#include <Common/FieldVisitorSum.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


FieldVisitorSum::FieldVisitorSum(const Field & rhs_) : rhs(rhs_) {}

// We can add all ints as unsigned regardless of their actual signedness.
bool FieldVisitorSum::operator() (Int64 & x) const { return this->operator()(reinterpret_cast<UInt64 &>(x)); }
bool FieldVisitorSum::operator() (UInt64 & x) const
{
    x += applyVisitor(FieldVisitorConvertToNumber<UInt64>(), rhs);
    return x != 0;
}

bool FieldVisitorSum::operator() (Float64 & x) const { x += rhs.safeGet<Float64>(); return x != 0; }

bool FieldVisitorSum::operator() (Null &) const
{
    /// Do not add anything
    return false;
}

bool FieldVisitorSum::operator() (String &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum Strings"); }
bool FieldVisitorSum::operator() (Array &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum Arrays"); }
bool FieldVisitorSum::operator() (Tuple &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum Tuples"); }
bool FieldVisitorSum::operator() (Map &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum Maps"); }
bool FieldVisitorSum::operator() (Object &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum Objects"); }
bool FieldVisitorSum::operator() (UUID &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum UUIDs"); }
bool FieldVisitorSum::operator() (IPv4 &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum IPv4s"); }
bool FieldVisitorSum::operator() (IPv6 &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum IPv6s"); }
bool FieldVisitorSum::operator() (CustomType & x) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum custom type {}", x.getTypeName()); }

bool FieldVisitorSum::operator() (AggregateFunctionStateData &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum AggregateFunctionStates");
}

bool FieldVisitorSum::operator() (bool &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum Bools"); }

}
