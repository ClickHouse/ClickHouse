#include <Common/FieldVisitorScale.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FieldVisitorScale::FieldVisitorScale(Int32 rhs_) : rhs(rhs_) {}

void FieldVisitorScale::operator() (Int64 & x) const { x *= rhs; }
void FieldVisitorScale::operator() (UInt64 & x) const { x *= rhs; }
void FieldVisitorScale::operator() (Float64 & x) const { x *= rhs; }
void FieldVisitorScale::operator() (Null &) const { /*Do not scale anything*/ }

void FieldVisitorScale::operator() (String &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Strings"); }
void FieldVisitorScale::operator() (Array &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Arrays"); }
void FieldVisitorScale::operator() (Tuple &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Tuples"); }
void FieldVisitorScale::operator() (Map &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Maps"); }
void FieldVisitorScale::operator() (Object &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Objects"); }
void FieldVisitorScale::operator() (UUID &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply UUIDs"); }
void FieldVisitorScale::operator() (IPv4 &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply IPv4s"); }
void FieldVisitorScale::operator() (IPv6 &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply IPv6s"); }
void FieldVisitorScale::operator() (CustomType & x) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply custom type {}", x.getTypeName()); }
void FieldVisitorScale::operator() (AggregateFunctionStateData &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply AggregateFunctionStates"); }
void FieldVisitorScale::operator() (bool &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot multiply Bools"); }

}
