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
    x += rhs.reinterpret<UInt64>();
    return x != 0;
}

bool FieldVisitorSum::operator() (Float64 & x) const { x += get<Float64>(rhs); return x != 0; }

bool FieldVisitorSum::operator() (Null &) const { throw Exception("Cannot sum Nulls", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (String &) const { throw Exception("Cannot sum Strings", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (Array &) const { throw Exception("Cannot sum Arrays", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (Tuple &) const { throw Exception("Cannot sum Tuples", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (Map &) const { throw Exception("Cannot sum Maps", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (Object &) const { throw Exception("Cannot sum Objects", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (UUID &) const { throw Exception("Cannot sum UUIDs", ErrorCodes::LOGICAL_ERROR); }

bool FieldVisitorSum::operator() (AggregateFunctionStateData &) const
{
    throw Exception("Cannot sum AggregateFunctionStates", ErrorCodes::LOGICAL_ERROR);
}

bool FieldVisitorSum::operator() (bool &) const { throw Exception("Cannot sum Bools", ErrorCodes::LOGICAL_ERROR); }

}

