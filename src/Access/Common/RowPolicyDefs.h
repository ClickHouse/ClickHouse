#pragma once

#include <Core/Types.h>


namespace DB
{

struct RowPolicyName
{
    String short_name;
    String database;
    String table_name;

    bool empty() const { return short_name.empty(); }
    String toString() const;
    auto toTuple() const { return std::tie(short_name, database, table_name); }
    friend bool operator ==(const RowPolicyName & left, const RowPolicyName & right) { return left.toTuple() == right.toTuple(); }
    friend bool operator !=(const RowPolicyName & left, const RowPolicyName & right) { return left.toTuple() != right.toTuple(); }
};

}
