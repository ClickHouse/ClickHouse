#pragma once
#include <base/types.h>

namespace DB
{
class ColumnFilter;
using ColumnFilterPtr = std::shared_ptr<ColumnFilter>;

enum ColumnFilterKind
{
    AlwaysTrue,
    AlwaysFalse,
    IsNull,
    IsNotNull,
    Int64Range,
    Int64MultiRange,
    Int64In,
};

class ColumnFilter
{
public:
    bool testNull();
    bool testNotNull();
    bool testInt64(Int64 value);
    bool testFloat32(Float32 value);
    bool testFloat64(Float64 value);
    bool testBool(bool value);
    bool testInt64Range(Int64 min, Int64 max);
    bool testFloat32Range(Float32 min, Float32 max);
    bool testFloat64Range(Float64 min, Float64 max);
};

}

