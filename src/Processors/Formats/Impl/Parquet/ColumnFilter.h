#pragma once
#include <base/types.h>

namespace DB
{
class ColumnFilter;
using ColumnFilterPtr = std::shared_ptr<ColumnFilter>;

enum ColumnFilterKind
{
    Unknown,
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
    virtual ~ColumnFilter() { }
    virtual ColumnFilterKind kind() { return Unknown; }
    virtual bool testNull() { return true; }
    virtual bool testNotNull() { return true; }
    virtual bool testInt64(Int64) { return true; }
    virtual bool testFloat32(Float32) { return true; }
    virtual bool testFloat64(Float64) { return true; }
    virtual bool testBool(bool) { return true; }
    virtual bool testInt64Range(Int64, Int64) { return true; }
    virtual bool testFloat32Range(Float32, Float32) { return true; }
    virtual bool testFloat64Range(Float64, Float64) { return true; }
};

class AlwaysTrueFilter : public ColumnFilter
{
public:
    ColumnFilterKind kind() override { return AlwaysTrue; }
    bool testNull() override { return true; }
    bool testNotNull() override { return true; }
    bool testInt64(Int64) override { return true; }
    bool testFloat32(Float32) override { return true; }
    bool testFloat64(Float64) override { return true; }
    bool testBool(bool) override { return true; }
    bool testInt64Range(Int64, Int64) override { return true; }
    bool testFloat32Range(Float32, Float32) override { return true; }
    bool testFloat64Range(Float64, Float64) override { return true; }
};

class Int64RangeFilter : public ColumnFilter
{
public:
    explicit Int64RangeFilter(Int64 min_, Int64 max_) : max(max_), min(min_) { }
    ~Int64RangeFilter() override = default;
    ColumnFilterKind kind() override { return Int64Range; }
    bool testNull() override { return false; }
    bool testNotNull() override { return true; }
    bool testInt64(Int64 int64) override { return int64 >= min && int64 <= max; }
    bool testFloat32(Float32 float32) override { return ColumnFilter::testFloat32(float32); }
    bool testFloat64(Float64 float64) override { return ColumnFilter::testFloat64(float64); }
    bool testBool(bool b) override { return ColumnFilter::testBool(b); }
    bool testInt64Range(Int64 int64, Int64 int641) override { return ColumnFilter::testInt64Range(int64, int641); }
    bool testFloat32Range(Float32 float32, Float32 float321) override { return ColumnFilter::testFloat32Range(float32, float321); }
    bool testFloat64Range(Float64 float64, Float64 float641) override { return ColumnFilter::testFloat64Range(float64, float641); }

private:
    Int64 max = INT64_MAX;
    Int64 min = INT64_MIN;
};

}
