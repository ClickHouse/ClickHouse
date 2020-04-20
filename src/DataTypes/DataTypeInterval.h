#pragma once

#include <DataTypes/DataTypeNumberBase.h>
#include <Common/IntervalKind.h>


namespace DB
{

/** Data type to deal with INTERVAL in SQL (arithmetic on time intervals).
  *
  * Mostly the same as Int64.
  * But also tagged with interval kind.
  *
  * Intended usage is for temporary elements in expressions,
  *  not for storing values in tables.
  */
class DataTypeInterval final : public DataTypeNumberBase<Int64>
{
private:
    IntervalKind kind;

public:
    static constexpr bool is_parametric = true;

    IntervalKind getKind() const { return kind; }

    DataTypeInterval(IntervalKind kind_) : kind(kind_) {}

    std::string doGetName() const override { return std::string("Interval") + kind.toString(); }
    const char * getFamilyName() const override { return "Interval"; }
    TypeIndex getTypeId() const override { return TypeIndex::Interval; }

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool cannotBeStoredInTables() const override { return true; }
    bool isCategorial() const override { return false; }
};

}

