#pragma once

#include <Columns/IColumnDummy.h>
#include <Core/Field.h>


namespace DB
{

class Set;
using ConstSetPtr = std::shared_ptr<const Set>;


/** A column containing multiple values in the `IN` section.
  * Behaves like a constant-column (because the set is one, not its own for each line).
  * This column has a nonstandard value, so it can not be obtained via a normal interface.
  */
class ColumnSet final : public COWHelper<IColumnDummy, ColumnSet>
{
private:
    friend class COWHelper<IColumnDummy, ColumnSet>;

    ColumnSet(size_t s_, const ConstSetPtr & data_) : data(data_) { s = s_; }
    ColumnSet(const ColumnSet &) = default;

public:
    const char * getFamilyName() const override { return "Set"; }
    TypeIndex getDataType() const override { return TypeIndex::Set; }
    MutableColumnPtr cloneDummy(size_t s_) const override { return ColumnSet::create(s_, data); }

    ConstSetPtr getData() const { return data; }

    // Used only for debugging, making it DUMPABLE
    Field operator[](size_t) const override { return {}; }

private:
    ConstSetPtr data;
};

}
