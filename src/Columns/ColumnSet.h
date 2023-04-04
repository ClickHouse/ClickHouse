#pragma once

#include <Columns/IColumnDummy.h>
#include <Core/Field.h>
#include "Common/Exception.h"
#include <chrono>
#include <future>
#include <stdexcept>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;
using ConstSetPtr = std::shared_ptr<const Set>;
using FutureSet = std::shared_future<SetPtr>;


/** A column containing multiple values in the `IN` section.
  * Behaves like a constant-column (because the set is one, not its own for each line).
  * This column has a nonstandard value, so it can not be obtained via a normal interface.
  */
class ColumnSet final : public COWHelper<IColumnDummy, ColumnSet>
{
private:
    friend class COWHelper<IColumnDummy, ColumnSet>;

    ColumnSet(size_t s_, FutureSet data_) : data(std::move(data_)) { s = s_; }
    ColumnSet(const ColumnSet &) = default;

public:
    const char * getFamilyName() const override { return "Set"; }
    TypeIndex getDataType() const override { return TypeIndex::Set; }
    MutableColumnPtr cloneDummy(size_t s_) const override { return ColumnSet::create(s_, data); }

    ConstSetPtr getData() const { if (!data.valid() || data.wait_for(std::chrono::seconds(0)) != std::future_status::ready ) return nullptr; return data.get(); }

    // Used only for debugging, making it DUMPABLE
    Field operator[](size_t) const override { return {}; }

private:
    FutureSet data;
};

}
