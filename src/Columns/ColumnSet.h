#pragma once

#include <Interpreters/PreparedSets.h>
#include <Columns/IColumnDummy.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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

    ColumnSet(size_t s_, FutureSetPtr data_) : data(std::move(data_)) { s = s_; }
    ColumnSet(const ColumnSet &) = default;

public:
    const char * getFamilyName() const override { return "Set"; }
    TypeIndex getDataType() const override { return TypeIndex::Set; }
    MutableColumnPtr cloneDummy(size_t s_) const override { return ColumnSet::create(s_, data); }

    /// `insertFrom` and `insertRangeFrom` only grow the size and keep the destination `FutureSet`, so only columns
    /// holding the same set instance can be concatenated.
    bool structureEquals(const IColumn & rhs) const override
    {
        const auto * rhs_set = typeid_cast<const ColumnSet *>(&rhs);
        if (!rhs_set)
            return false;
        return data.get() == rhs_set->data.get();
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src, size_t /*n*/) override
#else
    void doInsertFrom(const IColumn & src, size_t /*n*/) override
#endif
    {
        checkSameSet(src);
        ++s;
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn & src, size_t /*start*/, size_t length) override
#else
    void doInsertRangeFrom(const IColumn & src, size_t /*start*/, size_t length) override
#endif
    {
        checkSameSet(src);
        s += length;
    }

    FutureSetPtr getData() const { return data; }
    void setData(FutureSetPtr data_) { data = std::move(data_); }

    // Used only for debugging, making it DUMPABLE
    Field operator[](size_t) const override { return {}; }

private:
    FutureSetPtr data;

    /// The appended rows would otherwise silently be checked against the destination set instead of their own.
    void checkSameSet(const IColumn & src) const
    {
        if (!structureEquals(src))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert into {} from {} holding a different set", getName(), src.getName());
    }
};

}
