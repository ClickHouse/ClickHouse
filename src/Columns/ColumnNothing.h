#pragma once

#include <Columns/IColumnDummy.h>


namespace DB
{

class ColumnNothing final : public COWHelper<IColumnDummy, ColumnNothing>
{
private:
    friend class COWHelper<IColumnDummy, ColumnNothing>;

    ColumnNothing(size_t s_)
    {
        s = s_;
    }

    ColumnNothing(const ColumnNothing &) = default;

public:
    const char * getFamilyName() const override { return "Nothing"; }
    MutableColumnPtr cloneDummy(size_t s_) const override { return ColumnNothing::create(s_); }
    TypeIndex getDataType() const override { return TypeIndex::Nothing; }

    bool canBeInsideNullable() const override { return true; }

    bool structureEquals(const IColumn & rhs) const override
    {
        return typeid(rhs) == typeid(ColumnNothing);
    }
};

}
