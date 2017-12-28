#pragma once

#include <Columns/IColumnDummy.h>


namespace DB
{

class ColumnNothing final : public COWPtrHelper<IColumnDummy, ColumnNothing>
{
private:
    friend class COWPtrHelper<IColumnDummy, ColumnNothing>;

    ColumnNothing(size_t s_)
    {
        s = s_;
    }

    ColumnNothing(const ColumnNothing &) = default;

public:
    const char * getFamilyName() const override { return "Nothing"; }
    MutableColumnPtr cloneDummy(size_t s) const override { return ColumnNothing::create(s); };

    bool canBeInsideNullable() const override { return true; }
};

}
