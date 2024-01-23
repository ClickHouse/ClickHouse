#pragma once

#include <Columns/IColumnDummy.h>


namespace DB
{

class ColumnNothing final : public COWHelper<IColumnDummy, ColumnNothing>
{
private:
    friend class COWHelper<IColumnDummy, ColumnNothing>;

    explicit ColumnNothing(size_t s_)
    {
        s = s_;
    }

    ColumnNothing(const ColumnNothing &) = default;

public:
    const char * getFamilyName() const override { return "Nothing"; }
    MutableColumnPtr cloneDummy(size_t s_) const override { return ColumnNothing::create(s_); }
    TypeIndex getDataType() const override { return TypeIndex::Nothing; }

    Field operator[](size_t) const override { return Null{}; }

    void get(size_t, Field & field) const override { field = Null{}; }

    void insert(const Field &) override { addSize(1); }

    bool isDefaultAt(size_t) const override { return true; }

    double getRatioOfDefaultRows(double) const override { return 1.0; }

    void getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const override {}

    bool canBeInsideNullable() const override { return true; }

    bool structureEquals(const IColumn & rhs) const override
    {
        return typeid(rhs) == typeid(ColumnNothing);
    }
};

}
