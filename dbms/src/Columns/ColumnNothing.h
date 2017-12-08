#pragma once

#include <Columns/IColumnDummy.h>


namespace DB
{

class ColumnNothing final : public IColumnDummy
{
public:
    using IColumnDummy::IColumnDummy;

    const char * getFamilyName() const override { return "Nothing"; }
    ColumnPtr cloneDummy(size_t s) const override { return std::make_shared<ColumnNothing>(s); };
};

}
