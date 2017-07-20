#pragma once
#include <Columns/IColumn.h>
#include <Columns/ColumnVisitor.h>

namespace DB
{

template <typename Derived>
class ColumnImpl : public IColumn
{
public:
    void accept(ColumnVisitor & visitor) override { visitor.visit(*static_cast<Derived *>(this)); }
    void accept(ColumnVisitor & visitor) const override { visitor.visit(*static_cast<const Derived *>(this)); }
};

}
