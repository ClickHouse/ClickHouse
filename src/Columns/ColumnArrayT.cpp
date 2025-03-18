#include <Columns/ColumnArray.h>
#include <Columns/ColumnArrayT.h>
#include <Columns/ColumnTuple.h>

#include <DataTypes/DataTypeFactory.h>

#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

ColumnArrayT::ColumnArrayT(MutableColumnPtr && tuple_)
    : tuple(std::move(tuple_))
{
}

Field ColumnArrayT::operator[](size_t n) const
{
    Field res;
    get(n, res);
    return res;
}

MutableColumnPtr ColumnArrayT::cloneResized(size_t new_size) const
{
    MutableColumnPtr resized_tuple = tuple->cloneResized(new_size);
    return ColumnArrayT::create(std::move(resized_tuple));
}

size_t ColumnArrayT::groupCount() const
{
    const auto * tuple_column = typeid_cast<const ColumnTuple *>(tuple.get());
    return tuple_column->getColumns().size();
}

std::string ColumnArrayT::getName() const
{
    return groupCount() == 16 ? "ArrayT(BFloat16, " + toString(size()) + ")"
        : groupCount() == 32  ? "ArrayT(Float32, " + toString(size()) + ")"
                              : "ArrayT(Float64, " + toString(size()) + ")";
}

void ColumnArrayT::insert(const Field & x)
{
    if (x.getType() != Field::Types::ArrayT)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert {} into column of type ArrayT", x.getTypeName());

    const ArrayT & arrayt = x.safeGet<ArrayT>();
    auto * t = dynamic_cast<ColumnTuple *>(&getTupleColumn());

    t->addSize(1);
    for (size_t i = 0; i < arrayt.size(); ++i)
        t->getColumn(i).insert(arrayt[i]);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnArrayT::insertFrom(const IColumn & src_, size_t n)
{
    const auto * arrayt_column = typeid_cast<const ColumnArrayT *>(&src_);
    tuple->insertFrom(arrayt_column->getTupleColumn(), n);
}

void ColumnArrayT::insertManyFrom(const IColumn & src, size_t position, size_t length)
{
    const auto * arrayt_column = typeid_cast<const ColumnArrayT *>(&src);
    tuple->insertManyFrom(arrayt_column->getTupleColumn(), position, length);
}

void ColumnArrayT::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto * arrayt_column = typeid_cast<const ColumnArrayT *>(&src);
    tuple->insertRangeFrom(arrayt_column->getTupleColumn(), start, length);
}

int ColumnArrayT::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    const auto * arrayt_column = typeid_cast<const ColumnArrayT *>(&rhs);
    return tuple->compareAt(n, m, arrayt_column->getTupleColumn(), nan_direction_hint);
}
#else
void ColumnArrayT::doInsertFrom(const IColumn & src_, size_t n)
{
    const auto * arrayt_column = typeid_cast<const ColumnArrayT *>(&src_);
    tuple->doInsertFrom(arrayt_column->getTupleColumn(), n);
}

void ColumnArrayT::doInsertManyFrom(const IColumn & src, size_t position, size_t length)
{
    const auto * arrayt_column = typeid_cast<const ColumnArrayT *>(&src);
    tuple->doInsertManyFrom(arrayt_column->getTupleColumn(), position, length);
}

void ColumnArrayT::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto * arrayt_column = typeid_cast<const ColumnArrayT *>(&src);
    tuple->doInsertRangeFrom(arrayt_column->getTupleColumn(), start, length);
}

int ColumnArrayT::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    const auto * arrayt_column = typeid_cast<const ColumnArrayT *>(&rhs);
    return tuple->doCompareAt(n, m, arrayt_column->getTupleColumn(), nan_direction_hint);
}
#endif

/// TODO: the following are placeholders and need an implementation
std::pair<String, DataTypePtr> ColumnArrayT::getValueNameAndType(size_t n) const
{
    return tuple->getValueNameAndType(n);
}

void ColumnArrayT::get(size_t n, Field & res) const
{
    tuple->get(n, res);
}

ColumnPtr ColumnArrayT::replicate(const Offsets & offsets) const
{
    return tuple->replicate(offsets);
}

}
