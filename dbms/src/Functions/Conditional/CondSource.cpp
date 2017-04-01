#include <Functions/Conditional/CondSource.h>
#include <Functions/Conditional/CondException.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{

extern const int ILLEGAL_COLUMN;

}

namespace Conditional
{

const ColumnPtr CondSource::null_materialized_col;
const PaddedPODArray<UInt8> CondSource::empty_null_map;

CondSource::CondSource(const Block & block, const ColumnNumbers & args, size_t i)
    : materialized_col{initMaterializedCol(block, args, i)},
    data_array{initDataArray(block, args, i, materialized_col)},
    null_map{initNullMap(block, args, i)}
{
}

const ColumnPtr CondSource::initMaterializedCol(const Block & block, const ColumnNumbers & args, size_t i)
{
    const ColumnPtr & col = block.safeGetByPosition(args[i]).column;

    if (col->isNull())
    {
        const ColumnNull & null_col = static_cast<const ColumnNull &>(*col);
        return null_col.convertToFullColumn();
    }

    const IColumn * observed_col;

    if (col->isNullable())
    {
        const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*col);
        observed_col = nullable_col.getNestedColumn().get();
    }
    else
        observed_col = col.get();

    const auto * const_col = typeid_cast<const ColumnConst<UInt8> *>(observed_col);

    if (const_col != nullptr)
        return const_col->convertToFullColumn();
    else
        return null_materialized_col;
}

const PaddedPODArray<UInt8> & CondSource::initDataArray(const Block & block, const ColumnNumbers & args,
    size_t i, const ColumnPtr & materialized_col_)
{
    const IColumn * source_col;

    if (materialized_col_)
        source_col = materialized_col_.get();
    else
    {
        const ColumnPtr & col = block.safeGetByPosition(args[i]).column;
        source_col = col.get();
    }

    const IColumn * observed_col;

    if (source_col->isNullable())
    {
        const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*source_col);
        observed_col = nullable_col.getNestedColumn().get();
    }
    else
        observed_col = source_col;

    const auto * vec_col = typeid_cast<const ColumnUInt8 *>(observed_col);

    if (vec_col == nullptr)
        throw CondException{CondErrorCodes::COND_SOURCE_ILLEGAL_COLUMN,
            source_col->getName(), toString(i)};

    return vec_col->getData();
}

const PaddedPODArray<UInt8> & CondSource::initNullMap(const Block & block, const ColumnNumbers & args, size_t i)
{
    const ColumnPtr & col = block.safeGetByPosition(args[i]).column;
    if (col->isNullable())
    {
        const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*col);
        const ColumnPtr & null_map = nullable_col.getNullMapColumn();
        const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*null_map);

        return content.getData();
    }
    else
        return empty_null_map;
}

}

}
