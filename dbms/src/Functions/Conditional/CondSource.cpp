#include <Functions/Conditional/CondSource.h>
#include <Functions/Conditional/CondException.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{

extern const int ILLEGAL_COLUMN;

}

namespace Conditional
{

static const ColumnPtr initMaterializedCol(const Block & block, const ColumnNumbers & args, size_t i)
{
    ColumnPtr col = block.getByPosition(args[i]).column;

    if (col->isConst())
        col = col->convertToFullColumnIfConst();

    if (col->isNullable())
    {
        const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*col);
        return nullable_col.getNestedColumn();
    }
    else
        return col;
}

static const PaddedPODArray<UInt8> & initDataArray(const Block & block, const ColumnNumbers & args,
    size_t i, const ColumnPtr & materialized_col)
{
    auto vec_col = checkAndGetColumn<ColumnUInt8>(materialized_col.get());

    if (!vec_col)
        throw CondException{CondErrorCodes::COND_SOURCE_ILLEGAL_COLUMN,
            materialized_col->getName(), toString(i)};

    return vec_col->getData();
}

static const PaddedPODArray<UInt8> & initNullMap(const Block & block, const ColumnNumbers & args, size_t i)
{
    const ColumnPtr & col = block.getByPosition(args[i]).column;
    if (col->isNullable())
    {
        const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*col);
        const ColumnPtr & null_map = nullable_col.getNullMapColumn();
        const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*null_map);

        return content.getData();
    }
    else
    {
        static const PaddedPODArray<UInt8> empty_null_map;
        return empty_null_map;
    }
}


CondSource::CondSource(const Block & block, const ColumnNumbers & args, size_t i)
    : materialized_col{initMaterializedCol(block, args, i)},
    data_array{initDataArray(block, args, i, materialized_col)},
    null_map{initNullMap(block, args, i)}
{
}

}

}
