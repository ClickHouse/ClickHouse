#include <DB/Functions/Conditional/CondSource.h>
#include <DB/Functions/Conditional/CondException.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnConst.h>

namespace DB
{

namespace ErrorCodes
{

extern const int ILLEGAL_COLUMN;

}

namespace Conditional
{

const ColumnPtr CondSource::null_materialized_col;

CondSource::CondSource(const Block & block, const ColumnNumbers & args, size_t i)
	: materialized_col{initMaterializedCol(block, args, i)},
	data_array{initDataArray(block, args, i, materialized_col)}
{
}

const ColumnPtr CondSource::initMaterializedCol(const Block & block, const ColumnNumbers & args, size_t i)
{
	const ColumnPtr & col = block.getByPosition(args[i]).column;
	const auto * const_col = typeid_cast<const ColumnConst<UInt8> *>(&*col);

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
		const ColumnPtr & col = block.getByPosition(args[i]).column;
		source_col = col.get();
	}

	const auto * vec_col = typeid_cast<const ColumnUInt8 *>(source_col);

	if (vec_col == nullptr)
		throw CondException{CondErrorCodes::COND_SOURCE_ILLEGAL_COLUMN,
			source_col->getName(), toString(i)};

	return vec_col->getData();
}

}

}
