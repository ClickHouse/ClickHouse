#include <DB/DataStreams/MaterializingBlockOutputStream.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <ext/range.hpp>

namespace DB
{

MaterializingBlockOutputStream::MaterializingBlockOutputStream(const BlockOutputStreamPtr & output)
	: output{output}
{
}

void MaterializingBlockOutputStream::write(const Block & block)
{
	output->write(materialize(block));
}

void MaterializingBlockOutputStream::flush()
{
	output->flush();
}

void MaterializingBlockOutputStream::writePrefix()
{
	output->writePrefix();
}

void MaterializingBlockOutputStream::writeSuffix()
{
	output->writeSuffix();
}

void MaterializingBlockOutputStream::setRowsBeforeLimit(size_t rows_before_limit)
{
	output->setRowsBeforeLimit(rows_before_limit);
}

void MaterializingBlockOutputStream::setTotals(const Block & totals)
{
	output->setTotals(materialize(totals));
}

void MaterializingBlockOutputStream::setExtremes(const Block & extremes)
{
	output->setExtremes(materialize(extremes));
}

String MaterializingBlockOutputStream::getContentType() const
{
	return output->getContentType();
}

Block MaterializingBlockOutputStream::materialize(const Block & original_block)
{
	/// copy block to get rid of const
	auto block = original_block;

	for (const auto i : ext::range(0, block.columns()))
	{
		auto & element = block.getByPosition(i);
		auto & src = element.column;
		ColumnPtr converted = src->convertToFullColumnIfConst();
		if (converted)
		{
			src = converted;
			auto & type = element.type;
			if (type.get()->isNull())
			{
				/// A ColumnNull that is converted to a full column
				/// has the type DataTypeNullable(DataTypeUInt8).
				type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
			}
		}
	}

	return block;
}

}
