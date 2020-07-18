#include <DataStreams/AddingVersionsBlockOutputStream.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

void AddingVersionsBlockOutputStream::writePrefix()
{
    output->writePrefix();
}

void AddingVersionsBlockOutputStream::writeSuffix()
{
    output->writeSuffix();
}

void AddingVersionsBlockOutputStream::flush()
{
    output->flush();
}

void AddingVersionsBlockOutputStream::write(const Block & block)
{
    Block res;
    size_t rows = block.rows();

    for (size_t index = 0; index < block.columns(); ++index)
        res.insert(block.getByPosition(index));

    DataTypePtr sign_type = std::make_shared<DataTypeInt8>();
    DataTypePtr version_type = std::make_shared<DataTypeUInt64>();

    ColumnPtr sign_column = sign_type->createColumnConst(rows, Field(Int8(1)))->convertToFullColumnIfConst();
    ColumnPtr version_column = version_type->createColumnConst(rows, Field(UInt64(++version)))->convertToFullColumnIfConst();

    Block header = output->getHeader();
    res.insert({sign_column, sign_type, header.getByPosition(header.columns() - 2).name});
    res.insert({version_column, version_type, header.getByPosition(header.columns() - 1).name});
    output->write(res);

    written_rows += block.rows();
    written_bytes += block.bytes();
}
Block AddingVersionsBlockOutputStream::getHeader() const
{
    Block res;
    Block header = output->getHeader();

    for (size_t index = 0; index < header.columns() - 2; ++index)
        res.insert(header.getByPosition(index));

    return res;
}

}
