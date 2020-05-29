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
    /// create_version and delete_version are always in the last place
    Block res;
    size_t rows = block.rows();

    for (size_t index = 0; index < block.columns(); ++index)
        res.insert(block.getByPosition(index));

    DataTypePtr data_type = std::make_shared<DataTypeUInt64>();

    auto create_version = ColumnUInt64::create(rows);
    for (size_t index = 0; index < rows; ++index)
        create_version->getData()[index] = ++version;

    Block header = output->getHeader();
    res.insert(ColumnWithTypeAndName(create_version->getPtr(), data_type, header.getByPosition(header.columns() - 2).name));
    res.insert(ColumnWithTypeAndName(data_type->createColumnConstWithDefaultValue(rows)->convertToFullColumnIfConst(), data_type, header.getByPosition(header.columns() - 1).name));
    output->write(res);
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
