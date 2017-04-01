#include <Core/Block.h>
#include <DataStreams/ProhibitColumnsBlockOutputStream.h>


namespace DB
{

void ProhibitColumnsBlockOutputStream::write(const Block & block)
{
    for (const auto & column : columns)
        if (block.has(column.name))
            throw Exception{"Cannot insert column " + column.name, ErrorCodes::ILLEGAL_COLUMN};

    output->write(block);
}

}
