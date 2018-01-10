#include <Core/Block.h>
#include <DataStreams/ProhibitColumnsBlockOutputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


void ProhibitColumnsBlockOutputStream::write(const Block & block)
{
    for (const auto & column : columns)
        if (block.has(column.name))
            throw Exception{"Cannot insert column " + column.name, ErrorCodes::ILLEGAL_COLUMN};

    output->write(block);
}

}
