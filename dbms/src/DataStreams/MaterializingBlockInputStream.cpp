#include <DataStreams/MaterializingBlockInputStream.h>
#include <Core/materializeBlock.h>


namespace DB
{

MaterializingBlockInputStream::MaterializingBlockInputStream(const BlockInputStreamPtr & input)
{
    children.push_back(input);
}

String MaterializingBlockInputStream::getName() const
{
    return "Materializing";
}

Block MaterializingBlockInputStream::getHeader() const
{
    return materializeBlock(children.back()->getHeader());
}

Block MaterializingBlockInputStream::readImpl()
{
    return materializeBlock(children.back()->read());
}

}
