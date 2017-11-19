#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/materializeBlock.h>


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

String MaterializingBlockInputStream::getID() const
{
    std::stringstream res;
    res << "Materializing(" << children.back()->getID() << ")";
    return res.str();
}

Block MaterializingBlockInputStream::readImpl()
{
    return materializeBlock(children.back()->read());
}

}
