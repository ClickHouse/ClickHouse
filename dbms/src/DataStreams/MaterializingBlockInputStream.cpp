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

Block MaterializingBlockInputStream::getHeader()
{
    Block res = children.back()->getHeader();

    /// Constant columns become non constant.
    for (auto & elem : res)
        if (!elem.column->isColumnConst())
            elem.column = nullptr;

    return res;
}

Block MaterializingBlockInputStream::readImpl()
{
    return materializeBlock(children.back()->read());
}

}
