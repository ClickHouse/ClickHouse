#include "FillingBlockInputStream.h"

FillingBlockInputStream::FillingBlockInputStream(
        const BlockInputStreamPtr & input, const SortDescription & description_)
        : description(description_)
{
    children.push_back(input);
}


Block FillingBlockInputStream::readImpl()
{
    Block res;
    UInt64 rows = 0;



}