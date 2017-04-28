#include <Dictionaries/DictionaryBlockInputStreamBase.h>

namespace DB
{

String DictionaryBlockInputStreamBase::getID() const
{
    std::stringstream ss;
    ss << static_cast<const void*> (this);
    return ss.str();
}

Block DictionaryBlockInputStreamBase::readImpl()
{
    if (was_read)
        return Block();

    was_read = true;
    return block;
}

}