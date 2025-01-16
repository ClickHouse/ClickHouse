#include "ObjectId.h"
#include <Poco/Format.h>


namespace DB
{
namespace BSON
{


ObjectId::ObjectId()
{
    id = std::string(ID_LEN, ' ');
}


ObjectId::ObjectId(const std::string & id_) : ObjectId()
{
    poco_assert_dbg(id_.size() == 24);

    for (std::size_t i = 0; i < 12; ++i)
        id[i] = fromHex(id_.substr(2 * i, 2));
}


ObjectId::ObjectId(const ObjectId & copy)
{
    id = copy.id;
}


ObjectId::~ObjectId()
{
}


}
}
