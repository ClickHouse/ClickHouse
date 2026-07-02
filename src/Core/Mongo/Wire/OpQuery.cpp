#include <Core/Mongo/Wire/OpQuery.h>
#include "IO/WriteHelpers.h"
#include "base/types.h"

#include <iostream>

namespace DB::MongoProtocol
{

void OpQuery::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(flags, in);
    readNullTerminated(full_collection_name, in);
    readBinaryLittleEndian(number_to_skip, in);
    readBinaryLittleEndian(number_to_skip, in);
    query.deserialize(in);
}

void OpQuery::serialize(WriteBuffer & out) const
{
    header.serialize(out);

    UInt64 cursor_id = 0;
    UInt32 starting_from = 0;
    UInt32 number_returned = 1;

    writeBinaryLittleEndian(flags, out);
    writeBinaryLittleEndian(cursor_id, out);
    writeBinaryLittleEndian(starting_from, out);
    writeBinaryLittleEndian(number_returned, out);

    query.serialize(out);
}

}
