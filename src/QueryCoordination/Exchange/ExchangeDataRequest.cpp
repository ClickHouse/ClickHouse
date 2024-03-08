#include <QueryCoordination/Exchange/ExchangeDataRequest.h>

namespace DB
{

void ExchangeDataRequest::write(WriteBuffer & out) const
{
    writeStringBinary(from_host, out);
    writeStringBinary(query_id, out);
    writeVarUInt(fragment_id, out);
    writeVarUInt(exchange_id, out);
}

void ExchangeDataRequest::read(ReadBuffer & in)
{
    readStringBinary(from_host, in);
    readStringBinary(query_id, in);
    readVarUInt(fragment_id, in);
    readVarUInt(exchange_id, in);
}

String ExchangeDataRequest::toString() const
{
    return query_id + ", from " + from_host + ", destination fragment " + std::to_string(fragment_id) + ", destination exchange "
        + std::to_string(exchange_id);
}

}
