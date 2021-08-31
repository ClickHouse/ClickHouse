#pragma once

#include <Poco/Net/MessageHeader.h>

namespace DB
{

class ReadBuffer;

void readHeaders(
    Poco::Net::MessageHeader & headers,
    ReadBuffer & in,
    size_t max_fields_number = 100,
    size_t max_name_length = 256,
    size_t max_value_length = 8192);

}
