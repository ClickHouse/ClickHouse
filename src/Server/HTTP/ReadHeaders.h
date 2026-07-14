#pragma once

#include <Poco/Net/MessageHeader.h>

namespace DB
{

class ReadBuffer;

void readHeaders(
    Poco::Net::MessageHeader & headers, ReadBuffer & in, size_t max_fields_number, size_t max_name_length, size_t max_value_length);

}
