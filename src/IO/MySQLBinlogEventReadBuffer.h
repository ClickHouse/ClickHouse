#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

class MySQLBinlogEventReadBuffer : public ReadBuffer
{
protected:
    ReadBuffer & in;
    size_t checksum_signature_length;

    size_t checksum_buff_size = 0;
    size_t checksum_buff_limit = 0;
    char * checksum_buf = nullptr;

    bool nextImpl() override;

public:
    ~MySQLBinlogEventReadBuffer() override;

    MySQLBinlogEventReadBuffer(ReadBuffer & in_, size_t checksum_signature_length_);

};


}
