#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

class MySQLBinlogEventReadBuffer : public ReadBuffer
{
protected:
    static const size_t MAX_CHECKSUM_SIGNATURE_LENGTH = 4;

    ReadBuffer & in;
    size_t checksum_signature_length;

    size_t checksum_buff_size = 0;
    size_t checksum_buff_limit = 0;
    char checksum_buf[MAX_CHECKSUM_SIGNATURE_LENGTH];

    bool nextImpl() override;

public:
    ~MySQLBinlogEventReadBuffer() override;

    MySQLBinlogEventReadBuffer(ReadBuffer & in_, size_t checksum_signature_length_);

};


}
