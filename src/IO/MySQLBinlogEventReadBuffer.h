#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

class MySQLBinlogEventReadBuffer : public ReadBuffer
{
protected:
    static const size_t CHECKSUM_CRC32_SIGNATURE_LENGTH = 4;
    ReadBuffer & in;

    size_t checksum_buff_size = 0;
    size_t checksum_buff_limit = 0;
    char checksum_buf[CHECKSUM_CRC32_SIGNATURE_LENGTH];

    bool nextImpl() override;

public:
    ~MySQLBinlogEventReadBuffer() override;

    MySQLBinlogEventReadBuffer(ReadBuffer & in_);

};


}
