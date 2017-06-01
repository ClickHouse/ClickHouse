#pragma once

#include <IO/WriteBuffer.h>


/// Так как HexWriteBuffer часто создаётся во внутреннем цикле, сделаем у него размер буфера маленьким.
#define DBMS_HEX_WRITE_BUFFER_SIZE 32


namespace DB
{

/** Всё что в него пишут, переводит в HEX (большими буквами) и пишет в другой WriteBuffer.
  */
class HexWriteBuffer final : public WriteBuffer
{
protected:
    char buf[DBMS_HEX_WRITE_BUFFER_SIZE];
    WriteBuffer & out;

    void nextImpl() override;

public:
    HexWriteBuffer(WriteBuffer & out_) : WriteBuffer(buf, sizeof(buf)), out(out_) {}
    ~HexWriteBuffer() override;
};

}
