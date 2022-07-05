#pragma once

#include <IO/WriteBufferFromFile.h>

namespace DB
{

/** TeeWriteBuffer extends from WriteBufferFromFile and has
 * WriteBufferFromFileDescriptor inside the class which is created
 * by using the same buffer as TeeWriteBuffer. So both the data are written
 * using same buffer
 **/
class TeeWriteBuffer : public WriteBufferFromFile
{

public:
    explicit TeeWriteBuffer(
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        mode_t mode = 0666,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~TeeWriteBuffer() override;

protected:
    void nextImpl() override;
    void finalizeImpl() override;

    WriteBufferFromFileDescriptor stdout_buffer;
};

}
