#include <IO/StdIStreamFromMemory.h>

namespace DB
{

StdIStreamFromMemory::MemoryBuf::MemoryBuf(char * begin_, size_t size_)
    : begin(begin_)
    , size(size_)
{
    this->setg(begin, begin, begin + size);
}

StdIStreamFromMemory::MemoryBuf::int_type StdIStreamFromMemory::MemoryBuf::underflow()
{
    if (gptr() < egptr())
            return traits_type::to_int_type(*gptr());
    return traits_type::eof();
}

StdIStreamFromMemory::MemoryBuf::pos_type
StdIStreamFromMemory::MemoryBuf::seekoff(off_type off, std::ios_base::seekdir way,
                 std::ios_base::openmode mode)
{
    bool out_mode = (std::ios_base::out & mode) != 0;
    if (out_mode)
        return off_type(-1);

    off_type ret(-1);

    if (way == std::ios_base::beg)
        ret = 0;
    else if (way == std::ios_base::cur)
        ret = gptr() - begin;
    else if (way == std::ios_base::end)
        ret = size;

    if (ret == off_type(-1))
        return ret;

    ret += off;
    if (!(ret >= 0 && size_t(ret) <= size))
        return off_type(-1);

    this->setg(begin, begin + ret, begin + size);

    return pos_type(ret);
}

StdIStreamFromMemory::MemoryBuf::pos_type StdIStreamFromMemory::MemoryBuf::seekpos(pos_type sp,
                 std::ios_base::openmode mode)
{
    return seekoff(off_type(sp), std::ios_base::beg, mode);
}

StdIStreamFromMemory::StdIStreamFromMemory(char * begin_, size_t size_)
    : std::iostream(nullptr)
    , mem_buf(begin_, size_)
{
    init(&mem_buf);
}

}
