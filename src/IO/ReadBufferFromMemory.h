#pragma once

#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

/// Helper class that implements seek and getPosition method from SeekableReadBuffer.
template <typename Derived>
class ReadBufferFromMemoryHelper
{
public:
    off_t seekImpl(off_t offset, int whence);

    off_t getPositionImpl();
};

/** Allows to read from memory range.
  * In comparison with just ReadBuffer, it only adds convenient constructors, that do const_cast.
  * In fact, ReadBuffer will not modify data in buffer, but it requires non-const pointer.
  */
class ReadBufferFromMemory : public SeekableReadBuffer, private ReadBufferFromMemoryHelper<ReadBufferFromMemory>
{
public:
    template <typename CharT>
    requires (sizeof(CharT) == 1)
    ReadBufferFromMemory(const CharT * buf, size_t size)
        : SeekableReadBuffer(const_cast<char *>(reinterpret_cast<const char *>(buf)), size, 0) {}
    explicit ReadBufferFromMemory(const std::string_view&& str)
        : SeekableReadBuffer(const_cast<char *>(str.data()), str.size(), 0) {}

    off_t seek(off_t off, int whence) override
    {
        return seekImpl(off, whence);
    }

    off_t getPosition() override
    {
        return getPositionImpl();
    }

private:
    friend class ReadBufferFromMemoryHelper<ReadBufferFromMemory>;
};

class ReadBufferFromMemoryFileBase : public ReadBufferFromFileBase, private ReadBufferFromMemoryHelper<ReadBufferFromMemoryFileBase>
{
protected:
    ReadBufferFromMemoryFileBase(bool owns_memory,
        String file_name_,
        std::string_view data);

    String getFileName() const override
    {
        return file_name;
    }

    off_t seek(off_t off, int whence) override
    {
        return seekImpl(off, whence);
    }

    off_t getPosition() override
    {
        return getPositionImpl();
    }

private:
    friend class ReadBufferFromMemoryHelper<ReadBufferFromMemoryFileBase>;

    const String file_name;
};

/// Prevent implicit template instantiation of ReadBufferFromMemoryHelper for common types

extern template class ReadBufferFromMemoryHelper<ReadBufferFromMemory>;
extern template class ReadBufferFromMemoryHelper<ReadBufferFromMemoryFileBase>;

/** Buffer to read from memory as from file.
  * Memory is copied and owned by this class.
  */
class ReadBufferFromOwnMemoryFile final : public ReadBufferFromMemoryFileBase
{
public:
    using Base = ReadBufferFromMemoryFileBase;

    ReadBufferFromOwnMemoryFile(String file_name_, std::string_view data)
        : Base(true /*owns_memory*/, std::move(file_name_), data)
    {
    }
};

/** Buffer to read from memory as from file.
  * Memory is not owned by this class.
  */
class ReadBufferFromOutsideMemoryFile final : public ReadBufferFromMemoryFileBase
{
public:
    using Base = ReadBufferFromMemoryFileBase;

    ReadBufferFromOutsideMemoryFile(String file_name_, std::string_view data)
        : Base(false /*owns_memory*/, std::move(file_name_), data)
    {
    }
};

}
