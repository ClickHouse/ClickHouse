#include <IO/BufferSourceReader.h>

namespace DB
{

BufferSourceReader::BufferSourceReader(BufferFactory factory_, String name_)
    : factory(std::move(factory_))
    , source_name(std::move(name_))
{
}

std::unique_ptr<ReadBufferFromFileBase> BufferSourceReader::open(const StoredObject & object)
{
    /// The factory is expected to honor external-buffer mode where its
    /// underlying API supports it; the executor drives reads via set()+next().
    return factory(object);
}

}
