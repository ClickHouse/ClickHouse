#include <IO/CryptographicHashingWriteBuffer.h>

namespace DB
{
    template <typename Buffer>
    void ICryptoHashingBuffer<Buffer>::calculateHash(DB::BufferBase::Position data, size_t len) {
        // todo
    }

    template class ICryptoHashingBuffer<DB::ReadBuffer>;
    template class ICryptoHashingBuffer<DB::WriteBuffer>;
}
