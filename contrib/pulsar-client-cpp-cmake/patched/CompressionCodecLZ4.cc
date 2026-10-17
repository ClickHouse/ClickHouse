/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
// This is a patched copy of lib/CompressionCodecLZ4.cc: it uses the LZ4 library
// bundled with ClickHouse (contrib/lz4) instead of the copy vendored inside
// pulsar-client-cpp, so that only one LZ4 implementation is linked into the
// binary. It also uses LZ4_decompress_safe instead of the unsafe, deprecated
// LZ4_decompress_fast, which is not protected against malformed input.
#include "CompressionCodecLZ4.h"

#include <cassert>

#include <lz4.h>

namespace pulsar {

SharedBuffer CompressionCodecLZ4::encode(const SharedBuffer& raw) {
    // Get the max size of the compressed data and allocate a buffer to hold it
    int maxCompressedSize = LZ4_compressBound(raw.readableBytes());
    SharedBuffer compressed = SharedBuffer::allocate(maxCompressedSize);

    int compressedSize =
        LZ4_compress_default(raw.data(), compressed.mutableData(), raw.readableBytes(), maxCompressedSize);
    assert(compressedSize > 0);
    compressed.bytesWritten(compressedSize);

    return compressed;
}

bool CompressionCodecLZ4::decode(const SharedBuffer& encoded, uint32_t uncompressedSize,
                                 SharedBuffer& decoded) {
    SharedBuffer decompressed = SharedBuffer::allocate(uncompressedSize);

    int result = LZ4_decompress_safe(encoded.data(), decompressed.mutableData(), encoded.readableBytes(),
                                     uncompressedSize);
    if (result == static_cast<int>(uncompressedSize)) {
        decompressed.bytesWritten(uncompressedSize);
        decoded = decompressed;
        return true;
    } else {
        // Decompression failed
        return false;
    }
}
}  // namespace pulsar
