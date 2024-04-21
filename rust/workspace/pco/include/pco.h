#pragma once

#include <cstdint>

extern "C" {

using PcoError = enum PcoError {
  PcoSuccess,
  PcoInvalidType,
  PcoInvalidArgument,
  PcoCompressionError,
  PcoDecompressionError,
};

PcoError pco_simple_compress(uint8_t dtype,
                             const uint8_t * src,
                             uint32_t src_len,
                             uint8_t * dst,
                             uint32_t dst_len,
                             uint8_t level,
                             uint32_t * compressed_bytes_written);

PcoError pco_simple_decompress(uint8_t dtype,
                               const uint8_t * src,
                               uint32_t src_len,
                               uint8_t * dst,
                               uint32_t dst_len);

PcoError pco_file_size(uint8_t dtype,
                       uint32_t len,
                       uint32_t * file_size);

}
