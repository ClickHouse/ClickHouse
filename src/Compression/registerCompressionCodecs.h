#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecFactory;

void registerCodecNone(CompressionCodecFactory & factory);
void registerCodecLZ4(CompressionCodecFactory & factory);
void registerCodecLZ4HC(CompressionCodecFactory & factory);
void registerCodecZSTD(CompressionCodecFactory & factory);
void registerCodecMultiple(CompressionCodecFactory & factory);
void registerCodecDelta(CompressionCodecFactory & factory);
void registerCodecT64(CompressionCodecFactory & factory);
void registerCodecDoubleDelta(CompressionCodecFactory & factory);
void registerCodecGorilla(CompressionCodecFactory & factory);
void registerCodecEncrypted(CompressionCodecFactory & factory);
void registerCodecFPC(CompressionCodecFactory & factory);
void registerCodecGCD(CompressionCodecFactory & factory);
void registerCodecALP(CompressionCodecFactory & factory);
void registerCodecByteStreamSplit(CompressionCodecFactory & factory);

CompressionCodecPtr getCompressionCodecLZ4(int level);
CompressionCodecPtr getCompressionCodecZSTD(int level);
CompressionCodecPtr getCompressionCodecDelta(UInt8 delta_bytes_size);
CompressionCodecPtr getCompressionCodecDoubleDelta(UInt8 data_bytes_size);
CompressionCodecPtr getCompressionCodecGCD(UInt8 gcd_bytes_size);

}
