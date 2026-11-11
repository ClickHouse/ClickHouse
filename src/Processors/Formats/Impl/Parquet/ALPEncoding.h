#pragma once

#include <Processors/Formats/Impl/Parquet/Decoding.h>

#include <parquet/encoding.h>

/// The `ALP` (Adaptive Lossless floating-Point) encoding of Parquet `FLOAT` and `DOUBLE` values.
///
/// Specification: https://github.com/apache/parquet-format/blob/master/AlpEncoding.md
/// Paper: "ALP: Adaptive Lossless floating-Point Compression", Afroozeh, Kuffo, Boncz (SIGMOD 2024).
///
/// A page is a 7-byte header, an array of `num_vectors` 4-byte offsets, and the vectors. A vector
/// holds up to `vector_size` values, each turned into an integer `round(value * 10^e * 10^-f)`,
/// which is stored as a bit-packed difference from the minimum (frame of reference) of the vector.
/// The values that the decoding (`integer * 10^f * 10^-e`) does not reproduce bit-exactly are
/// stored verbatim as exceptions.
///
/// This doesn't reuse the `ALP` compression codec (`CompressionCodecALP.cpp`), which implements the
/// same idea for a different format:
/// - The codec bit-packs with FastLanes `FFOR` (`Compression/FFOR.h`), which interleaves 1024 values
///   across lanes, while Parquet packs the values sequentially, LSB first.
/// - The codec scales and decodes `Float32` in `Float64` arithmetic, while in Parquet the decoding is
///   normative and done in the precision of the value type. A value that round-trips with the codec
///   may decode differently in other Parquet readers.
/// - For `Float32`, the codec searches the exponents 0..9, the specification allows 0..10.
/// - The headers of the page and the vectors, and the layout of the exceptions, are different.
/// - The choice of parameters depends on all of the above. Besides, here the round trip is checked
///   bit-exactly, and the samples are taken at pseudo-random positions to avoid aliasing with periodic data.
/// The only common part, the table of powers of ten, does not justify coupling the two implementations.

namespace DB::Parquet
{

/// Decodes a data page of `ALP` encoded values of physical type `FLOAT` or `DOUBLE`.
std::unique_ptr<PageDecoder> makeALPDecoder(
    parq::Type::type physical_type, std::span<const char> data, std::shared_ptr<FixedSizeConverter> converter);

/// Encoders that buffer the values of a page and encode them with `ALP` in `FlushValues`.
/// A page for which `ALP` would be bigger than `PLAIN` is written as `PLAIN`: after `FlushValues`,
/// `encoding` returns the encoding of the page it has just produced, as `parquet::Encoding::type`
/// (which has no `ALP` enumerator, so `ALP` is the value of `parq::Encoding::ALP` cast to it).
/// `EstimatedDataEncodedSize` is the size of the values in `PLAIN`, which is thus an upper bound.
std::unique_ptr<parquet::TypedEncoder<parquet::FloatType>> makeALPFloatEncoder();
std::unique_ptr<parquet::TypedEncoder<parquet::DoubleType>> makeALPDoubleEncoder();

}
