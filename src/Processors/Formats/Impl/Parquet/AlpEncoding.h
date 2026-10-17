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

namespace DB::Parquet
{

/// Decodes a data page of `ALP` encoded values of physical type `FLOAT` or `DOUBLE`.
std::unique_ptr<PageDecoder> makeAlpDecoder(
    parq::Type::type physical_type, std::span<const char> data, std::shared_ptr<FixedSizeConverter> converter);

/// Encoders that buffer the values of a page and encode them with `ALP` in `FlushValues`.
/// A page for which `ALP` would be bigger than `PLAIN` is written as `PLAIN`: after `FlushValues`,
/// `encoding` returns the encoding of the page it has just produced, as `parquet::Encoding::type`
/// (which has no `ALP` enumerator, so `ALP` is the value of `parq::Encoding::ALP` cast to it).
/// `EstimatedDataEncodedSize` is the size of the values in `PLAIN`, which is thus an upper bound.
std::unique_ptr<parquet::TypedEncoder<parquet::FloatType>> makeAlpFloatEncoder();
std::unique_ptr<parquet::TypedEncoder<parquet::DoubleType>> makeAlpDoubleEncoder();

}
