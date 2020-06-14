#include <Compression/CompressionCodecMaskedVByte.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <common/unaligned.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <cstdlib>

namespace DB
{

namespace ErrorCodes
{
}

String CompressionCodecMaskedVByte::getCodecDesc() const
{
	return "MaskedVByte";
}

namespace {

UInt8 codecId()
{
	return static_cast<UInt8>(CompressionMethodByte::MaskedVByte);
}

size_t vbyte_encode(uint32_t *in, size_t length, uint8_t *bout) {
	uint8_t * initbout = bout;
	for (size_t k = 0; k < length; ++k) {
		const uint32_t val = in[k];

		if (val < (1U << 7)) {
			*bout = val & 0x7F;
			++bout;
		} else if (val < (1U << 14)) {
			*bout = (uint8_t) ((val & 0x7F) | (1U << 7));
			++bout;
			*bout = (uint8_t) (val >> 7);
			++bout;
		} else if (val < (1U << 21)) {
			*bout = (uint8_t) ((val & 0x7F) | (1U << 7));
			++bout;
			*bout = (uint8_t) (((val >> 7) & 0x7F) | (1U << 7));
			++bout;
			*bout = (uint8_t) (val >> 14);
			++bout;
		} else if (val < (1U << 28)) {
			*bout = (uint8_t) ((val & 0x7F) | (1U << 7));
			++bout;
			*bout = (uint8_t) (((val >> 7) & 0x7F) | (1U << 7));
			++bout;
			*bout = (uint8_t) (((val >> 14) & 0x7F) | (1U << 7));
			++bout;
			*bout = (uint8_t) (val >> 21);
			++bout;
		} else {
			*bout = (uint8_t) ((val & 0x7F) | (1U << 7));
			++bout;
			*bout = (uint8_t) (((val >> 7) & 0x7F) | (1U << 7));
			++bout;
			*bout = (uint8_t) (((val >> 14) & 0x7F) | (1U << 7));
			++bout;
			*bout = (uint8_t) (((val >> 21) & 0x7F) | (1U << 7));
			++bout;
			*bout = (uint8_t) (val >> 28);
			++bout;
		}
	}
	return bout - initbout;
}

// len_signed : number of ints we want to decode
size_t masked_vbyte_decode(const uint8_t *in, uint32_t *out, uint64_t length) {
	size_t consumed = 0; // number of bytes read
	uint64_t count = 0; // how many integers we have read so far
	uint64_t sig = 0;
	int availablebytes = 0;
	if (96 < length) {
		size_t scanned = 0;


#ifdef __AVX2__
		__m256i low = _mm256_loadu_si256((__m256i *)(in + scanned));
		uint32_t lowSig = _mm256_movemask_epi8(low);
#else
		__m128i low1 = _mm_loadu_si128((__m128i *) (in + scanned));
		uint32_t lowSig1 = _mm_movemask_epi8(low1);
		__m128i low2 = _mm_loadu_si128((__m128i *) (in + scanned + 16));
		uint32_t lowSig2 = _mm_movemask_epi8(low2);
		uint32_t lowSig = lowSig2 << 16;
		lowSig |= lowSig1;
#endif

		// excess verbosity to avoid problems with sign extension on conversions
		// better to think about what's happening and make it clearer
		__m128i high = _mm_loadu_si128((__m128i *) (in + scanned + 32));
		uint32_t highSig = _mm_movemask_epi8(high);
		uint64_t nextSig = highSig;
		nextSig <<= 32;
		nextSig |= lowSig;
		scanned += 48;

		do {
			uint64_t thisSig = nextSig;

#ifdef __AVX2__
			low = _mm256_loadu_si256((__m256i *)(in + scanned));
			lowSig = _mm256_movemask_epi8(low);
#else
			low1 = _mm_loadu_si128((__m128i *) (in + scanned));
			lowSig1 = _mm_movemask_epi8(low1);
			low2 = _mm_loadu_si128((__m128i *) (in + scanned + 16));
			lowSig2 = _mm_movemask_epi8(low2);
			lowSig = lowSig2 << 16;
			lowSig |= lowSig1;
#endif

			high = _mm_loadu_si128((__m128i *) (in + scanned + 32));
			highSig = _mm_movemask_epi8(high);
			nextSig = highSig;
			nextSig <<= 32;
			nextSig |= lowSig;

			uint64_t remaining = scanned - (consumed + 48);
			sig = (thisSig << remaining) | sig;

			uint64_t reload = scanned - 16;
			scanned += 48;

			// need to reload when less than 16 scanned bytes remain in sig
			while (consumed < reload) {
				uint64_t ints_read;
				uint64_t bytes = masked_vbyte_read_group(in + consumed,
														 out + count, sig, &ints_read);
				sig >>= bytes;

				// seems like this might force the compiler to prioritize shifting sig >>= bytes
				if (sig == 0xFFFFFFFFFFFFFFFF)
					return 0; // fake check to force earliest evaluation

				consumed += bytes;
				count += ints_read;
			}
		} while (count + 112 <
				 length);  // 112 == 48 + 48 ahead for scanning + up to 16 remaining in sig
		sig = (nextSig << (scanned - consumed - 48)) | sig;
		availablebytes = scanned - consumed;
	}
	while (availablebytes + count < length) {
		if (availablebytes < 16) {
			if (availablebytes + count + 31 < length) {
#ifdef __AVX2__
				uint64_t newsigavx = (uint32_t) _mm256_movemask_epi8(_mm256_loadu_si256((__m256i *)(in + availablebytes + consumed)));
				sig |= (newsigavx << availablebytes);
#else
				uint64_t newsig = _mm_movemask_epi8(
						_mm_lddqu_si128(
								(const __m128i *) (in + availablebytes
												   + consumed)));
				uint64_t newsig2 = _mm_movemask_epi8(
						_mm_lddqu_si128(
								(const __m128i *) (in + availablebytes + 16
												   + consumed)));
				sig |= (newsig << availablebytes)
					   | (newsig2 << (availablebytes + 16));
#endif
				availablebytes += 32;
			} else if (availablebytes + count + 15 < length) {
				int newsig = _mm_movemask_epi8(
						_mm_lddqu_si128(
								(const __m128i *) (in + availablebytes
												   + consumed)));
				sig |= newsig << availablebytes;
				availablebytes += 16;
			} else {
				break;
			}
		}
		uint64_t ints_read;

		uint64_t eaten = masked_vbyte_read_group(in + consumed, out + count, sig, &ints_read);
		consumed += eaten;
		availablebytes -= eaten;
		sig >>= eaten;
		count += ints_read;
	}
	for (; count < length; count++) {
		consumed += read_int(in + consumed, out + count);
	}
	return consumed;
}

}

UInt32 CompressionCodecMaskedVByte::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
	return vbyte_encode(source, source_size, dest);
}

void CompressionCodecMaskedVByte::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
	uint64_t length = uncompressed_size / sizeof(uint32_t);
	masked_vbyte_decode(source, dest, length);
}

UInt8 CompressionCodecMaskedVByte::getMethodByte() const
{
	return codecId();
}

void registerCodecMaskedVByte(CompressionCodecFactory & factory)
{
	factory.registerSimpleCompressionCodec("MaskedVByte", codecId(), [&] ()
	{
		return std::make_shared<CompressionCodecMaskedVByte>();
	});
}

}