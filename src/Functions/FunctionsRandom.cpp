#include <Functions/FunctionsRandom.h>
#include <Functions/FunctionFactory.h>
#include <Functions/VectorExtension.h>
#include <Common/HashTable/Hash.h>
#include <Common/TargetSpecific.h>
#include <Common/randomSeed.h>
#include <base/unaligned.h>
#if defined(__AVX2__)
#  include <x86intrin.h>
#endif
#if defined(__aarch64__) && defined(__ARM_FEATURE_AES)
#  include <arm_neon.h>
#endif

/// Random bytes come from two algorithms, between them implemented five ways.
///
/// LCG, a linear congruential generator: x = a * x + c, several independent chains stepped side by
/// side, four bytes taken out of the middle of each state. See LinearCongruentialGenerator below.
/// This is what every target used before the AArch64 arm was added.
///
/// ARS, reduced-round AES applied to a counter. Faster wherever the CPU has AES instructions,
/// because it issues on the cryptographic pipe instead of contending for the multipliers that limit
/// the LCG. Described in full on ArsParams below.
///
/// Which implementation runs where:
///
///   target                  generator                     chosen by
///   ----------------------  ----------------------------  ----------------------------------------
///   AArch64 with AES        ARS, over AESE/AESMC          RAND_ARS_NEON, at compile time
///   x86-64-v3 or higher     ARS, over VAESENC at v4       run time: VAES and x86-64-v4
///                           ARS, over VAESENC at v3       run time: VAES only, i.e. Zen 3
///                           LCG, vectorised at v3         run time: no VAES
///   everything else         LCG, four scalar chains       compile time
///
/// The three x86-64 rows exist only where RAND_ARS_VAES is set, which needs __AVX2__ - what
/// x86-64-v3 supplies - and multitarget code enabled. "Everything else" is AArch64 built for the
/// compat profile, ppc64le, riscv64, and x86-64 below v3.
///
/// Only x86-64 needs the run-time check, because VAES belongs to no x86-64 microarchitecture level -
/// Zen 3 has it without any AVX-512, Intel only from Ice Lake - so the build flags cannot imply it.
/// The two conditions together also keep AES off Skylake-SP and Cascade Lake, which are x86-64-v4
/// but have no VAES, and which are the parts where 512-bit code costs the most in clock frequency.
/// AArch64 decides at compile time instead: the server profile always builds with the crypto
/// extensions (cpu_features.cmake passes +crypto) and the compat profile never does, so
/// __ARM_FEATURE_AES already answers the question.
///
/// The five produce different streams, which is fine: rand() declares isDeterministic() == false and
/// is reseeded from randomSeed() on every call, so no caller may depend on the exact sequence.
#if defined(__aarch64__) && defined(__ARM_FEATURE_AES)
#  define RAND_ARS_NEON 1
#else
#  define RAND_ARS_NEON 0
#endif
#if defined(__AVX2__) && USE_MULTITARGET_CODE
#  define RAND_ARS_VAES 1
#else
#  define RAND_ARS_VAES 0
#endif


namespace DB
{
namespace
{
/// NOTE Probably
///    http://www.pcg-random.org/
/// or http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/SFMT/
/// or http://docs.yeppp.info/c/group__yep_random___w_e_l_l1024a.html
/// could go better.

struct LinearCongruentialGenerator
{
    /// Constants from `man lrand48_r`.
    static constexpr UInt64 a = 0x5DEECE66D;
    static constexpr UInt64 c = 0xB;

    /// And this is from `head -c8 /dev/urandom | xxd -p`
    UInt64 current = 0x09826f4a081cee35ULL;

    void seed(UInt64 value)
    {
        current = value;
    }

    [[maybe_unused]] UInt32 next()
    {
        current = current * a + c;
        return static_cast<UInt32>(current >> 16);
    }
};

UInt64 calcSeed(UInt64 rand_seed, UInt64 additional_seed)
{
    return intHash64(rand_seed ^ intHash64(additional_seed));
}

[[maybe_unused]] void seed(LinearCongruentialGenerator & generator, UInt64 rand_seed, intptr_t additional_seed)
{
    generator.seed(calcSeed(rand_seed, additional_seed));
}

/// The array of random numbers from 'head -c8 /dev/urandom | xxd -p'.
/// Can be used for creating seeds for random generators.
constexpr std::array<UInt64, 32> random_numbers = {
    0x0c8ff307dabc0c4cULL, 0xf4bce78bf3821c1bULL, 0x4eb628a1e189c21aULL, 0x85ae000d253e0dbcULL,
    0xc98073e6480f8a10ULL, 0xb17e9b70a084d570ULL, 0x1361c752b768da8cULL, 0x3d915f60c06d144dULL,
    0xd5bc9b7aced79587ULL, 0x66c28000ba8a66cfULL, 0x0fb58da7a48820f5ULL, 0x540ee1b57aa861a1ULL,
    0x212f11936ef2db04ULL, 0xa3939cd900edcc58ULL, 0xc676c84420170102ULL, 0xcbdc824e8b4bf3edULL,

    0x8296f9d93cc94e3bULL, 0x78a7e826d62085b2ULL, 0xaa30620211fc6c69ULL, 0xbd38de52f0a93677ULL,
    0x19983de8d79dcc4eULL, 0x8afe883ef2199e6fULL, 0xb7160f7ed022b60aULL, 0x2ce173d373ddafd4ULL,
    0x15762761bb55b9acULL, 0x3e448fc94fdd28e7ULL, 0xa5121232adfbe70aULL, 0xb1e0f6d286112804ULL,
    0x6062e96de9554806ULL, 0xcc679b329c28882aULL, 0x5c6d29f45cbc060eULL, 0x1af1325a86ffb162ULL,
};

#if RAND_ARS_NEON || RAND_ARS_VAES
/// Parameters shared by the two arms below that generate bytes with ARS.
///
/// ARS, "Advanced Randomization System", is reduced-round AES in counter mode, from Salmon et al.,
/// "Parallel Random Numbers: As Easy as 1, 2, 3" (SC'11). The name is that paper's play on AES; it
/// is not a cipher, only the AES round function reused as a generator. Five rounds is the
/// configuration the paper reports as passing TestU01 BigCrush; its reference implementation
/// defaults to seven for margin. The round keys are a Weyl sequence, so there is no AES key
/// schedule inside the generation loop.
///
/// Only the seeding and the constants are common: the round itself is AESE/AESMC on AArch64 and
/// VAESENC on x86-64, and the two instruction sets order the key addition differently.
struct ArsParams
{
    static constexpr int rounds = 5;
    static constexpr int blocks = 8;    /// independent counter blocks in flight
    static constexpr UInt64 weyl_0 = 0x9E3779B97F4A7C15ULL;
    static constexpr UInt64 weyl_1 = 0xBB67AE8584CAA73BULL;

    UInt64 key_lo;
    UInt64 key_hi;
    UInt64 counter;

    explicit ArsParams(const char * output)
    {
        const UInt64 rand_seed = randomSeed();
        const intptr_t additional_seed = reinterpret_cast<intptr_t>(output);
        key_lo = calcSeed(rand_seed, random_numbers[0] + additional_seed);
        key_hi = calcSeed(rand_seed, random_numbers[1] + additional_seed);
        counter = calcSeed(rand_seed, random_numbers[2] + additional_seed);
    }

    /// Step the Weyl sequence to the next round key.
    void advanceKey()
    {
        key_lo += weyl_0;
        key_hi += weyl_1;
    }
};
#endif
}

#if RAND_ARS_NEON

/// AArch64 has no 128-bit vector multiply for 64-bit lanes - there is no NEON
/// counterpart of the AVX-512 VPMULLQ - so the LinearCongruentialGenerator loop
/// of the scalar arm below cannot be vectorised the way the AVX2 arm is:
/// synthesising a 64x64 product out of vmull_u32 costs more instructions per
/// generated byte than the scalar MADD it would replace, and measures slower.
///
/// Use ARS instead (see ArsParams above). AESE/AESMC issue on the cryptographic
/// pipe, so generation stops competing for the single integer multiply unit that
/// throughput-limits the scalar arm.
void RandImpl::execute(char * output, size_t size)
{
    if (size == 0)
        return;

    static constexpr int rounds = ArsParams::rounds;
    static constexpr int blocks = ArsParams::blocks;
    static constexpr ptrdiff_t bytes_per_write = 16 * blocks;
    static constexpr ptrdiff_t safe_overwrite = PADDING_FOR_SIMD - 1;

    ArsParams params(output);

    uint8x16_t round_keys[rounds + 1];
    for (auto & round_key : round_keys)
    {
        const UInt64 key[2] = {params.key_lo, params.key_hi};
        round_key = vreinterpretq_u8_u64(vld1q_u64(key));
        params.advanceKey();
    }

    UInt64 counter = params.counter;

    char * end = output + size;

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        uint8x16_t state[blocks];
        for (int i = 0; i < blocks; ++i)
        {
            const UInt64 block_counter[2] = {counter + static_cast<UInt64>(i), 0};
            state[i] = vreinterpretq_u8_u64(vld1q_u64(block_counter));
        }
        for (int r = 0; r + 1 < rounds; ++r)
            for (auto & block : state)
                block = vaesmcq_u8(vaeseq_u8(block, round_keys[r]));
        for (auto & block : state)
            block = veorq_u8(vaeseq_u8(block, round_keys[rounds - 1]), round_keys[rounds]);
        for (int i = 0; i < blocks; ++i)
            vst1q_u8(reinterpret_cast<uint8_t *>(output) + 16 * i, state[i]);
        output += bytes_per_write;
        counter += static_cast<UInt64>(blocks);
    }

    /// Tail, one 16-byte block at a time. It is guaranteed (by PaddedPODArray)
    /// that we can overwrite up to PADDING_FOR_SIMD - 1 bytes after end.
    while (output < end)
    {
        const UInt64 block_counter[2] = {counter, 0};
        uint8x16_t state = vreinterpretq_u8_u64(vld1q_u64(block_counter));
        for (int r = 0; r + 1 < rounds; ++r)
            state = vaesmcq_u8(vaeseq_u8(state, round_keys[r]));
        state = veorq_u8(vaeseq_u8(state, round_keys[rounds - 1]), round_keys[rounds]);
        vst1q_u8(reinterpret_cast<uint8_t *>(output), state);
        output += 16;
        ++counter;
    }
}

#elif !defined(__AVX2__)

void RandImpl::execute(char * output, size_t size)
{
    LinearCongruentialGenerator generator0;
    LinearCongruentialGenerator generator1;
    LinearCongruentialGenerator generator2;
    LinearCongruentialGenerator generator3;

    UInt64 rand_seed = randomSeed();

    seed(generator0, rand_seed, random_numbers[0] + reinterpret_cast<intptr_t>(output));
    seed(generator1, rand_seed, random_numbers[1] + reinterpret_cast<intptr_t>(output));
    seed(generator2, rand_seed, random_numbers[2] + reinterpret_cast<intptr_t>(output));
    seed(generator3, rand_seed, random_numbers[3] + reinterpret_cast<intptr_t>(output));

    for (const char * end = output + size; output < end; output += 16)
    {
        unalignedStore<UInt32>(output, generator0.next());
        unalignedStore<UInt32>(output + 4, generator1.next());
        unalignedStore<UInt32>(output + 8, generator2.next());
        unalignedStore<UInt32>(output + 12, generator3.next());
    }
    /// It is guaranteed (by PaddedPODArray) that we can overwrite up to 15 bytes after end.
}

#else

using namespace VectorExtension;

/// The Murmur finalizer of intHash64, applied to four values at once.
inline UInt64x4 intHash64x4(UInt64x4 x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;

    return x;
}

/// calcSeed for four consecutive entries of random_numbers, starting at `offset`.
inline UInt64x4 calcSeeds(UInt64 rand_seed, size_t offset, const char * output)
{
    const UInt64 additional_seed = reinterpret_cast<intptr_t>(output);
    return intHash64x4(intHash64x4(unalignedLoad<UInt64x4>(&random_numbers[offset]) + additional_seed) ^ rand_seed);
}

/* Takes 2 vectors with LinearCongruentialGenerator states and combines them into vector with random values.
 * From every rand-state we use only bits 15...47 to generate random vector.
 */
inline UInt64x4 combineValues(UInt64x4 a, UInt64x4 b)
{
    auto xa = reinterpret_cast<__m256i>(a);
    auto xb = reinterpret_cast<__m256i>(b);
    /// Every state is 8-byte value and we need to use only 4 from the middle.
    /// Swap the low half and the high half of every state to move these bytes from the middle to sides.
    /// xa = xa[1, 0, 3, 2, 5, 4, 7, 6]
    xa = _mm256_shuffle_epi32(xa, 0xb1);
    /// Now every 8-byte value in xa is xx....xx and every value in xb is ..xxxx.. where x is random byte we want to use.
    /// Just blend them to get the result vector.
    /// result = xa[0],xb[1,2],xa[3,4],xb[5,6],xa[7,8],xb[9,10],xa[11,12],xb[13,14],xa[15]
    __m256i result = _mm256_blend_epi16(xa, xb, 0x66);
    return reinterpret_cast<UInt64x4>(result);
}

/// Used on any x86-64 CPU without VAES.
static void executeLinearCongruential(char * output, size_t size)
{
    if (size == 0)
        return;

    char * end = output + size;

    constexpr int vec_size = 4;
    constexpr int safe_overwrite = PADDING_FOR_SIMD - 1;
    constexpr int bytes_per_write = 4 * sizeof(UInt64x4);

    UInt64 rand_seed = randomSeed();

    UInt64 a = LinearCongruentialGenerator::a;
    constexpr UInt64 c = LinearCongruentialGenerator::c;

    /// Same seeds as the scalar formula calcSeed(rand_seed, random_numbers[i] + output), computed four at a time.
    UInt64x4 gens1 = calcSeeds(rand_seed, 0, output);
    UInt64x4 gens2 = calcSeeds(rand_seed, vec_size, output);
    UInt64x4 gens3 = calcSeeds(rand_seed, 2 * vec_size, output);
    UInt64x4 gens4 = calcSeeds(rand_seed, 3 * vec_size, output);

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        gens1 = gens1 * a + c;
        gens2 = gens2 * a + c;
        unalignedStore<UInt64x4>(output, combineValues(gens1, gens2));
        gens3 = gens3 * a + c;
        gens4 = gens4 * a + c;
        unalignedStore<UInt64x4>(output + sizeof(UInt64x4), combineValues(gens3, gens4));
        gens1 = gens1 * a + c;
        gens2 = gens2 * a + c;
        unalignedStore<UInt64x4>(output + 2 * sizeof(UInt64x4), combineValues(gens1, gens2));
        gens3 = gens3 * a + c;
        gens4 = gens4 * a + c;
        unalignedStore<UInt64x4>(output + 3 * sizeof(UInt64x4), combineValues(gens3, gens4));
        output += bytes_per_write;
    }

    // Process tail
    while ((end - output) > 0)
    {
        gens1 = gens1 * a + c;
        gens2 = gens2 * a + c;
        UInt64x4 values = combineValues(gens1, gens2);
        for (int i = 0; i < vec_size && (end - output) > 0; ++i)
        {
            unalignedStore<UInt64>(output, values[i]);
            output += sizeof(UInt64);
        }
    }
}

#if RAND_ARS_VAES

/// ARS on VAES (see ArsParams above), the x86-64 counterpart of the AArch64 arm.
///
/// The reason here is not the one given for AArch64: x86-64 does have a vector 64-bit multiply, so
/// the arm above is already vectorised. AES is still faster, because VAESENC retires several AES
/// blocks per instruction on the cryptographic pipe while the generator competes for the multiply
/// ports. Single-thread generation throughput against the arm above, at one default block of
/// randomFixedString(10):
///
///                                    this arm   executeArs512
///   Zen 4 (Ryzen 9 7950X3D)          1.91x      2.34x
///   Granite Rapids (Xeon 6975P-C)    2.07x      2.98x
///   Sapphire Rapids (Xeon 8488C)     2.08x      2.94x
///   Ice Lake (Xeon 8375C)            1.94x      2.75x
///   Zen 3 (EPYC 7R13)                1.47x      no AVX-512
///
/// A 256-bit register holds two AES blocks, so the eight blocks in flight that are eight registers
/// on AArch64 are four here. This arm runs only where there is VAES but no AVX-512, which today
/// means Zen 3; everything newer takes executeArs512 below.
X86_64_VAES_FUNCTION_SPECIFIC_ATTRIBUTE
static void executeArs(char * output, size_t size)
{
    if (size == 0)
        return;

    static constexpr int rounds = ArsParams::rounds;
    static constexpr int vectors = ArsParams::blocks / 2;
    static constexpr ptrdiff_t bytes_per_write = 32 * vectors;
    static constexpr ptrdiff_t safe_overwrite = PADDING_FOR_SIMD - 1;

    ArsParams params(output);

    __m256i round_keys[rounds + 1];
    for (auto & round_key : round_keys)
    {
        round_key = _mm256_broadcastsi128_si256(
            _mm_set_epi64x(static_cast<Int64>(params.key_hi), static_cast<Int64>(params.key_lo)));
        params.advanceKey();
    }

    UInt64 counter = params.counter;

    char * end = output + size;

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        __m256i state[vectors];
        for (int i = 0; i < vectors; ++i)
        {
            const UInt64 block_counter = counter + static_cast<UInt64>(2 * i);
            state[i] = _mm256_xor_si256(
                _mm256_set_epi64x(0, static_cast<Int64>(block_counter + 1), 0, static_cast<Int64>(block_counter)),
                round_keys[0]);
        }
        for (int r = 1; r < rounds; ++r)
        {
            for (auto & block : state)
                block = _mm256_aesenc_epi128(block, round_keys[r]);
        }
        for (auto & block : state)
            block = _mm256_aesenclast_epi128(block, round_keys[rounds]);
        for (int i = 0; i < vectors; ++i)
            _mm256_storeu_si256(reinterpret_cast<__m256i *>(output) + i, state[i]);
        output += bytes_per_write;
        counter += static_cast<UInt64>(ArsParams::blocks);
    }

    /// Tail, two 16-byte blocks at a time. It is guaranteed (by PaddedPODArray)
    /// that we can overwrite up to PADDING_FOR_SIMD - 1 bytes after end.
    while (output < end)
    {
        __m256i state = _mm256_xor_si256(
            _mm256_set_epi64x(0, static_cast<Int64>(counter + 1), 0, static_cast<Int64>(counter)), round_keys[0]);
        for (int r = 1; r < rounds; ++r)
            state = _mm256_aesenc_epi128(state, round_keys[r]);
        state = _mm256_aesenclast_epi128(state, round_keys[rounds]);
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(output), state);
        output += 32;
        counter += 2;
    }
}

/// The same generator over 512-bit registers, four AES blocks per instruction instead of two.
///
/// Worth a separate arm because the saving is in instructions retired rather than in AES throughput,
/// so it shows up on parts that execute AVX-512 over narrower datapaths as much as on those that do
/// not: see the table above, roughly 1.2x to 1.4x over the 256-bit arm on all four measured.
///
/// One caveat, so nobody re-derives it from a profile: on Intel, a call of a few MB or more makes
/// this arm slower than the 256-bit one - 0.79x at 8 MB on Ice Lake, 0.80x on Sapphire Rapids,
/// 0.96x on Granite Rapids - while AMD shows no crossover at any size. The cause was not
/// established. It is not the memory system running out of bandwidth: that would make the two arms
/// converge, not invert. A frequency effect fits better, being Intel-only and appearing only once
/// a call runs long enough. Calls that large are rare - one default block of randomFixedString(10)
/// is 640 KiB - so there is no size threshold here.
X86_64_VAES512_FUNCTION_SPECIFIC_ATTRIBUTE
static void executeArs512(char * output, size_t size)
{
    if (size == 0)
        return;

    static constexpr int rounds = ArsParams::rounds;
    static constexpr int vectors = ArsParams::blocks / 4;
    static constexpr ptrdiff_t bytes_per_write = 64 * vectors;
    static constexpr ptrdiff_t safe_overwrite = PADDING_FOR_SIMD - 1;

    ArsParams params(output);

    __m512i round_keys[rounds + 1];
    for (auto & round_key : round_keys)
    {
        round_key = _mm512_broadcast_i32x4(
            _mm_set_epi64x(static_cast<Int64>(params.key_hi), static_cast<Int64>(params.key_lo)));
        params.advanceKey();
    }

    UInt64 counter = params.counter;

    char * end = output + size;

    while ((end - output) + safe_overwrite >= bytes_per_write)
    {
        __m512i state[vectors];
        for (int i = 0; i < vectors; ++i)
        {
            const UInt64 block_counter = counter + static_cast<UInt64>(4 * i);
            state[i] = _mm512_xor_si512(
                _mm512_set_epi64(0, static_cast<Int64>(block_counter + 3),
                                 0, static_cast<Int64>(block_counter + 2),
                                 0, static_cast<Int64>(block_counter + 1),
                                 0, static_cast<Int64>(block_counter)),
                round_keys[0]);
        }
        for (int r = 1; r < rounds; ++r)
        {
            for (auto & block : state)
                block = _mm512_aesenc_epi128(block, round_keys[r]);
        }
        for (auto & block : state)
            block = _mm512_aesenclast_epi128(block, round_keys[rounds]);
        for (int i = 0; i < vectors; ++i)
            _mm512_storeu_si512(output + 64 * i, state[i]);
        output += bytes_per_write;
        counter += static_cast<UInt64>(ArsParams::blocks);
    }

    /// Tail, four 16-byte blocks at a time. The last store can reach exactly PADDING_FOR_SIMD - 1
    /// bytes past end, which is all PaddedPODArray guarantees - this arm has no margin left over,
    /// so a wider tail store would be out of bounds.
    while (output < end)
    {
        __m512i state = _mm512_xor_si512(
            _mm512_set_epi64(0, static_cast<Int64>(counter + 3), 0, static_cast<Int64>(counter + 2),
                             0, static_cast<Int64>(counter + 1), 0, static_cast<Int64>(counter)),
            round_keys[0]);
        for (int r = 1; r < rounds; ++r)
            state = _mm512_aesenc_epi128(state, round_keys[r]);
        state = _mm512_aesenclast_epi128(state, round_keys[rounds]);
        _mm512_storeu_si512(output, state);
        output += 64;
        counter += 4;
    }
}

#endif

void RandImpl::execute(char * output, size_t size)
{
#if RAND_ARS_VAES
    if (isArchSupported(TargetArch::x86_64_vaes))
    {
        if (isArchSupported(TargetArch::x86_64_v4))
            executeArs512(output, size);
        else
            executeArs(output, size);
        return;
    }
#endif
    executeLinearCongruential(output, size);
}

#endif

}
