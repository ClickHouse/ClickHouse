#pragma once

#include <Compression/ALPCommon.h>

#include <cstring>
#include <limits>
#include <utility>
#include <vector>

namespace DB::Parquet::ALP
{

template <typename T>
struct WireTraits;

template <>
struct WireTraits<Float64>
{
    using StorageInt = Int64;
    static constexpr UInt8 max_exponent_excl = 19 static constexpr UInt8 exception_bytes = 8;
    static constexpr bool is_float = false;
};

template <>
struct WireTraits<Float32>
{
    using StorageInt = Int32;
    static constexpr UInt8 max_exponent_excl = 10;
    static constexpr UInt8 exception_bytes = 4;
    static constexpr bool is_float = true;
};

template <typename T>
struct Codec
{
    using StorageInt = typename WireTraits<T>::StorageInt;

    static constexpr UInt8 default_log_vector_size = 10;

    static bool encodeValue(T value, UInt8 exponent, UInt8 factor, StorageInt & encoded_out)
    {
        const Int64 encoded = ALPFloatUtils::encodeValue(value, exponent, factor);

        if constexpr (WireTraits<T>::is_float)
            if (encoded < std::numeric_limits<Int32>::min() || encoded > std::numeric_limits<Int32>::max())
                return false;

        if (ALPFloatUtils::decodeValue<T>(encoded, exponent, factor) != value)
            return false;

        encoded_out = static_cast<StorageInt>(encoded);
        return true;
    }

    static T decodeValue(StorageInt encoded, UInt8 exponent, UInt8 factor)
    {
        return ALPFloatUtils::decodeValue<T>(static_cast<Int64>(encoded), exponent, factor);
    }

    static UInt8 bitWidth(UInt64 range) { return range == 0 ? 0 : static_cast<UInt8>(std::bit_width(range)); }

    static UInt64 estimateCost(const T * values, size_t count, UInt8 exponent, UInt8 factor)
    {
        StorageInt min_v = std::numeric_limits<StorageInt>::max();
        StorageInt max_v = std::numeric_limits<StorageInt>::min();
        UInt64 good = 0;
        UInt64 exceptions = 0;
        const size_t stride = count > 64 ? count / 32 : 1;

        for (size_t i = 0; i < count; i += stride)
        {
            StorageInt encoded;
            if (encodeValue(values[i], exponent, factor, encoded))
            {
                ++good;
                min_v = std::min(min_v, encoded);
                max_v = std::max(max_v, encoded);
            }
            else
                ++exceptions;
        }

        if (good == 0)
            return std::numeric_limits<UInt64>::max();

        const UInt64 range = static_cast<UInt64>(max_v) - static_cast<UInt64>(min_v);
        const UInt64 sampled = good + exceptions;
        const UInt64 exception_bits = WireTraits<T>::exception_bytes * 8 + 16;
        return static_cast<UInt64>(bitWidth(range)) * count + (exceptions * count / sampled) * exception_bits;
    }

    static std::pair<UInt8, UInt8> chooseParams(const T * values, size_t count)
    {
        UInt8 best_exponent = 0;
        UInt8 best_factor = 0;
        UInt64 best_cost = std::numeric_limits<UInt64>::max();

        for (UInt8 exponent = 0; exponent < WireTraits<T>::max_exponent_excl; ++exponent)
            for (UInt8 factor = 0; factor <= exponent; ++factor)
            {
                const UInt64 cost = estimateCost(values, count, exponent, factor);
                if (cost < best_cost)
                {
                    best_cost = cost;
                    best_exponent = exponent;
                    best_factor = factor;
                }
            }

        return {best_exponent, best_factor};
    }

    static void appendLE(std::vector<UInt8> & out, UInt8 value) { out.push_back(value); }
    static void appendLE(std::vector<UInt8> & out, UInt16 value)
    {
        out.push_back(static_cast<UInt8>(value & 0xFF));
        out.push_back(static_cast<UInt8>((value >> 8) & 0xFF));
    }
    static void appendLE(std::vector<UInt8> & out, UInt32 value)
    {
        for (int i = 0; i < 4; ++i)
            out.push_back(static_cast<UInt8>((value >> (8 * i)) & 0xFF));
    }
    static void appendLE(std::vector<UInt8> & out, Int32 value) { appendLE(out, static_cast<UInt32>(value)); }
    static void appendLE(std::vector<UInt8> & out, Int64 value)
    {
        const UInt64 bits = static_cast<UInt64>(value);
        for (int i = 0; i < 8; ++i)
            out.push_back(static_cast<UInt8>((bits >> (8 * i)) & 0xFF));
    }

    static void bitPack(const std::vector<UInt64> & deltas, UInt8 bit_width, std::vector<UInt8> & out)
    {
        if (bit_width == 0)
            return;

        const UInt64 mask = bit_width == 64 ? ~0ULL : ((1ULL << bit_width) - 1);
        unsigned __int128 buffer = 0;
        int bits_in_buffer = 0;

        for (UInt64 delta : deltas)
        {
            buffer |= static_cast<unsigned __int128>(delta & mask) << bits_in_buffer;
            bits_in_buffer += bit_width;
            while (bits_in_buffer >= 8)
            {
                out.push_back(static_cast<UInt8>(buffer & 0xFF));
                buffer >>= 8;
                bits_in_buffer -= 8;
            }
        }
        if (bits_in_buffer > 0)
            out.push_back(static_cast<UInt8>(buffer & 0xFF));
    }

    static void encodeVector(const T * values, size_t count, std::vector<UInt8> & out, int forced_exponent = -1, int forced_factor = -1)
    {
        UInt8 exponent;
        UInt8 factor;
        if (forced_exponent >= 0)
        {
            exponent = static_cast<UInt8>(forced_exponent);
            factor = static_cast<UInt8>(forced_factor);
        }
        else
        {
            const auto params = chooseParams(values, count);
            exponent = params.first;
            factor = params.second;
        }

        std::vector<StorageInt> encoded(count);
        std::vector<UInt16> exception_positions;
        std::vector<T> exception_values;

        StorageInt placeholder = 0;
        bool have_good = false;
        for (size_t i = 0; i < count; ++i)
        {
            StorageInt value;
            if (encodeValue(values[i], exponent, factor, value))
            {
                encoded[i] = value;
                if (!have_good)
                {
                    placeholder = value;
                    have_good = true;
                }
            }
            else
            {
                exception_positions.push_back(static_cast<UInt16>(i));
                exception_values.push_back(values[i]);
                encoded[i] = 0;
            }
        }
        for (UInt16 position : exception_positions)
            encoded[position] = placeholder;

        StorageInt min_encoded = std::numeric_limits<StorageInt>::max();
        StorageInt max_encoded = std::numeric_limits<StorageInt>::min();
        for (StorageInt value : encoded)
        {
            min_encoded = std::min(min_encoded, value);
            max_encoded = std::max(max_encoded, value);
        }
        if (count == 0)
        {
            min_encoded = 0;
            max_encoded = 0;
        }

        const UInt64 range = static_cast<UInt64>(max_encoded) - static_cast<UInt64>(min_encoded);
        const UInt8 bit_width = bitWidth(range);

        std::vector<UInt64> deltas(count);
        for (size_t i = 0; i < count; ++i)
            deltas[i] = static_cast<UInt64>(encoded[i]) - static_cast<UInt64>(min_encoded);

        appendLE(out, exponent);
        appendLE(out, factor);
        appendLE(out, static_cast<UInt16>(exception_positions.size()));
        if constexpr (std::is_same_v<T, Float64>)
            appendLE(out, min_encoded);
        else
            appendLE(out, static_cast<Int32>(min_encoded));
        appendLE(out, bit_width);
        bitPack(deltas, bit_width, out);
        for (UInt16 position : exception_positions)
            appendLE(out, position);
        for (T value : exception_values)
        {
            UInt8 bytes[sizeof(T)];
            std::memcpy(bytes, &value, sizeof(T));
            out.insert(out.end(), bytes, bytes + sizeof(T));
        }
    }

    static void encodePage(
        const T * values,
        size_t count,
        std::vector<UInt8> & page,
        UInt8 log_vector_size = default_log_vector_size,
        int forced_exponent = -1,
        int forced_factor = -1)
    {
        const size_t vector_size = static_cast<size_t>(1) << log_vector_size;
        const size_t num_vectors = (count + vector_size - 1) / vector_size;

        appendLE(page, static_cast<UInt8>(0));
        appendLE(page, static_cast<UInt8>(0));
        appendLE(page, log_vector_size);
        appendLE(page, static_cast<Int32>(count));

        std::vector<UInt8> body;
        std::vector<UInt32> offsets;
        UInt32 offset = static_cast<UInt32>(num_vectors * 4);

        for (size_t v = 0; v < num_vectors; ++v)
        {
            offsets.push_back(offset);
            const size_t start = v * vector_size;
            const size_t vector_count = std::min(vector_size, count - start);
            std::vector<UInt8> vector_bytes;
            encodeVector(values + start, vector_count, vector_bytes, forced_exponent, forced_factor);
            offset += static_cast<UInt32>(vector_bytes.size());
            body.insert(body.end(), vector_bytes.begin(), vector_bytes.end());
        }

        for (UInt32 vector_offset : offsets)
            appendLE(page, vector_offset);
        page.insert(page.end(), body.begin(), body.end());
    }

    static void decodePage(const UInt8 * data, std::vector<T> & out)
    {
        auto readLE32 = [](const UInt8 * p) -> UInt32
        {
            UInt32 value = 0;
            for (int i = 0; i < 4; ++i)
                value |= static_cast<UInt32>(p[i]) << (8 * i);
            return value;
        };

        const UInt8 log_vector_size = data[2];
        const Int32 num_elements = static_cast<Int32>(readLE32(data + 3));
        const size_t vector_size = static_cast<size_t>(1) << log_vector_size;
        const size_t num_vectors = (static_cast<size_t>(num_elements) + vector_size - 1) / vector_size;
        const UInt8 * offset_array = data + 7;

        out.clear();
        for (size_t v = 0; v < num_vectors; ++v)
        {
            const UInt8 * vp = offset_array + readLE32(offset_array + v * 4);
            const size_t vector_count = std::min(vector_size, static_cast<size_t>(num_elements) - v * vector_size);

            const UInt8 exponent = vp[0];
            const UInt8 factor = vp[1];
            const UInt16 num_exceptions = static_cast<UInt16>(vp[2] | (vp[3] << 8));

            const UInt8 * cursor = vp + 4;
            StorageInt frame_of_reference;
            UInt8 bit_width;
            if constexpr (std::is_same_v<T, Float64>)
            {
                UInt64 bits = 0;
                for (int i = 0; i < 8; ++i)
                    bits |= static_cast<UInt64>(cursor[i]) << (8 * i);
                frame_of_reference = static_cast<StorageInt>(bits);
                bit_width = cursor[8];
                cursor += 9;
            }
            else
            {
                frame_of_reference = static_cast<StorageInt>(static_cast<Int32>(readLE32(cursor)));
                bit_width = cursor[4];
                cursor += 5;
            }

            std::vector<StorageInt> encoded(vector_count);
            const UInt64 mask = bit_width == 64 ? ~0ULL : (bit_width == 0 ? 0ULL : ((1ULL << bit_width) - 1));
            unsigned __int128 buffer = 0;
            int bits_in_buffer = 0;
            size_t byte_pos = 0;
            for (size_t i = 0; i < vector_count; ++i)
            {
                while (bits_in_buffer < bit_width)
                {
                    buffer |= static_cast<unsigned __int128>(cursor[byte_pos++]) << bits_in_buffer;
                    bits_in_buffer += 8;
                }
                const UInt64 delta = static_cast<UInt64>(buffer & mask);
                if (bit_width)
                {
                    buffer >>= bit_width;
                    bits_in_buffer -= bit_width;
                }
                encoded[i] = static_cast<StorageInt>(static_cast<UInt64>(frame_of_reference) + delta);
            }

            const size_t packed_bytes = (vector_count * static_cast<size_t>(bit_width) + 7) / 8;
            const UInt8 * exception_positions = cursor + packed_bytes;
            const UInt8 * exception_values = exception_positions + static_cast<size_t>(num_exceptions) * 2;

            std::vector<T> decoded(vector_count);
            for (size_t i = 0; i < vector_count; ++i)
                decoded[i] = decodeValue(encoded[i], exponent, factor);
            for (UInt16 j = 0; j < num_exceptions; ++j)
            {
                const UInt16 position = static_cast<UInt16>(exception_positions[2 * j] | (exception_positions[2 * j + 1] << 8));
                T value;
                std::memcpy(&value, exception_values + static_cast<size_t>(j) * sizeof(T), sizeof(T));
                decoded[position] = value;
            }

            for (T value : decoded)
                out.push_back(value);
        }
    }
};

}
