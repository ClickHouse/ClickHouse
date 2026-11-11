#include "config.h"

#if USE_BROTLI

#include <brotli/decode.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>

TEST(BrotliDecoder, RejectInvalidBlockSwitch)
{
    // A block switch is invalid when the stream declares only one block type.
    // From https://github.com/nodejs/node/blob/main/test/parallel/test-zlib-invalid-input.js.
    constexpr std::array<uint8_t, 22> input = {
        0x20, 0x00, 0x10, 0x41, 0x41, 0x41, 0xfc, 0xff, 0xff, 0x07, 0x00,
        0x22, 0x28, 0xf0, 0x0b, 0x60, 0xf7, 0xf4, 0x1f, 0x00, 0x00, 0x00};

    for (size_t chunk_size : {input.size(), size_t{1}})
    {
        std::unique_ptr<BrotliDecoderState, decltype(&BrotliDecoderDestroyInstance)> state(
            BrotliDecoderCreateInstance(nullptr, nullptr, nullptr), &BrotliDecoderDestroyInstance);
        ASSERT_NE(state.get(), nullptr);

        std::array<uint8_t, 4096> output{};
        size_t total_output = 0;
        BrotliDecoderResult result = BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT;

        for (size_t offset = 0; offset < input.size() && result == BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT;)
        {
            size_t available_input = std::min(chunk_size, input.size() - offset);
            const size_t provided_input = available_input;
            const uint8_t * next_input = input.data() + offset;
            do
            {
                uint8_t * next_output = output.data();
                size_t available_output = output.size();
                result = BrotliDecoderDecompressStream(
                    state.get(), &available_input, &next_input, &available_output, &next_output, nullptr);
                total_output += output.size() - available_output;
                ASSERT_LT(total_output, 64 * 1024 * 1024);
            }
            while (result == BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT);
            offset += provided_input - available_input;
            if (result == BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT)
                ASSERT_GT(provided_input - available_input, 0);
        }

        EXPECT_EQ(result, BROTLI_DECODER_RESULT_ERROR);
        // -17 is the `BROTLI_DECODER_ERROR_FORMAT_BLOCK_SWITCH` code added by the fix.
        EXPECT_EQ(BrotliDecoderGetErrorCode(state.get()), static_cast<BrotliDecoderErrorCode>(-17));
    }
}

#endif
