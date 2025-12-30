#pragma once
/* Copyright (c) 2017, 2021 Pieter Wuille
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <cstdint>
#include <string>
#include <vector>

namespace bech32
{

enum class Encoding
{
    INVALID,

    BECH32, //! Bech32 encoding as defined in BIP173
    BECH32M, //! Bech32m encoding as defined in BIP350
};

/** Encode a Bech32 or Bech32m string. If hrp contains uppercase characters, this will cause an
 *  assertion error. Encoding must be one of BECH32 or BECH32M. */
std::string encode(const std::string & hrp, const std::vector<uint8_t> & values, Encoding encoding);

/** A type for the result of decoding. */
struct DecodeResult
{
    Encoding encoding; //!< What encoding was detected in the result; Encoding::INVALID if failed.
    std::string hrp; //!< The human readable part
    std::vector<uint8_t> data; //!< The payload (excluding checksum)

    DecodeResult()
        : encoding(Encoding::INVALID)
    {
    }
    DecodeResult(Encoding enc, std::string && h, std::vector<uint8_t> && d)
        : encoding(enc)
        , hrp(std::move(h))
        , data(std::move(d))
    {
    }
};

/** Decode a Bech32 or Bech32m string. */
DecodeResult decode(const std::string & str);

}
