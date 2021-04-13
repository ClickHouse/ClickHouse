/*
    __ _____ _____ _____
 __|  |   __|     |   | |  JSON for Modern C++ (fuzz test support)
|  |  |__   |  |  | | | |  version 3.9.1
|_____|_____|_____|_|___|  https://github.com/nlohmann/json

This file implements a parser test suitable for fuzz testing. Given a byte
array data, it performs the following steps:

- j1 = from_cbor(data)
- vec = to_cbor(j1)
- j2 = from_cbor(vec)
- assert(j1 == j2)

The provided function `LLVMFuzzerTestOneInput` can be used in different fuzzer
drivers.

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
*/

#include <iostream>
#include <sstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// see http://llvm.org/docs/LibFuzzer.html
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size)
{
    try
    {
        // step 1: parse input
        std::vector<uint8_t> vec1(data, data + size);
        json j1 = json::from_cbor(vec1);

        try
        {
            // step 2: round trip
            std::vector<uint8_t> vec2 = json::to_cbor(j1);

            // parse serialization
            json j2 = json::from_cbor(vec2);

            // serializations must match
            assert(json::to_cbor(j2) == vec2);
        }
        catch (const json::parse_error&)
        {
            // parsing a CBOR serialization must not fail
            assert(false);
        }
    }
    catch (const json::parse_error&)
    {
        // parse errors are ok, because input may be random bytes
    }
    catch (const json::type_error&)
    {
        // type errors can occur during parsing, too
    }
    catch (const json::out_of_range&)
    {
        // out of range errors can occur during parsing, too
    }

    // return 0 - non-zero return values are reserved for future use
    return 0;
}
