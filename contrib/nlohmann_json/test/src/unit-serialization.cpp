/*
    __ _____ _____ _____
 __|  |   __|     |   | |  JSON for Modern C++ (test suite)
|  |  |__   |  |  | | | |  version 3.9.1
|_____|_____|_____|_|___|  https://github.com/nlohmann/json

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2013-2019 Niels Lohmann <http://nlohmann.me>.

Permission is hereby  granted, free of charge, to any  person obtaining a copy
of this software and associated  documentation files (the "Software"), to deal
in the Software  without restriction, including without  limitation the rights
to  use, copy,  modify, merge,  publish, distribute,  sublicense, and/or  sell
copies  of  the Software,  and  to  permit persons  to  whom  the Software  is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE  IS PROVIDED "AS  IS", WITHOUT WARRANTY  OF ANY KIND,  EXPRESS OR
IMPLIED,  INCLUDING BUT  NOT  LIMITED TO  THE  WARRANTIES OF  MERCHANTABILITY,
FITNESS FOR  A PARTICULAR PURPOSE AND  NONINFRINGEMENT. IN NO EVENT  SHALL THE
AUTHORS  OR COPYRIGHT  HOLDERS  BE  LIABLE FOR  ANY  CLAIM,  DAMAGES OR  OTHER
LIABILITY, WHETHER IN AN ACTION OF  CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE  OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include "doctest_compatibility.h"

#include <nlohmann/json.hpp>
using nlohmann::json;

#include <sstream>
#include <iomanip>

TEST_CASE("serialization")
{
    SECTION("operator<<")
    {
        SECTION("no given width")
        {
            std::stringstream ss;
            json j = {"foo", 1, 2, 3, false, {{"one", 1}}};
            ss << j;
            CHECK(ss.str() == "[\"foo\",1,2,3,false,{\"one\":1}]");
        }

        SECTION("given width")
        {
            std::stringstream ss;
            json j = {"foo", 1, 2, 3, false, {{"one", 1}}};
            ss << std::setw(4) << j;
            CHECK(ss.str() ==
                  "[\n    \"foo\",\n    1,\n    2,\n    3,\n    false,\n    {\n        \"one\": 1\n    }\n]");
        }

        SECTION("given fill")
        {
            std::stringstream ss;
            json j = {"foo", 1, 2, 3, false, {{"one", 1}}};
            ss << std::setw(1) << std::setfill('\t') << j;
            CHECK(ss.str() ==
                  "[\n\t\"foo\",\n\t1,\n\t2,\n\t3,\n\tfalse,\n\t{\n\t\t\"one\": 1\n\t}\n]");
        }
    }

    SECTION("operator>>")
    {
        SECTION("no given width")
        {
            std::stringstream ss;
            json j = {"foo", 1, 2, 3, false, {{"one", 1}}};
            j >> ss;
            CHECK(ss.str() == "[\"foo\",1,2,3,false,{\"one\":1}]");
        }

        SECTION("given width")
        {
            std::stringstream ss;
            json j = {"foo", 1, 2, 3, false, {{"one", 1}}};
            ss.width(4);
            j >> ss;
            CHECK(ss.str() ==
                  "[\n    \"foo\",\n    1,\n    2,\n    3,\n    false,\n    {\n        \"one\": 1\n    }\n]");
        }

        SECTION("given fill")
        {
            std::stringstream ss;
            json j = {"foo", 1, 2, 3, false, {{"one", 1}}};
            ss.width(1);
            ss.fill('\t');
            j >> ss;
            CHECK(ss.str() ==
                  "[\n\t\"foo\",\n\t1,\n\t2,\n\t3,\n\tfalse,\n\t{\n\t\t\"one\": 1\n\t}\n]");
        }
    }

    SECTION("dump")
    {
        SECTION("invalid character")
        {
            json j = "ä\xA9ü";

            CHECK_THROWS_AS(j.dump(), json::type_error&);
            CHECK_THROWS_WITH(j.dump(), "[json.exception.type_error.316] invalid UTF-8 byte at index 2: 0xA9");
            CHECK_THROWS_AS(j.dump(1, ' ', false, json::error_handler_t::strict), json::type_error&);
            CHECK_THROWS_WITH(j.dump(1, ' ', false, json::error_handler_t::strict), "[json.exception.type_error.316] invalid UTF-8 byte at index 2: 0xA9");
            CHECK(j.dump(-1, ' ', false, json::error_handler_t::ignore) == "\"äü\"");
            CHECK(j.dump(-1, ' ', false, json::error_handler_t::replace) == "\"ä\xEF\xBF\xBDü\"");
            CHECK(j.dump(-1, ' ', true, json::error_handler_t::replace) == "\"\\u00e4\\ufffd\\u00fc\"");
        }

        SECTION("ending with incomplete character")
        {
            json j = "123\xC2";

            CHECK_THROWS_AS(j.dump(), json::type_error&);
            CHECK_THROWS_WITH(j.dump(), "[json.exception.type_error.316] incomplete UTF-8 string; last byte: 0xC2");
            CHECK_THROWS_AS(j.dump(1, ' ', false, json::error_handler_t::strict), json::type_error&);
            CHECK(j.dump(-1, ' ', false, json::error_handler_t::ignore) == "\"123\"");
            CHECK(j.dump(-1, ' ', false, json::error_handler_t::replace) == "\"123\xEF\xBF\xBD\"");
            CHECK(j.dump(-1, ' ', true, json::error_handler_t::replace) == "\"123\\ufffd\"");
        }

        SECTION("unexpected character")
        {
            json j = "123\xF1\xB0\x34\x35\x36";

            CHECK_THROWS_AS(j.dump(), json::type_error&);
            CHECK_THROWS_WITH(j.dump(), "[json.exception.type_error.316] invalid UTF-8 byte at index 5: 0x34");
            CHECK_THROWS_AS(j.dump(1, ' ', false, json::error_handler_t::strict), json::type_error&);
            CHECK(j.dump(-1, ' ', false, json::error_handler_t::ignore) == "\"123456\"");
            CHECK(j.dump(-1, ' ', false, json::error_handler_t::replace) == "\"123\xEF\xBF\xBD\x34\x35\x36\"");
            CHECK(j.dump(-1, ' ', true, json::error_handler_t::replace) == "\"123\\ufffd456\"");
        }

        SECTION("U+FFFD Substitution of Maximal Subparts")
        {
            // Some tests (mostly) from
            // https://www.unicode.org/versions/Unicode11.0.0/ch03.pdf
            // Section 3.9 -- U+FFFD Substitution of Maximal Subparts

            auto test = [&](std::string const & input, std::string const & expected)
            {
                json j = input;
                CHECK(j.dump(-1, ' ', true, json::error_handler_t::replace) == "\"" + expected + "\"");
            };

            test("\xC2", "\\ufffd");
            test("\xC2\x41\x42", "\\ufffd" "\x41" "\x42");
            test("\xC2\xF4", "\\ufffd" "\\ufffd");

            test("\xF0\x80\x80\x41", "\\ufffd" "\\ufffd" "\\ufffd" "\x41");
            test("\xF1\x80\x80\x41", "\\ufffd" "\x41");
            test("\xF2\x80\x80\x41", "\\ufffd" "\x41");
            test("\xF3\x80\x80\x41", "\\ufffd" "\x41");
            test("\xF4\x80\x80\x41", "\\ufffd" "\x41");
            test("\xF5\x80\x80\x41", "\\ufffd" "\\ufffd" "\\ufffd" "\x41");

            test("\xF0\x90\x80\x41", "\\ufffd" "\x41");
            test("\xF1\x90\x80\x41", "\\ufffd" "\x41");
            test("\xF2\x90\x80\x41", "\\ufffd" "\x41");
            test("\xF3\x90\x80\x41", "\\ufffd" "\x41");
            test("\xF4\x90\x80\x41", "\\ufffd" "\\ufffd" "\\ufffd" "\x41");
            test("\xF5\x90\x80\x41", "\\ufffd" "\\ufffd" "\\ufffd" "\x41");

            test("\xC0\xAF\xE0\x80\xBF\xF0\x81\x82\x41", "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\x41");
            test("\xED\xA0\x80\xED\xBF\xBF\xED\xAF\x41", "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\x41");
            test("\xF4\x91\x92\x93\xFF\x41\x80\xBF\x42", "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\x41" "\\ufffd""\\ufffd" "\x42");
            test("\xE1\x80\xE2\xF0\x91\x92\xF1\xBF\x41", "\\ufffd" "\\ufffd" "\\ufffd" "\\ufffd" "\x41");
        }
    }

    SECTION("to_string")
    {
        auto test = [&](std::string const & input, std::string const & expected)
        {
            using std::to_string;
            json j = input;
            CHECK(to_string(j) == "\"" + expected + "\"");
        };

        test("{\"x\":5,\"y\":6}", "{\\\"x\\\":5,\\\"y\\\":6}");
        test("{\"x\":[10,null,null,null]}", "{\\\"x\\\":[10,null,null,null]}");
        test("test", "test");
        test("[3,\"false\",false]", "[3,\\\"false\\\",false]");
    }
}

TEST_CASE_TEMPLATE("serialization for extreme integer values", T, int32_t, uint32_t, int64_t, uint64_t)
{
    SECTION("minimum")
    {
        constexpr auto minimum = (std::numeric_limits<T>::min)();
        json j = minimum;
        CHECK(j.dump() == std::to_string(minimum));
    }

    SECTION("maximum")
    {
        constexpr auto maximum = (std::numeric_limits<T>::max)();
        json j = maximum;
        CHECK(j.dump() == std::to_string(maximum));
    }
}

TEST_CASE("dump with binary values")
{
    auto binary = json::binary({1, 2, 3, 4});
    auto binary_empty = json::binary({});
    auto binary_with_subtype = json::binary({1, 2, 3, 4}, 128);
    auto binary_empty_with_subtype = json::binary({}, 128);

    json object = {{"key", binary}};
    json object_empty = {{"key", binary_empty}};
    json object_with_subtype = {{"key", binary_with_subtype}};
    json object_empty_with_subtype = {{"key", binary_empty_with_subtype}};

    json array = {"value", 1, binary};
    json array_empty = {"value", 1, binary_empty};
    json array_with_subtype = {"value", 1, binary_with_subtype};
    json array_empty_with_subtype = {"value", 1, binary_empty_with_subtype};

    SECTION("normal")
    {
        CHECK(binary.dump() == "{\"bytes\":[1,2,3,4],\"subtype\":null}");
        CHECK(binary_empty.dump() == "{\"bytes\":[],\"subtype\":null}");
        CHECK(binary_with_subtype.dump() == "{\"bytes\":[1,2,3,4],\"subtype\":128}");
        CHECK(binary_empty_with_subtype.dump() == "{\"bytes\":[],\"subtype\":128}");

        CHECK(object.dump() == "{\"key\":{\"bytes\":[1,2,3,4],\"subtype\":null}}");
        CHECK(object_empty.dump() == "{\"key\":{\"bytes\":[],\"subtype\":null}}");
        CHECK(object_with_subtype.dump() == "{\"key\":{\"bytes\":[1,2,3,4],\"subtype\":128}}");
        CHECK(object_empty_with_subtype.dump() == "{\"key\":{\"bytes\":[],\"subtype\":128}}");

        CHECK(array.dump() == "[\"value\",1,{\"bytes\":[1,2,3,4],\"subtype\":null}]");
        CHECK(array_empty.dump() == "[\"value\",1,{\"bytes\":[],\"subtype\":null}]");
        CHECK(array_with_subtype.dump() == "[\"value\",1,{\"bytes\":[1,2,3,4],\"subtype\":128}]");
        CHECK(array_empty_with_subtype.dump() == "[\"value\",1,{\"bytes\":[],\"subtype\":128}]");
    }

    SECTION("pretty-printed")
    {
        CHECK(binary.dump(4) == "{\n"
              "    \"bytes\": [1, 2, 3, 4],\n"
              "    \"subtype\": null\n"
              "}");
        CHECK(binary_empty.dump(4) == "{\n"
              "    \"bytes\": [],\n"
              "    \"subtype\": null\n"
              "}");
        CHECK(binary_with_subtype.dump(4) == "{\n"
              "    \"bytes\": [1, 2, 3, 4],\n"
              "    \"subtype\": 128\n"
              "}");
        CHECK(binary_empty_with_subtype.dump(4) == "{\n"
              "    \"bytes\": [],\n"
              "    \"subtype\": 128\n"
              "}");

        CHECK(object.dump(4) == "{\n"
              "    \"key\": {\n"
              "        \"bytes\": [1, 2, 3, 4],\n"
              "        \"subtype\": null\n"
              "    }\n"
              "}");
        CHECK(object_empty.dump(4) == "{\n"
              "    \"key\": {\n"
              "        \"bytes\": [],\n"
              "        \"subtype\": null\n"
              "    }\n"
              "}");
        CHECK(object_with_subtype.dump(4) == "{\n"
              "    \"key\": {\n"
              "        \"bytes\": [1, 2, 3, 4],\n"
              "        \"subtype\": 128\n"
              "    }\n"
              "}");
        CHECK(object_empty_with_subtype.dump(4) == "{\n"
              "    \"key\": {\n"
              "        \"bytes\": [],\n"
              "        \"subtype\": 128\n"
              "    }\n"
              "}");

        CHECK(array.dump(4) == "[\n"
              "    \"value\",\n"
              "    1,\n"
              "    {\n"
              "        \"bytes\": [1, 2, 3, 4],\n"
              "        \"subtype\": null\n"
              "    }\n"
              "]");
        CHECK(array_empty.dump(4) == "[\n"
              "    \"value\",\n"
              "    1,\n"
              "    {\n"
              "        \"bytes\": [],\n"
              "        \"subtype\": null\n"
              "    }\n"
              "]");
        CHECK(array_with_subtype.dump(4) == "[\n"
              "    \"value\",\n"
              "    1,\n"
              "    {\n"
              "        \"bytes\": [1, 2, 3, 4],\n"
              "        \"subtype\": 128\n"
              "    }\n"
              "]");
        CHECK(array_empty_with_subtype.dump(4) == "[\n"
              "    \"value\",\n"
              "    1,\n"
              "    {\n"
              "        \"bytes\": [],\n"
              "        \"subtype\": 128\n"
              "    }\n"
              "]");
    }
}
