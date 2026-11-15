#include <gtest/gtest.h>

#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPResponseParser.h>
#include <Poco/Net/NetException.h>

#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>


using Poco::Net::HTTPHeaderLimits;
using Poco::Net::HTTPResponse;
using Poco::Net::HTTPResponseParser;

namespace
{

/// What a parse produced: the response and the number of bytes it consumed, or the exception.
struct ParseResult
{
    std::string error;
    size_t consumed = 0;
    std::string version;
    int status = 0;
    std::string reason;
    std::vector<std::pair<std::string, std::string>> fields;

    bool operator==(const ParseResult &) const = default;
};

[[maybe_unused]] void PrintTo(const ParseResult & result, std::ostream * out)
{
    if (!result.error.empty())
    {
        *out << "error: " << result.error;
        return;
    }
    *out << "consumed " << result.consumed << ", '" << result.version << "' " << result.status << " '" << result.reason << "'";
    for (const auto & [name, value] : result.fields)
        *out << ", " << name << ": '" << value << "'";
}

ParseResult fromResponse(const HTTPResponse & response, size_t consumed)
{
    ParseResult result;
    result.consumed = consumed;
    result.version = response.getVersion();
    result.status = response.getStatus();
    result.reason = response.getReason();
    for (const auto & [name, value] : response)
        result.fields.emplace_back(name, value);
    return result;
}

ParseResult fromException(const Poco::Exception & e)
{
    ParseResult result;
    result.error = std::string(e.className()) + ": " + e.displayText();
    return result;
}

/// The reference: `Poco::Net::HTTPClientSession::receiveResponse`, which calls
/// `HTTPResponse::read` until the status is not `100`, each time on a new stream that ends where
/// the input ends.
ParseResult parseWithPoco(const std::string & input, const HTTPHeaderLimits & limits)
{
    HTTPResponse response;
    response.setFieldLimit(limits.fields);
    response.setNameLengthLimit(limits.name_length);
    response.setValueLengthLimit(limits.value_length);
    size_t position = 0;
    try
    {
        do
        {
            std::istringstream in(input.substr(position));
            response.clear();
            response.read(in);
            const auto end = in.tellg();
            position = end < 0 ? input.size() : position + static_cast<size_t>(end);
        } while (response.getStatus() == HTTPResponse::HTTP_CONTINUE);
    }
    catch (const Poco::Exception & e)
    {
        return fromException(e);
    }
    return fromResponse(response, position);
}

/// Feeds `input` to the parser in pieces that end at `ends`, and then reports the end of the data.
ParseResult parseInPieces(const std::string & input, const std::vector<size_t> & ends, const HTTPHeaderLimits & limits)
{
    HTTPResponseParser parser(limits);
    size_t consumed = 0;
    try
    {
        for (const size_t end : ends)
        {
            if (parser.complete())
                break;
            const size_t size = end - consumed;
            const size_t taken = parser.feed(input.data() + consumed, size);
            /// Until the header is complete, the parser takes everything it is given.
            if (!parser.complete())
                EXPECT_EQ(taken, size);
            consumed += taken;
        }
        if (!parser.complete())
            parser.feedEof();
    }
    catch (const Poco::Exception & e)
    {
        return fromException(e);
    }
    EXPECT_TRUE(parser.complete());
    return fromResponse(parser.response(), consumed);
}

const HTTPHeaderLimits default_limits;
const HTTPHeaderLimits small_limits{.fields = 3, .name_length = 8, .value_length = 16};
const HTTPHeaderLimits large_limits{.fields = 1000, .name_length = 1024, .value_length = 128 * 1024};

const std::vector<std::string> inputs = {
    "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Amz-Id: abc\r\n\r\nhello",
    "\r\n\r\nHTTP/1.1 404 Not Found\r\nA: 1\r\n\r\n",
    "HTTP/1.1 200 OK\nA: 1\nB:2\n\nbody",
    "HTTP/1.1 200 OK\r\nA: first\r\n second\r\n\tthird\r\nB: x\r\n\r\n",
    "HTTP/1.1 200 OK\r\ninvalid line\r\nA: 1\r\n\r\n",
    "HTTP/1.1 200 OK\r\nA:   spaced value   \r\n\r\n",
    "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n\r\n",
    "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 404 Not Found\r\nA: 1\r\n\r\nbody",
    "HTTP/1.1 100 Continue\r\nA: 1\r\nB: 2\r\nC: 3\r\n\r\nHTTP/1.1 200 OK\r\nA: 1\r\nB: 2\r\nC: 3\r\n\r\n",
    "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nA: 1\r\nB: 2\r\nC: 3\r\nD: 4\r\n\r\n",
    "HTTP/1.1 103 Early Hints\r\nLink: x\r\n\r\nHTTP/1.1 200 OK\r\n\r\n",
    "HTTP/1.1 100 Continue\r\n\r\n",
    "HTTP/1.1 100 Continue\r\nA: 1",
    "HTTP/1.1 100 Continue\r\n\r\n\r\n",
    "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 20",
    "HTTP/1.1 204\r\n\r\n",
    "HTTP/1.1  503   Slow Down \r\nRetry-After: 1\r\n\r\n",
    "HTTP/1.1 200 OK\r\nA: =?UTF-8?B?aGVsbG8=?=\r\n\r\n",
    "HTTP/1.1 200 OK\r\n\rgarbage\r\nbody",
    "HTTP/1.1 200 OK\r\nA: " + std::string(9000, 'v') + "\r\n\r\n",
    "HTTP/1.1 200 OK\r\n" + std::string(300, 'n') + ": 1\r\n\r\n",
    "HTTP/1.1 200 " + std::string(600, 'r') + "\r\n\r\n",
    [] { std::string s = "HTTP/1.1 200 OK\r\n"; for (int i = 0; i < 101; ++i) s += "F" + std::to_string(i) + ": v\r\n"; return s + "\r\n"; }(),
    "HTTP/1.1 2000 OK\r\n\r\n",
    "HTTP/1.1.1.1 200 OK\r\n\r\n",
    "HTTP/1.1 abc OK\r\n\r\n",
    "HTTP/1.1 200 OK\rX\n\r\n",
    "",
    "\r\n",
    "HTTP/1.1",
    "HTTP/1.1 20",
    "HTTP/1.1 200 O",
    "HTTP/1.1 200 OK\r",
    "HTTP/1.1 200 OK\r\nA",
    "HTTP/1.1 200 OK\r\nA:",
    "HTTP/1.1 200 OK\r\nA: b",
    "HTTP/1.1 200 OK\r\nA: b\r",
    "HTTP/1.1 200 OK\r\nA: b\r\n folded",
    "HTTP/1.1 200 OK\r\nA: b\r\n\r",
};

}


/// Wherever the data is split, the parser gives what `Poco` gives for the same bytes: the same
/// response, the same number of consumed bytes, or the same exception.
TEST(HTTPResponseParser, MatchesPocoForEverySplit)
{
    for (const auto & limits : {default_limits, small_limits, large_limits})
    {
        for (const auto & input : inputs)
        {
            SCOPED_TRACE(input.substr(0, 100));
            const auto expected = parseWithPoco(input, limits);

            for (size_t split = 0; split <= input.size(); ++split)
                ASSERT_EQ(parseInPieces(input, {split, input.size()}, limits), expected) << "split at " << split;

            std::vector<size_t> every_byte;
            for (size_t end = 1; end <= input.size(); ++end)
                every_byte.push_back(end);
            ASSERT_EQ(parseInPieces(input, every_byte, limits), expected) << "byte by byte";
        }
    }
}

/// The limits hold while the header has not ended yet, so the peer cannot make the parser keep
/// an unbounded amount of state: the parser throws before the end of the data.
TEST(HTTPResponseParser, LimitsHoldBeforeTheHeaderEnds)
{
    const auto feed_unterminated = [](const std::string & data)
    {
        HTTPResponseParser parser(small_limits);
        parser.feed(data.data(), data.size());
    };

    EXPECT_THROW(feed_unterminated("HTTP/1.1 404 Not Found\r\nA: 1\r\nB: 2\r\nC: 3\r\nD"), Poco::Net::MessageException);
    EXPECT_THROW(feed_unterminated("HTTP/1.1 404 Not Found\r\nA: 1234567890\r\n 1234567890"), Poco::Net::MessageException);
    EXPECT_THROW(feed_unterminated("HTTP/1.1 100 Continue\r\nA: 1\r\nB: 2\r\nC: 3\r\nD"), Poco::Net::MessageException);
}

/// The status line is available before the rest of the header arrives.
TEST(HTTPResponseParser, StatusLineBeforeTheHeaderEnds)
{
    HTTPResponseParser parser;
    const std::string data = "HTTP/1.1 403 Forbidden\r\nContent-Type: application/xml\r\nContent-Le";
    EXPECT_EQ(parser.feed(data.data(), data.size()), data.size());
    EXPECT_TRUE(parser.statusLineComplete());
    EXPECT_FALSE(parser.complete());
    EXPECT_EQ(parser.response().getStatus(), HTTPResponse::HTTP_FORBIDDEN);
}

/// After a `100 Continue`, the parser waits for the status line of the final response.
TEST(HTTPResponseParser, ContinueIsDropped)
{
    HTTPResponseParser parser;
    const std::string data = "HTTP/1.1 100 Continue\r\nA: 1\r\n\r\nHTTP/1.1 4";
    EXPECT_EQ(parser.feed(data.data(), data.size()), data.size());
    EXPECT_FALSE(parser.statusLineComplete());
    EXPECT_FALSE(parser.complete());
    EXPECT_FALSE(parser.response().has("A"));
}

/// After `reset`, the parser reads the next response with the same limits.
TEST(HTTPResponseParser, ResetKeepsTheLimits)
{
    HTTPResponseParser parser(small_limits);
    const std::string first = "HTTP/1.1 200 OK\r\n\r\n";
    const std::string second = "HTTP/1.1 200 OK\r\nA: 1\r\nB: 2\r\nC: 3\r\nD: 4\r\n\r\n";

    parser.feed(first.data(), first.size());
    ASSERT_TRUE(parser.complete());

    parser.reset();
    EXPECT_FALSE(parser.statusLineComplete());
    EXPECT_THROW(parser.feed(second.data(), second.size()), Poco::Net::MessageException);
}
