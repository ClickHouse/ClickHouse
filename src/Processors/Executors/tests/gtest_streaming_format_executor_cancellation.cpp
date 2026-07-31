#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/EmptyReadBuffer.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
extern const int TIMEOUT_EXCEEDED;
extern const int CANNOT_PARSE_NUMBER;
}

namespace
{

SharedHeader makeHeader()
{
    Block header{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "x")};
    return std::make_shared<const Block>(std::move(header));
}

/// An input format whose first `read` throws a chosen error code, so a test can pick which arm of
/// `StreamingFormatExecutor::execute`'s `catch (Exception &)` is taken without any parsing or I/O.
/// `on_read` runs just before the throw; a test uses it to flip the cancellation flag, modelling a
/// `KILL` that lands while the block is being parsed.
///
/// A real `ReadBuffer` is required even though nothing is read: `IInputFormat::generate` catches the
/// exception and calls `getReadBuffer`, whose `chassert(in)` aborts on a null buffer in debug and
/// sanitizer builds.
class ThrowingInputFormat : public IInputFormat
{
public:
    ThrowingInputFormat(SharedHeader header, ReadBuffer & buf, int code_, std::function<void()> on_read_)
        : IInputFormat(std::move(header), &buf), code(code_), on_read(std::move(on_read_))
    {
    }

    String getName() const override { return "ThrowingInputFormat"; }

    Chunk read() override
    {
        ++read_calls;
        if (on_read)
            on_read();
        throw Exception(code, "Injected failure from the test input format");
    }

    size_t getReadCalls() const { return read_calls; }

private:
    const int code;
    const std::function<void()> on_read;
    size_t read_calls = 0;
};

enum class CancelMode
{
    /// The executor is built without a cancellation callback: the shape of the five streaming-engine
    /// callers (Kafka, Kafka2, FileLog, NATS, RabbitMQ), which pass only the first three arguments.
    /// With no callback the classification rests on the error code alone.
    NotInstalled,
    /// A callback is installed and reports "not cancelled" until `read` is entered, then "cancelled":
    /// what `query_status->isKilled` does for an async-insert flush that is killed part way through
    /// parsing one block. Reporting cancelled from the start would instead be caught by the loop's own
    /// pre-chunk check, which throws QUERY_WAS_CANCELLED before `read` is ever called - the code arm
    /// would then decide the outcome and the callback arm would go untested.
    CancelledDuringRead,
};

struct ExecuteResult
{
    bool threw = false;
    int thrown_code = 0;
    size_t on_error_calls = 0;
    size_t read_calls = 0;
};

ExecuteResult runExecutor(int throw_code, CancelMode cancel_mode)
{
    ExecuteResult result;
    bool cancelled = false;

    EmptyReadBuffer buf;
    auto header = makeHeader();

    std::function<void()> on_read;
    if (cancel_mode == CancelMode::CancelledDuringRead)
        on_read = [&cancelled] { cancelled = true; };

    auto format = std::make_shared<ThrowingInputFormat>(header, buf, throw_code, std::move(on_read));

    StreamingFormatExecutor::ErrorCallback on_error
        = [&result](const MutableColumns &, const ColumnCheckpoints &, Exception &) -> size_t
    {
        ++result.on_error_calls;
        return 0;
    };

    StreamingFormatExecutor::CancelCallback is_cancelled;
    if (cancel_mode == CancelMode::CancelledDuringRead)
        is_cancelled = [&cancelled] { return cancelled; };

    StreamingFormatExecutor executor(
        *header,
        format,
        std::move(on_error),
        /*total_bytes_=*/0,
        /*total_chunks_=*/0,
        /*adding_defaults_transform_=*/nullptr,
        std::move(is_cancelled));

    try
    {
        executor.execute();
    }
    catch (const Exception & e)
    {
        result.threw = true;
        result.thrown_code = e.code();
    }

    result.read_calls = format->getReadCalls();
    return result;
}

}

/// `StreamingFormatExecutor::execute` must not route a cancellation to `on_error`: for an
/// async-insert flush `on_error` records the exception against the entry as a parsing failure and
/// carries on, so the flush would report success with zero rows instead of failing. Since the row
/// loops now abort part way through a block, the classification has two independent arms (the
/// QUERY_WAS_CANCELLED code and the cancellation callback), and each test below isolates one of them
/// while pinning that no other error code is read as cancellation. The shell test
/// 04652_async_insert_flush_cancellation_inside_block covers the row loops end to end but cannot
/// separate the arms, because a killed query satisfies more than one at once.

/// `TIMEOUT_EXCEEDED` must NOT be read as cancellation on the strength of its code alone. A caller
/// that installs no cancellation callback (the shape of the five streaming-engine callers) can see
/// that code from a single message exceeding the query deadline in place, with the query still
/// running: `QueryStatus::checkTimeLimit` falls through to `ExecutionSpeedLimits::checkTimeLimit`,
/// which throws it directly under the default `timeout_overflow_mode = throw` without the query ever
/// being cancelled, and `ValuesBlockInputFormat::parseExpression` evaluates arbitrary constant
/// expressions that reach exactly that (`arrayFold`, `FunctionBaseXXConversion`). Routing it past
/// `on_error` would abort the whole consumer instead of producing `_error` or a dead-letter row.
TEST(StreamingFormatExecutorCancellation, RoutesTimeoutToOnErrorWithoutCancelCallback)
{
    const auto result = runExecutor(ErrorCodes::TIMEOUT_EXCEEDED, CancelMode::NotInstalled);

    EXPECT_EQ(result.read_calls, 1u) << "the format must have been reached, otherwise the case is vacuous";
    EXPECT_FALSE(result.threw);
    EXPECT_EQ(result.on_error_calls, 1u);
}

/// The other half of the same contract: once a callback IS installed and reports cancelled, a
/// deadline that surfaces as TIMEOUT_EXCEEDED mid-parse is a cancellation and must be rethrown. This
/// is the async-insert flush shape, and it is what keeps `max_execution_time` honoured mid-block.
TEST(StreamingFormatExecutorCancellation, RethrowsTimeoutWhenCancelCallbackReportsCancelled)
{
    const auto result = runExecutor(ErrorCodes::TIMEOUT_EXCEEDED, CancelMode::CancelledDuringRead);

    EXPECT_EQ(result.read_calls, 1u) << "the pre-chunk check must not have fired before the format ran";
    EXPECT_TRUE(result.threw);
    EXPECT_EQ(result.thrown_code, ErrorCodes::TIMEOUT_EXCEEDED);
    EXPECT_EQ(result.on_error_calls, 0u);
}

/// A query cancelled while the block was being parsed can report a code that is not
/// QUERY_WAS_CANCELLED, so the executor asks the cancellation callback as well. Isolated here with a
/// parse-error code, which the code arm does not match: only the callback arm can keep this out of
/// `on_error`.
TEST(StreamingFormatExecutorCancellation, RethrowsParseErrorCodeWhenCancelCallbackReportsCancelled)
{
    const auto result = runExecutor(ErrorCodes::CANNOT_PARSE_NUMBER, CancelMode::CancelledDuringRead);

    EXPECT_EQ(result.read_calls, 1u) << "the pre-chunk check must not have fired before the format ran";
    EXPECT_TRUE(result.threw);
    EXPECT_EQ(result.thrown_code, ErrorCodes::CANNOT_PARSE_NUMBER);
    EXPECT_EQ(result.on_error_calls, 0u);
}

/// The control: with the same parse-error code and nothing reporting cancellation, the exception must
/// still reach `on_error`. Without this the two tests above would also pass if `on_error` had become
/// unreachable in this harness.
TEST(StreamingFormatExecutorCancellation, RoutesParseErrorToOnErrorWhenNotCancelled)
{
    const auto result = runExecutor(ErrorCodes::CANNOT_PARSE_NUMBER, CancelMode::NotInstalled);

    EXPECT_EQ(result.read_calls, 1u);
    EXPECT_FALSE(result.threw);
    EXPECT_EQ(result.on_error_calls, 1u);
}

/// Regression guard for the arm that predates this change: an explicit QUERY_WAS_CANCELLED is
/// rethrown on the strength of its code alone.
TEST(StreamingFormatExecutorCancellation, RethrowsQueryWasCancelledWithoutCancelCallback)
{
    const auto result = runExecutor(ErrorCodes::QUERY_WAS_CANCELLED, CancelMode::NotInstalled);

    EXPECT_EQ(result.read_calls, 1u);
    EXPECT_TRUE(result.threw);
    EXPECT_EQ(result.thrown_code, ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_EQ(result.on_error_calls, 0u);
}
