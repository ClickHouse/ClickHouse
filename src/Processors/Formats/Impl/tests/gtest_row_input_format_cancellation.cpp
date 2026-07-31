#include <thread>

#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/TabSeparatedRowInputFormat.h>
#include <Processors/Formats/Impl/ValuesBlockInputFormat.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
}

namespace
{

/// Counts how often the row loop asked whether the query was cancelled, and optionally answers
/// "cancelled" - which is what `ProcessListElement::throwIfKilled` does after a `KILL QUERY`.
struct CancelProbe
{
    size_t calls = 0;
    bool cancel = false;
};

/// The production predicate lives in `ThreadStatus::local_data`, which `ThreadGroup`'s constructor
/// fills in and `attachToGroupImpl` copies into the thread. It is `protected` and has no public
/// setter, so a test reaches it the way the class already allows: by deriving. Everything else on
/// the path stays production code - `CurrentThread::checkIfNotCancelled` calls
/// `ThreadStatus::throwIfQueryCanceled`, which returns early unless a thread group is attached and
/// otherwise invokes exactly this predicate.
class TestThreadStatus : public ThreadStatus
{
public:
    void installCancelPredicate(CancelProbe & probe)
    {
        local_data.throw_if_query_canceled_predicate = [&probe]
        {
            ++probe.calls;
            if (probe.cancel)
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled (injected by the test)");
        };
    }
};

/// Serves a payload in fixed-size pieces and records how much of it the parser has consumed.
///
/// The consumed count is what makes "aborted part way through the block" measurable. When the
/// predicate throws, `read` propagates and the partially filled columns are destroyed with it, so
/// there is no chunk to count rows in; the bytes the parser had drawn from the source is the same
/// claim seen from the input side. Pieces are small relative to the payload so the measurement does
/// not depend on how the formats' `PeekableReadBuffer` wrapper forwards a read.
class ChunkedReadBuffer : public ReadBuffer
{
public:
    ChunkedReadBuffer(std::string payload_, size_t piece_size_)
        : ReadBuffer(nullptr, 0), payload(std::move(payload_)), piece_size(piece_size_)
    {
    }

    size_t getConsumed() const { return served; }

private:
    bool nextImpl() override
    {
        if (served == payload.size())
            return false;

        const size_t piece = std::min(piece_size, payload.size() - served);
        BufferBase::set(payload.data() + served, piece, 0);
        served += piece;
        return true;
    }

    std::string payload;
    const size_t piece_size;
    size_t served = 0;
};

/// `\n`-separated integers, as TSV wants them.
std::string makeTabSeparatedPayload(size_t rows)
{
    std::string payload;
    payload.reserve(rows * 6);
    for (size_t i = 1; i <= rows; ++i)
    {
        payload += std::to_string(i);
        payload += '\n';
    }
    return payload;
}

/// `(1)\n(2)\n...`, as Values wants them.
std::string makeValuesPayload(size_t rows)
{
    std::string payload;
    payload.reserve(rows * 8);
    for (size_t i = 1; i <= rows; ++i)
    {
        payload += '(';
        payload += std::to_string(i);
        payload += ")\n";
    }
    return payload;
}

SharedHeader makeHeader()
{
    Block header{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "x")};
    return std::make_shared<const Block>(std::move(header));
}

struct ReadOutcome
{
    bool threw = false;
    int thrown_code = 0;
    size_t predicate_calls = 0;
    size_t rows_in_chunk = 0;
    size_t consumed = 0;
};

/// Runs `read` on a freshly built format in a dedicated thread, so `current_thread` starts as
/// `nullptr` regardless of what other gtests in `unit_tests_dbms` left behind (the reason
/// gtest_thread_group_switcher.cpp uses the same shape; `ThreadStatus`'s constructor asserts it).
///
/// `build_format` returns the format as an `IInputFormat`: `read` is public there and both formats
/// under test override it, one of them privately.
template <typename BuildFormat>
ReadOutcome runRead(BuildFormat && build_format, ChunkedReadBuffer & buf, bool cancel, size_t max_block_size_rows)
{
    ReadOutcome outcome;
    CancelProbe probe;
    probe.cancel = cancel;

    std::thread t([&]
    {
        TestThreadStatus thread_status;
        auto context = getContext().context;
        /// `throwIfQueryCanceled` is a no-op while the thread is detached, so the group is what
        /// makes the checkpoint live at all.
        auto thread_group = std::make_shared<ThreadGroup>(context, 0);
        CurrentThread::attachToGroup(thread_group);
        /// Attaching copied the group's own predicate in; replace it with the test's.
        thread_status.installCancelPredicate(probe);

        RowInputFormatParams params;
        params.max_block_size_rows = max_block_size_rows;

        std::shared_ptr<IInputFormat> format = build_format(params, context);

        try
        {
            outcome.rows_in_chunk = format->read().getNumRows();
        }
        catch (const Exception & e)
        {
            outcome.threw = true;
            outcome.thrown_code = e.code();
        }

        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();

    outcome.predicate_calls = probe.calls;
    outcome.consumed = buf.getConsumed();
    return outcome;
}

/// Small relative to the payloads below, so a mid-block abort leaves a wide margin.
constexpr size_t PIECE_SIZE = 512;

/// Payload for the two abort cases. The loop aborts at the first checkpoint, i.e. after
/// CANCELLATION_CHECK_PERIOD_ROWS rows, so a payload only just above the stride would leave a single
/// unparsed row - below the granularity at which any input-side measurement can resolve it. Several
/// times the stride makes the abort point a wide margin instead of a rounding question, and parsing
/// this many rows still costs a couple of milliseconds.
constexpr size_t ABORT_CASE_ROWS = CANCELLATION_CHECK_PERIOD_ROWS * 4;

/// Payload for the two `OneCheckPerStridePeriod` cases, which pin the stride's VALUE and not just its
/// existence. The two row loops carry independent copies of the period, so each one gets its own case
/// over this same payload.
/// One period plus one row is the smallest payload that reaches the first checkpoint and not the
/// second, so the number of checks it costs is sensitive to the period in both directions: halving
/// the period gives 2, doubling it gives 0. That is what an equality assertion needs.
///
/// Deliberately a literal rather than `CANCELLATION_CHECK_PERIOD_ROWS + 1`, which would be the
/// natural spelling but is inert: written in terms of the constant, the payload moves with it, every
/// period yields exactly one check, and the assertion could no longer tell one period from another.
constexpr size_t STRIDE_PERIOD_PROBE_ROWS = 8193;

}

/// The two row loops (`IRowInputFormat::read` and `ValuesBlockInputFormat::read`) each poll query
/// cancellation every `CANCELLATION_CHECK_PERIOD_ROWS` rows, so a `KILL QUERY` is observed part way
/// through parsing a single block instead of only once the whole block is done. The end-to-end
/// evidence is the shell test 04652_async_insert_flush_cancellation_inside_block; the cases below
/// pin the same checkpoint with no server, no failpoint and no global state, so a concurrent copy of
/// that test cannot perturb them.

TEST(RowInputFormatCancellation, TabSeparatedAbortsInsideOneBlock)
{
    const size_t rows = ABORT_CASE_ROWS;
    const std::string payload = makeTabSeparatedPayload(rows);

    ChunkedReadBuffer buf(payload, PIECE_SIZE);
    auto header = makeHeader();
    FormatSettings format_settings;

    const auto outcome = runRead(
        [&](const RowInputFormatParams & params, const ContextPtr &)
        {
            return std::make_shared<TabSeparatedRowInputFormat>(
                header, buf, params,
                /*with_names_=*/false, /*with_types_=*/false, /*is_raw=*/false,
                format_settings);
        },
        buf,
        /*cancel=*/true,
        /*max_block_size_rows=*/rows * 2);

    ASSERT_TRUE(outcome.threw);
    EXPECT_EQ(outcome.thrown_code, ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_GE(outcome.predicate_calls, 1u) << "the loop never asked whether the query was cancelled";
    EXPECT_LT(outcome.consumed, payload.size())
        << "the loop drew the whole payload, so the abort was not inside the block";
}

TEST(RowInputFormatCancellation, ValuesAbortsInsideOneBlock)
{
    const size_t rows = ABORT_CASE_ROWS;
    const std::string payload = makeValuesPayload(rows);

    ChunkedReadBuffer buf(payload, PIECE_SIZE);
    auto header = makeHeader();
    FormatSettings format_settings;

    const auto outcome = runRead(
        [&](const RowInputFormatParams & params, const ContextPtr & context) -> std::shared_ptr<IInputFormat>
        {
            auto format = std::make_shared<ValuesBlockInputFormat>(buf, header, params, format_settings);
            /// Values needs a context for `max_parser_depth` and the expression fallback.
            format->setContext(context);
            return format;
        },
        buf,
        /*cancel=*/true,
        /*max_block_size_rows=*/rows * 2);

    ASSERT_TRUE(outcome.threw);
    EXPECT_EQ(outcome.thrown_code, ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_GE(outcome.predicate_calls, 1u) << "the loop never asked whether the query was cancelled";
    EXPECT_LT(outcome.consumed, payload.size())
        << "the loop drew the whole payload, so the abort was not inside the block";
}

/// The non-vacuity control for the `rows != 0` guard and the modulo: a block that stays below the
/// stride must cost no cancellation checks at all. Without it, a checkpoint that ran on every row
/// would satisfy the two cases above just as well.
///
/// The guard exists twice, once per row loop, and the two copies are independent code. This case
/// covers `IRowInputFormat`'s; `ValuesShortBlockAsksNothing` below covers the one in
/// `ValuesBlockInputFormat::read`. Neither can see the other's, because deleting a row-zero guard
/// only moves the abort earlier, which the two cases above still accept.
TEST(RowInputFormatCancellation, ShortBlockAsksNothing)
{
    const size_t rows = CANCELLATION_CHECK_PERIOD_ROWS - 1;
    const std::string payload = makeTabSeparatedPayload(rows);

    ChunkedReadBuffer buf(payload, PIECE_SIZE);
    auto header = makeHeader();
    FormatSettings format_settings;

    const auto outcome = runRead(
        [&](const RowInputFormatParams & params, const ContextPtr &)
        {
            return std::make_shared<TabSeparatedRowInputFormat>(
                header, buf, params,
                /*with_names_=*/false, /*with_types_=*/false, /*is_raw=*/false,
                format_settings);
        },
        buf,
        /*cancel=*/false,
        /*max_block_size_rows=*/rows * 2);

    ASSERT_FALSE(outcome.threw);
    EXPECT_EQ(outcome.rows_in_chunk, rows);
    EXPECT_EQ(outcome.predicate_calls, 0u)
        << "a block below the stride must not pay for a cancellation check";
}

/// Pins the stride's value, not just that a stride exists. The cases above stay green for any period
/// at or below a quarter of their payload, and a block short of one period cannot distinguish
/// periods either, so without an equality assertion on a payload of exactly one period plus one row
/// nothing here would notice the constant changing. A widened period is the regression that matters:
/// it is how this bug reached production, where the only check was at the block boundary.
TEST(RowInputFormatCancellation, OneCheckPerStridePeriod)
{
    /// Non-fatal on purpose: when the period changes, the count below is the assertion that carries
    /// the regression, and this one only names why the payload has to change with it.
    EXPECT_EQ(CANCELLATION_CHECK_PERIOD_ROWS + 1, STRIDE_PERIOD_PROBE_ROWS)
        << "the payload no longer straddles exactly one period, so the count below means nothing";

    const size_t rows = STRIDE_PERIOD_PROBE_ROWS;
    const std::string payload = makeTabSeparatedPayload(rows);

    ChunkedReadBuffer buf(payload, PIECE_SIZE);
    auto header = makeHeader();
    FormatSettings format_settings;

    const auto outcome = runRead(
        [&](const RowInputFormatParams & params, const ContextPtr &)
        {
            return std::make_shared<TabSeparatedRowInputFormat>(
                header, buf, params,
                /*with_names_=*/false, /*with_types_=*/false, /*is_raw=*/false,
                format_settings);
        },
        buf,
        /*cancel=*/false,
        /*max_block_size_rows=*/rows * 2);

    ASSERT_FALSE(outcome.threw);
    EXPECT_EQ(outcome.rows_in_chunk, rows);
    EXPECT_EQ(outcome.predicate_calls, 1u)
        << "the row loop no longer asks exactly once per " << CANCELLATION_CHECK_PERIOD_ROWS << " rows";
}

/// Same control for the `rows_in_block != 0` guard in `ValuesBlockInputFormat::read`.
TEST(RowInputFormatCancellation, ValuesShortBlockAsksNothing)
{
    const size_t rows = CANCELLATION_CHECK_PERIOD_ROWS - 1;
    const std::string payload = makeValuesPayload(rows);

    ChunkedReadBuffer buf(payload, PIECE_SIZE);
    auto header = makeHeader();
    FormatSettings format_settings;

    const auto outcome = runRead(
        [&](const RowInputFormatParams & params, const ContextPtr & context) -> std::shared_ptr<IInputFormat>
        {
            auto format = std::make_shared<ValuesBlockInputFormat>(buf, header, params, format_settings);
            format->setContext(context);
            return format;
        },
        buf,
        /*cancel=*/false,
        /*max_block_size_rows=*/rows * 2);

    ASSERT_FALSE(outcome.threw);
    EXPECT_EQ(outcome.rows_in_chunk, rows);
    EXPECT_EQ(outcome.predicate_calls, 0u)
        << "a block below the stride must not pay for a cancellation check";
}

/// Same equality assertion for the period in `ValuesBlockInputFormat::read`. `OneCheckPerStridePeriod`
/// above cannot see it: it builds a `TabSeparatedRowInputFormat`, so it pins only the period in
/// `IRowInputFormat::read`. The two conditions are separate code with separate literals in the modulo,
/// and neither Values case above can distinguish periods either - one asks for at least one check over
/// four periods, the other for none over less than one - so widening the Values period alone would
/// leave every test green while breaking the bound this fix publishes.
TEST(RowInputFormatCancellation, ValuesOneCheckPerStridePeriod)
{
    /// Duplicated from `OneCheckPerStridePeriod` rather than shared, so that each case still names why
    /// its own count is meaningful if the other one is ever removed. Non-fatal for the same reason.
    EXPECT_EQ(CANCELLATION_CHECK_PERIOD_ROWS + 1, STRIDE_PERIOD_PROBE_ROWS)
        << "the payload no longer straddles exactly one period, so the count below means nothing";

    const size_t rows = STRIDE_PERIOD_PROBE_ROWS;
    const std::string payload = makeValuesPayload(rows);

    ChunkedReadBuffer buf(payload, PIECE_SIZE);
    auto header = makeHeader();
    FormatSettings format_settings;

    const auto outcome = runRead(
        [&](const RowInputFormatParams & params, const ContextPtr & context) -> std::shared_ptr<IInputFormat>
        {
            auto format = std::make_shared<ValuesBlockInputFormat>(buf, header, params, format_settings);
            format->setContext(context);
            return format;
        },
        buf,
        /*cancel=*/false,
        /*max_block_size_rows=*/rows * 2);

    ASSERT_FALSE(outcome.threw);
    EXPECT_EQ(outcome.rows_in_chunk, rows);
    EXPECT_EQ(outcome.predicate_calls, 1u)
        << "the Values row loop no longer asks exactly once per " << CANCELLATION_CHECK_PERIOD_ROWS << " rows";
}
