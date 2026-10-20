#include <gtest/gtest.h>

#include <Processors/Formats/Impl/TabSeparatedRowInputFormat.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

namespace
{

/// A ReadBuffer that serves a fixed payload once, then throws a parse-classified
/// error from nextImpl(). ReadBuffer::next() catches any throw from nextImpl(),
/// calls cancel() (setting canceled = true) and rethrows. This models the real
/// production case where an underlying read fails mid-parse (e.g. a malformed
/// HTTP chunk header, a truncated fixed-length body) and self-cancels the buffer.
class ThrowingReadBuffer : public ReadBuffer
{
public:
    explicit ThrowingReadBuffer(std::string payload_)
        : ReadBuffer(nullptr, 0), payload(std::move(payload_))
    {
    }

private:
    bool nextImpl() override
    {
        if (!served)
        {
            served = true;
            BufferBase::set(payload.data(), payload.size(), 0);
            return true;
        }
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected EOF (simulated mid-parse failure)");
    }

    std::string payload;
    bool served = false;
};

/// Keep allocated capacity larger than the readable chunk, as for a short final file buffer.
class ReadBufferWithSpareCapacity : public ReadBuffer
{
public:
    explicit ReadBufferWithSpareCapacity(std::string input) : ReadBuffer(nullptr, 0), storage(std::move(input))
    {
        const size_t valid_size = storage.size();
        storage.resize(valid_size + 64, '#');
        BufferBase::set(storage.data(), storage.size(), 0);
        buffer().resize(valid_size);
    }

private:
    bool nextImpl() override { return false; }
    std::string storage;
};

}

/// Regression test for the "ReadBuffer is canceled. Can't read from it." abort in
/// RowInputFormatWithDiagnosticInfo::getDiagnosticAndRawDataImpl().
///
/// Sequence: a row parse reads a partial row, then next() is called for the rest,
/// the underlying read throws a parse-classified error and self-cancels the buffer.
/// IRowInputFormat::read() catches the parse error and calls getDiagnosticInfo() to
/// enrich the message; the diagnostics code called eof() (-> next()) on the now
/// canceled buffer, tripping chassert(!isCanceled()) and aborting in assertion-enabled
/// builds. The fix guards with in->isCanceled() so it must throw a normal exception
/// instead of aborting.
TEST(RowInputFormatDiagnostics, CanceledBufferDoesNotAbort)
{
    Block header;
    header.insert({ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "a"});
    header.insert({ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "b"});
    auto shared_header = std::make_shared<const Block>(header);

    /// One complete row, then a partial row so the parser calls next() mid-row.
    ThrowingReadBuffer buf("1\t2\n3\t");

    RowInputFormatParams params;
    params.max_block_size_rows = 100;

    FormatSettings format_settings;
    TabSeparatedRowInputFormat format(
        shared_header, buf, params,
        /* with_names_ = */ false, /* with_types_ = */ false, /* is_raw = */ false,
        format_settings);

    /// Must throw a regular exception, not abort the process.
    EXPECT_THROW(format.read(), Exception);
}

/// Regression test for the second reachable path to the same "ReadBuffer is canceled"
/// abort, this time in IRowInputFormat::read()'s error-skipping branch.
///
/// With allow_errors_num / allow_errors_ratio set, read() catches the parse error inside
/// the row loop (before it reaches the diagnostics guard) and calls syncAfterError() to
/// skip to the next row. For TSV/CSV/template that starts with skipTo*NextLineOrEOF, whose
/// first buf.eof() calls next() on the now canceled buffer, tripping the same
/// chassert(!isCanceled()). The fix rethrows when the buffer is already canceled instead
/// of trying to skip, so a regular exception is thrown rather than aborting.
TEST(RowInputFormatDiagnostics, CanceledBufferDoesNotAbortWithAllowErrors)
{
    Block header;
    header.insert({ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "a"});
    header.insert({ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "b"});
    auto shared_header = std::make_shared<const Block>(header);

    /// One complete row, then a partial row so the parser calls next() mid-row.
    ThrowingReadBuffer buf("1\t2\n3\t");

    RowInputFormatParams params;
    params.max_block_size_rows = 100;
    /// Enable error skipping so read() takes the syncAfterError() path, not the diagnostics one.
    params.allow_errors_num = 10;
    params.allow_errors_ratio = 1.0;

    FormatSettings format_settings;
    TabSeparatedRowInputFormat format(
        shared_header, buf, params,
        /* with_names_ = */ false, /* with_types_ = */ false, /* is_raw = */ false,
        format_settings);

    /// Must throw a regular exception, not abort the process.
    EXPECT_THROW(format.read(), Exception);
}

/// The final invalid row must have the same per-column diagnostic with or without
/// a terminating newline. Test both formats and a preceding valid row.
TEST(RowInputFormatDiagnostics, InvalidLastRowWithoutTrailingNewline)
{
    Block header;
    header.insert({ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "id"});
    header.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "name"});
    const auto shared_header = std::make_shared<const Block>(header);

    RowInputFormatParams params;
    params.max_block_size_rows = 100;
    FormatSettings format_settings;

    for (bool csv : {false, true})
    {
        for (bool preceding_row : {false, true})
        {
            for (bool trailing_newline : {false, true})
            {
                std::string payload;
                if (preceding_row)
                    payload = csv ? "1,Alice\n" : "1\tAlice\n";
                payload += csv ? "x,Bob" : "x\tBob";
                if (trailing_newline)
                    payload += '\n';

                auto check_payload = [&](ReadBuffer & buf)
                {
                    auto check_diagnostic = [&](auto & format)
                    {
                        try
                        {
                            format.read();
                            ADD_FAILURE() << "Expected a parse error for " << payload;
                        }
                        catch (const Exception & e)
                        {
                            const auto & message = e.message();
                            EXPECT_NE(message.find(preceding_row ? "Row 2" : "Row 1"), std::string::npos);
                            EXPECT_NE(message.find("Column 0"), std::string::npos);
                            EXPECT_NE(message.find("type: UInt8"), std::string::npos);
                            EXPECT_EQ(message.find("Could not print diagnostic info"), std::string::npos);
                        }
                    };

                    if (csv)
                    {
                        CSVRowInputFormat format(shared_header, buf, params, false, false, format_settings);
                        check_diagnostic(format);
                    }
                    else
                    {
                        TabSeparatedRowInputFormat format(shared_header, buf, params, false, false, false, format_settings);
                        check_diagnostic(format);
                    }
                };

                ReadBufferFromString buf(payload);
                check_payload(buf);
                if (!trailing_newline)
                {
                    ReadBufferWithSpareCapacity spare_buf(payload);
                    check_payload(spare_buf);
                }
            }
        }
    }
}
