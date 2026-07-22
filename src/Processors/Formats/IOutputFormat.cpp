#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>
#include <Processors/Formats/Framing/IFramingFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Port.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <base/sleep.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace FailPoints
{
    extern const char output_format_sleep_on_progress[];
}

IOutputFormat::IOutputFormat(SharedHeader header_, WriteBuffer & out_)
    : IProcessor({header_, header_, header_}, {}), out(out_)
{
}

IOutputFormat::Status IOutputFormat::prepare()
{
    if (has_input)
        return Status::Ready;

    for (auto kind : {Main, Totals, Extremes})
    {
        auto & input = getPort(kind);

        if (kind != Main && !input.isConnected())
            continue;

        if (input.isFinished())
            continue;

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        current_chunk = input.pull(true);
        current_block_kind = kind;
        has_input = true;
        return Status::Ready;
    }

    finished = true;

    if (!finalized)
        return Status::Ready;

    return Status::Finished;
}

static Chunk prepareTotals(Chunk chunk)
{
    if (!chunk.hasRows())
        return {};

    if (chunk.getNumRows() > 1)
    {
        /// This may happen if something like ARRAY JOIN was executed on totals.
        /// Skip rows except the first one.
        auto columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
    }

    return chunk;
}

void IOutputFormat::writeProgressIfNeededUnlocked()
{
    if (!has_progress_update_to_write)
        return;

    if (framing)
        framing->onProgress(statistics.progress);
    else
        writeProgress(statistics.progress);

    has_progress_update_to_write = false;
}

void IOutputFormat::writeFramingPayloadBoundary(FramedPacketKind kind)
{
    /// Drain format-owned buffers into the framing payload before taking the boundary
    /// (see the declaration for the rationale). `out` is the framing's payload buffer here,
    /// so the `out.next()` inside `flushImpl` is a cheap no-op on a string buffer.
    flushImpl();
    framing->onPayload(kind);
}

void IOutputFormat::work()
{
    std::lock_guard lock(writing_mutex);

    writeProgressIfNeededUnlocked();

    writePrefixIfNeeded();

    if (finished && !finalized)
    {
        if (rows_before_limit_counter && rows_before_limit_counter->hasAppliedStep())
            setRowsBeforeLimit(rows_before_limit_counter->get());
        if (rows_before_aggregation_counter && rows_before_aggregation_counter->hasAppliedStep())
            setRowsBeforeAggregation(rows_before_aggregation_counter->get());

        finalizeUnlocked();
        return;
    }

    switch (current_block_kind)
    {
        case Main:
            result_rows += current_chunk.getNumRows();
            result_bytes += current_chunk.allocatedBytes();
            consume(std::move(current_chunk));
            if (framing)
                writeFramingPayloadBoundary(FramedPacketKind::Data);
            break;
        case Totals:
            writeSuffixIfNeeded();
            if (framing)
                writeFramingPayloadBoundary(FramedPacketKind::Data);
            if (auto totals = prepareTotals(std::move(current_chunk)))
            {
                consumeTotals(std::move(totals));
                are_totals_written = true;
                if (framing)
                    writeFramingPayloadBoundary(FramedPacketKind::Totals);
            }
            break;
        case Extremes:
            writeSuffixIfNeeded();
            if (framing)
                writeFramingPayloadBoundary(FramedPacketKind::Data);
            consumeExtremes(std::move(current_chunk));
            if (framing)
                writeFramingPayloadBoundary(FramedPacketKind::Extremes);
            break;
    }

    if (auto_flush)
        flushImpl();

    has_input = false;
}

void IOutputFormat::flushImpl()
{
    out.next();

    /// If output is a compressed buffer, we will flush the compressed chunk as well.
    if (auto * out_with_nested = dynamic_cast<WriteBufferWithOwnMemoryDecorator *>(&out))
        out_with_nested->getNestedBuffer()->next();
}

void IOutputFormat::flush()
{
    std::lock_guard lock(writing_mutex);
    flushImpl();
}

void IOutputFormat::write(const Block & block)
{
    std::lock_guard lock(writing_mutex);

    writeProgressIfNeededUnlocked();

    writePrefixIfNeeded();
    consume(Chunk(block.getColumns(), block.rows()));

    if (framing)
        writeFramingPayloadBoundary(FramedPacketKind::Data);

    if (auto_flush)
        flushImpl();
}

void IOutputFormat::finalizeUnlocked()
{
    if (finalized)
        return;

    if (framing && framing_exception_only)
    {
        /// Exception-only framing (see `setFraming`'s `for_exception`): the query failed before any
        /// output was produced, so the real output format must not contribute any bytes. Skipping its
        /// prefix, suffix and finalize keeps the payload buffer empty, so no `data` packet carrying an
        /// empty format skeleton (for example `{"meta":[],"data":[]}` for `FORMAT JSON`) is emitted
        /// before the `exception` packet. Only the framing's own auxiliary packets (logs, profile
        /// events) and the exception packet are written. `finalizeBuffers` is still called so any
        /// wrapping buffers of the unused output format are released cleanly.
        finalizeBuffers();
        framing->finalize();
        finalized = true;
        return;
    }

    writePrefixIfNeeded();

    writeProgressIfNeededUnlocked();

    writeSuffixIfNeeded();
    finalizeImpl();

    if (auto_flush)
        flushImpl();

    finalizeBuffers();

    if (framing)
    {
        if (framing_finalize_deferred)
            /// Emit the format suffix (if any) as the last data packet now, but leave the trailing
            /// logs, profile events, exception packet and stream close to the deferred
            /// `framing->finalize()` call, so logs emitted after this point are captured too.
            /// Format-owned buffers were already drained by `finalizeBuffers` above (they are
            /// finalized, so `flushImpl` must not be called here - see `writeFramingPayloadBoundary`).
            framing->onPayload(FramedPacketKind::Data);
        else
            framing->finalize();
    }

    finalized = true;
}

void IOutputFormat::finalize()
{
    std::lock_guard lock(writing_mutex);
    finalizeUnlocked();
}

void IOutputFormat::setTotals(const Block & totals)
{
    std::lock_guard lock(writing_mutex);
    writeSuffixIfNeeded();
    if (framing)
        writeFramingPayloadBoundary(FramedPacketKind::Data);
    consumeTotals(Chunk(totals.getColumns(), totals.rows()));
    are_totals_written = true;
    if (framing)
        writeFramingPayloadBoundary(FramedPacketKind::Totals);
}

void IOutputFormat::setExtremes(const Block & extremes)
{
    std::lock_guard lock(writing_mutex);
    writeSuffixIfNeeded();
    if (framing)
        writeFramingPayloadBoundary(FramedPacketKind::Data);
    consumeExtremes(Chunk(extremes.getColumns(), extremes.rows()));
    if (framing)
        writeFramingPayloadBoundary(FramedPacketKind::Extremes);
}

void IOutputFormat::onProgress(const Progress & progress)
{
    fiu_do_on(
        FailPoints::output_format_sleep_on_progress,
        {
            sleepForMilliseconds(100);
        });

    statistics.progress.incrementPiecewiseAtomically(progress);
    UInt64 elapsed_ns = statistics.watch.elapsedNanoseconds();
    statistics.progress.elapsed_ns = elapsed_ns;
    if (framing || writesProgressConcurrently())
    {
        has_progress_update_to_write = true;

        /// Do not write progress too frequently.
        if (elapsed_ns >= prev_progress_write_ns + 1000 * progress_write_frequency_us)
        {
            std::unique_lock lock(writing_mutex, std::try_to_lock);

            if (lock && has_progress_update_to_write && !finalized)
            {
                if (framing)
                {
                    framing->onProgress(statistics.progress);
                }
                else
                {
                    writeProgress(statistics.progress);
                    flushImpl();
                }
                prev_progress_write_ns = elapsed_ns;
                has_progress_update_to_write = false;
            }
        }
    }
}

void IOutputFormat::writeFinalProgress(const Progress & progress)
{
    std::lock_guard lock(writing_mutex);

    if (!framing)
        return;

    statistics.progress.incrementPiecewiseAtomically(progress);
    statistics.progress.elapsed_ns = statistics.watch.elapsedNanoseconds();

    /// The output format itself may already be finalized here (for a pulling query the pipeline
    /// finalized it before the final counters were known), so its own `finalized` guard would drop
    /// the update. Hand the accumulated progress to the framing format, which defers it to its own
    /// (separate, deferred) finalization: the query-finish logging still emits trailing `log` and
    /// `profile_events` packets after this point, and the framing writes the final `progress`
    /// packet - carrying `result_rows` / `result_bytes` / `memory_usage`, like the native
    /// protocol's final progress packet - after draining them, so a successful stream really ends
    /// with it. Also clear the pending (throttled) intermediate update, which is now folded into
    /// the final one, so a later `finalize` of this format does not write it as a stale extra
    /// `progress` packet.
    framing->setFinalProgress(statistics.progress);
    has_progress_update_to_write = false;
}

void IOutputFormat::setProgress(Progress progress)
{
    statistics.progress = std::move(progress);
}

void IOutputFormat::setFraming(const std::shared_ptr<IFramingFormat> & framing_, bool for_exception)
{
    /// Some output formats (for example `Template`) do not write totals and extremes in
    /// `consumeTotals` / `consumeExtremes`, but store them and emit them later from `finalizeImpl`.
    /// A framing format cannot tell such deferred totals/extremes apart from the main data, so it
    /// would mislabel them as `data` packets. Reject these formats instead of producing wrong output.
    /// On the exception path (`for_exception`) only the `exception` packet is written (no data,
    /// totals or extremes), so this restriction does not apply.
    if (!for_exception && areTotalsAndExtremesUsedInFinalize())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The output format {} is not compatible with framing formats, "
            "because it writes totals and extremes in a deferred way",
            getName());

    /// The `*WithProgress` output formats (`JSONEachRowWithProgress`, `JSONCompactEachRowWithProgress`)
    /// write progress as in-band rows that are part of their normal output (`writesProgressConcurrently`).
    /// A framing format routes progress to out-of-band `progress` packets instead, so those in-band rows
    /// would disappear and the concatenation of the `data` packets would no longer reproduce the unframed
    /// output. Reject such formats; use the base output format (for example `JSONEachRow`) with framing,
    /// which delivers progress as `progress` packets. On the exception path (`for_exception`) only the
    /// exception packet is written (no data or progress), so this restriction does not apply.
    if (!for_exception && writesProgressConcurrently())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The output format {} is not compatible with framing formats, "
            "because it writes progress in-band as part of its output. "
            "Use the base output format, which lets framing deliver progress as separate packets",
            getName());

    framing = framing_;
    framing_exception_only = for_exception;
}

InputPort & IOutputFormat::getPort(PortKind kind)
{
    return *std::next(inputs.begin(), kind);
}

}
