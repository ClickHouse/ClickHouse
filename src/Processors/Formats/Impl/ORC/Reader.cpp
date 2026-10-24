#include <Processors/Formats/Impl/ORC/Reader.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>

#include <algorithm>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::ORC
{

std::future<void> InputStream::readAsync(void * buf, UInt64 length, UInt64 offset)
{
    read(buf, length, offset);
    std::promise<void> promise;
    promise.set_value();
    return promise.get_future();
}


Reader::Reader(std::unique_ptr<InputStream> stream_, const ReaderOptions & options_)
    : stream(std::move(stream_)), options(options_)
{
    readTail();
}

Reader::~Reader()
{
    /// The background reads write into the buffers of the cache.
    for (auto & range : cache)
        if (!range.ready && range.future.valid())
            range.future.wait();
}

void Reader::readTail()
{
    const UInt64 file_size = stream->getLength();
    /// The header "ORC" and at least the length of the postscript.
    if (file_size < 4)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not an ORC file: {} is too small ({} bytes)", stream->getName(), file_size);

    static constexpr UInt64 DIRECTORY_SIZE_GUESS = 16 * 1024;
    UInt64 tail_size = std::min(file_size, DIRECTORY_SIZE_GUESS);
    std::string tail(tail_size, '\0');
    stream->read(tail.data(), tail_size, file_size - tail_size);

    postscript_length = static_cast<UInt8>(tail.back());
    if (postscript_length + 1 > tail_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid postscript length {} in ORC file {}", postscript_length, stream->getName());

    postscript.parse(std::string_view(tail).substr(tail_size - 1 - postscript_length, postscript_length));

    if (postscript.magic != "ORC")
    {
        /// Very old files do not have the magic in the postscript, but they have it at the beginning.
        char header[3];
        stream->read(header, 3, 0);
        if (memcmp(header, "ORC", 3) != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Not an ORC file: {}", stream->getName());
    }

    if (postscript.compression != CompressionKind::NONE)
    {
        if (postscript.compression_block_size == 0 || postscript.compression_block_size > (1ULL << 30))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid compression block size {} in ORC file {}", postscript.compression_block_size, stream->getName());
        if (postscript.compression == CompressionKind::BROTLI)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BROTLI compression in ORC files is not supported");
    }

    UInt64 tail_length = 0;
    if (__builtin_add_overflow(postscript.footer_length, postscript.metadata_length, &tail_length)
        || __builtin_add_overflow(tail_length, postscript_length + 1, &tail_length)
        || tail_length > file_size)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Invalid footer length {} and metadata length {} in ORC file {} of size {}",
            postscript.footer_length, postscript.metadata_length, stream->getName(), file_size);

    if (tail_length > tail_size)
    {
        tail.resize(tail_length);
        tail_size = tail_length;
        stream->read(tail.data(), tail_size, file_size - tail_size);
    }

    std::string_view footer_data = std::string_view(tail).substr(tail_size - 1 - postscript_length - postscript.footer_length, postscript.footer_length);
    footer.parse(decompress(footer_data, "footer"));

    if (footer.has_encryption)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Encrypted ORC files are not supported");

    schema = Type::fromProto(footer.types);

    std::string_view metadata_data = std::string_view(tail).substr(
        tail_size - 1 - postscript_length - postscript.footer_length - postscript.metadata_length, postscript.metadata_length);
    if (!metadata_data.empty())
        metadata.emplace().parse(decompress(metadata_data, "metadata"));
    else
        metadata.emplace();
}

const Proto::Metadata & Reader::getMetadata()
{
    return *metadata;
}

std::string Reader::decompress(std::string_view data, const std::string & name) const
{
    if (postscript.compression == CompressionKind::NONE)
        return std::string(data);

    std::string result;
    InputByteStream in(data, postscript.compression, postscript.compression_block_size, name);
    while (in.nextChunk())
        result.append(in.pos, in.end);
    return result;
}

Proto::StripeFooter Reader::readStripeFooter(size_t stripe_index)
{
    const auto & stripe = getStripe(stripe_index);
    UInt64 offset = stripe.offset + stripe.index_length + stripe.data_length;
    UInt64 end = 0;
    if (__builtin_add_overflow(offset, stripe.footer_length, &end) || end > stream->getLength() || offset < stripe.offset)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Malformed stripe information at stripe index {} in ORC file {}: offset={}, indexLength={}, dataLength={}, footerLength={}, fileLength={}",
            stripe_index, stream->getName(), stripe.offset, stripe.index_length, stripe.data_length, stripe.footer_length, stream->getLength());

    auto slices = readRanges({{offset, stripe.footer_length}});
    Proto::StripeFooter result;
    result.parse(decompress(slices[0].data, "stripe footer"));
    if (result.encryption_variants)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Encrypted ORC files are not supported");
    return result;
}

std::vector<std::pair<UInt64, UInt64>> Reader::getStripeRanges(
    size_t stripe_index, const Proto::StripeFooter & stripe_footer, const std::vector<bool> & selected) const
{
    const auto & stripe = getStripe(stripe_index);
    std::vector<std::pair<UInt64, UInt64>> ranges;
    UInt64 offset = stripe.offset;
    const UInt64 data_end = stripe.offset + stripe.index_length + stripe.data_length;
    for (const auto & s : stripe_footer.streams)
    {
        if (s.kind && s.column < selected.size() && selected[s.column] && s.length)
        {
            if (offset + s.length > data_end || offset + s.length < offset)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Malformed stream at offset {} of length {} in stripe {} of ORC file {}", offset, s.length, stripe_index, stream->getName());
            ranges.emplace_back(offset, s.length);
        }
        offset += s.length;
    }
    return ranges;
}

std::vector<Reader::Slice> Reader::readRanges(const std::vector<std::pair<UInt64, UInt64>> & ranges)
{
    std::vector<Slice> result(ranges.size());

    std::vector<size_t> order(ranges.size());
    for (size_t i = 0; i < order.size(); ++i)
        order[i] = i;
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return ranges[a].first < ranges[b].first; });

    size_t i = 0;
    while (i < order.size())
    {
        UInt64 begin = ranges[order[i]].first;
        UInt64 end = begin + ranges[order[i]].second;
        size_t j = i + 1;
        while (j < order.size())
        {
            UInt64 next_begin = ranges[order[j]].first;
            UInt64 next_end = next_begin + ranges[order[j]].second;
            UInt64 new_end = std::max(end, next_end);
            if (next_begin > end && next_begin - end > options.hole_size_limit)
                break;
            if (new_end - begin > options.range_size_limit)
                break;
            end = new_end;
            ++j;
        }

        std::shared_ptr<const PODArray<char>> holder;
        const char * base = nullptr;
        UInt64 base_offset = begin;
        {
            std::lock_guard lock(cache_mutex);
            for (auto & cached : cache)
            {
                if (cached.offset <= begin && end <= cached.offset + cached.length)
                {
                    if (!cached.ready)
                    {
                        cached.ready = true;
                        cached.future.get();
                    }
                    holder = cached.buffer;
                    base = cached.buffer->data();
                    base_offset = cached.offset;
                    break;
                }
            }
        }

        if (!holder)
        {
            if (end > stream->getLength())
                throw Exception(ErrorCodes::INCORRECT_DATA, "A range at {} of length {} goes beyond the end of ORC file {}", begin, end - begin, stream->getName());
            auto buffer = std::make_shared<PODArray<char>>(end - begin);
            if (end > begin)
                stream->read(buffer->data(), end - begin, begin);
            base = buffer->data();
            holder = std::move(buffer);
        }

        for (size_t k = i; k < j; ++k)
        {
            const auto & range = ranges[order[k]];
            result[order[k]] = Slice{holder, std::string_view(base + (range.first - base_offset), range.second)};
        }
        i = j;
    }
    return result;
}

void Reader::preBuffer(const std::vector<UInt32> & stripes, const std::vector<UInt64> & include_types)
{
    auto selected = selectColumnsByTypeIds(*schema, include_types);
    for (UInt32 stripe_index : stripes)
    {
        const auto & stripe = getStripe(stripe_index);
        auto stripe_footer = readStripeFooter(stripe_index);
        auto ranges = getStripeRanges(stripe_index, stripe_footer, selected);
        ranges.emplace_back(stripe.offset + stripe.index_length + stripe.data_length, stripe.footer_length);
        std::sort(ranges.begin(), ranges.end());

        size_t i = 0;
        while (i < ranges.size())
        {
            UInt64 begin = ranges[i].first;
            UInt64 end = begin + ranges[i].second;
            size_t j = i + 1;
            while (j < ranges.size())
            {
                UInt64 next_begin = ranges[j].first;
                UInt64 new_end = std::max(end, next_begin + ranges[j].second);
                if ((next_begin > end && next_begin - end > options.hole_size_limit) || new_end - begin > options.range_size_limit)
                    break;
                end = new_end;
                ++j;
            }

            CachedRange cached{.offset = begin, .length = end - begin, .buffer = std::make_shared<PODArray<char>>(end - begin), .future = {}};
            cached.future = stream->readAsync(cached.buffer->data(), end - begin, begin);
            {
                std::lock_guard lock(cache_mutex);
                cache.push_back(std::move(cached));
            }
            i = j;
        }
    }
}

void Reader::releaseBuffer(UInt64 boundary)
{
    std::lock_guard lock(cache_mutex);
    for (auto it = cache.begin(); it != cache.end();)
    {
        if (it->offset + it->length <= boundary)
        {
            /// The buffer can be destroyed only after the background read finishes. The data has
            /// not been used, so an error of reading it does not matter.
            if (!it->ready && it->future.valid())
                it->future.wait();
            it = cache.erase(it);
        }
        else
            ++it;
    }
}

std::unique_ptr<RowReader> Reader::createRowReader(const RowReaderOptions & row_options)
{
    return std::make_unique<RowReader>(*this, row_options);
}


class RowReader::StripeStreamsImpl final : public StripeStreams
{
public:
    StripeStreamsImpl(
        Reader & reader_,
        size_t stripe_index_,
        Proto::StripeFooter footer_,
        const std::vector<bool> & selected_columns_,
        const DateLUTImpl & writer_timezone_,
        const DateLUTImpl & reader_timezone_)
        : reader(reader_)
        , stripe_index(stripe_index_)
        , footer(std::move(footer_))
        , selected_columns(selected_columns_)
        , writer_timezone(writer_timezone_)
        , reader_timezone(reader_timezone_)
    {
        const auto & stripe = reader.getStripe(stripe_index);
        UInt64 offset = stripe.offset;
        const UInt64 data_end = stripe.offset + stripe.index_length + stripe.data_length;

        std::vector<std::pair<UInt64, UInt64>> ranges;
        std::vector<std::pair<UInt64, UInt32>> keys;
        for (const auto & s : footer.streams)
        {
            if (s.kind && s.column < selected_columns.size() && selected_columns[s.column])
            {
                if (offset + s.length > data_end || offset + s.length < offset)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Malformed stream meta in stripe {} of ORC file: streamOffset={}, streamLength={}, stripeOffset={}, stripeIndexLength={}, stripeDataLength={}",
                        stripe_index, offset, s.length, stripe.offset, stripe.index_length, stripe.data_length);
                auto key = std::make_pair(static_cast<UInt64>(s.column), *s.kind);
                /// Use the first stream if there are several of the same kind, as the reference implementation.
                if (std::find(keys.begin(), keys.end(), key) == keys.end())
                {
                    keys.push_back(key);
                    ranges.emplace_back(offset, s.length);
                }
            }
            else if (s.kind && s.column >= selected_columns.size())
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Malformed stream meta in stripe {} of ORC file: column {} is out of range, the file has {} columns",
                    stripe_index, s.column, selected_columns.size());
            offset += s.length;
        }

        slices = reader.readRanges(ranges);
        for (size_t i = 0; i < keys.size(); ++i)
            streams.emplace(keys[i], slices[i].data);
    }

    const Proto::ColumnEncoding & getEncoding(UInt64 column_id) const override
    {
        if (column_id >= footer.columns.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ORC stripe {} has no encoding for column {}, it has {} column encodings", stripe_index, column_id, footer.columns.size());
        return footer.columns[column_id];
    }

    std::unique_ptr<InputByteStream> getStream(UInt64 column_id, Proto::StreamKind kind) const override
    {
        auto it = streams.find({column_id, static_cast<UInt32>(kind)});
        if (it == streams.end())
            return nullptr;
        return std::make_unique<InputByteStream>(
            it->second, reader.getCompression(), reader.getCompressionBlockSize(),
            fmt::format("column {} kind {} in stripe {}", column_id, static_cast<UInt32>(kind), stripe_index));
    }

    std::optional<std::string_view> getStreamData(UInt64 column_id, Proto::StreamKind kind) const
    {
        auto it = streams.find({column_id, static_cast<UInt32>(kind)});
        if (it == streams.end())
            return {};
        return it->second;
    }

    const std::vector<bool> & getSelectedColumns() const override { return selected_columns; }
    const DateLUTImpl & getWriterTimezone() const override { return writer_timezone; }
    const DateLUTImpl & getReaderTimezone() const override { return reader_timezone; }
    UInt64 getNumberOfRows() const override { return reader.getStripe(stripe_index).number_of_rows; }

    const Proto::StripeFooter & getFooter() const { return footer; }

private:
    Reader & reader;
    size_t stripe_index;
    Proto::StripeFooter footer;
    const std::vector<bool> & selected_columns;
    const DateLUTImpl & writer_timezone;
    const DateLUTImpl & reader_timezone;
    std::vector<Reader::Slice> slices;
    std::map<std::pair<UInt64, UInt32>, std::string_view> streams;
};


RowReader::RowReader(Reader & reader_, const RowReaderOptions & options_)
    : reader(reader_)
    , options(options_)
    , reader_timezone(DateLUT::instance(options.timezone_name))
{
    const Type & schema = reader.getType();
    if (options.include_types)
        selected_columns = selectColumnsByTypeIds(schema, *options.include_types);
    else
        selected_columns.assign(schema.getMaximumColumnId() + 1, true);
    selected_type = Type::buildSelectedType(schema, selected_columns);

    UInt64 range_end = options.range_offset + std::min(options.range_length, std::numeric_limits<UInt64>::max() - options.range_offset);
    const auto & stripes = reader.getFooter().stripes;
    current_stripe = stripes.size();
    last_stripe = 0;
    for (size_t i = 0; i < stripes.size(); ++i)
    {
        if (stripes[i].offset >= options.range_offset && stripes[i].offset < range_end)
        {
            current_stripe = std::min<UInt64>(current_stripe, i);
            last_stripe = std::max<UInt64>(last_stripe, i + 1);
        }
    }
    if (current_stripe >= last_stripe)
        current_stripe = last_stripe = 0;

    /// The search argument can be applied only if there are row indexes.
    if (reader.getFooter().row_index_stride == 0)
        options.search_argument.reset();
}

RowReader::~RowReader() = default;

std::unique_ptr<ColumnVectorBatch> RowReader::createRowBatch(size_t capacity) const
{
    return ORC::createRowBatch(*selected_type, capacity, options.enable_lazy_decoding);
}

const DateLUTImpl & RowReader::getWriterTimezone(const Proto::StripeFooter & stripe_footer) const
{
    if (stripe_footer.writer_timezone)
        return DateLUT::instance(*stripe_footer.writer_timezone);
    /// The reference implementation uses the local time zone of the reader if the file does not specify it.
    return DateLUT::serverTimezoneInstance();
}

bool RowReader::evaluateStatistics(const std::vector<Proto::ColumnStatistics> & stats) const
{
    const auto & leaves = options.search_argument->getLeaves();
    std::vector<TruthValue> values(leaves.size(), TruthValue::YES_NO_NULL);
    UInt32 writer_version = reader.getPostScript().writer_version;
    for (size_t i = 0; i < leaves.size(); ++i)
    {
        UInt64 column = leaves[i].getColumnId();
        if (column < stats.size())
            values[i] = leaves[i].evaluate(writer_version, stats[column], nullptr);
    }
    return isNeeded(options.search_argument->evaluate(values));
}

void RowReader::loadStripeIndex()
{
    row_indexes.clear();
    bloom_filters.clear();
    const auto & stripe_footer = stripe_streams->getFooter();
    for (const auto & s : stripe_footer.streams)
    {
        if (!s.kind || s.column >= selected_columns.size() || !selected_columns[s.column])
            continue;
        auto kind = static_cast<Proto::StreamKind>(*s.kind);
        if (kind == Proto::StreamKind::ROW_INDEX)
        {
            if (row_indexes.contains(s.column))
                continue;
            auto data = stripe_streams->getStreamData(s.column, kind);
            row_indexes[s.column].parse(reader.decompress(data.value_or(std::string_view{}), "row index"));
        }
        else if (kind == Proto::StreamKind::BLOOM_FILTER_UTF8)
        {
            if (bloom_filters.contains(s.column))
                continue;
            const auto & encoding = stripe_streams->getEncoding(s.column);
            Proto::BloomFilterIndex index;
            auto data = stripe_streams->getStreamData(s.column, kind);
            index.parse(reader.decompress(data.value_or(std::string_view{}), "bloom filter"));
            auto & filters = bloom_filters[s.column];
            for (const auto & bf : index.bloom_filter)
            {
                /// Only the bloom filters with the UTF-8 strings and UTC timestamps can be used.
                if (encoding.bloom_encoding.value_or(0) == 1 && bf.num_hash_functions && bf.utf8bitset)
                    filters.push_back(std::make_shared<BloomFilter>(bf));
                else
                    filters.push_back(nullptr);
            }
        }
    }
}

void RowReader::pickRowGroups()
{
    const UInt64 stride = reader.getFooter().row_index_stride;
    const UInt64 groups = (rows_in_current_stripe + stride - 1) / stride;

    if (row_indexes.empty())
    {
        /// Without the row indexes all row groups are read.
        next_skipped_rows.clear();
        return;
    }

    next_skipped_rows.assign(groups, 0);
    const auto & leaves = options.search_argument->getLeaves();
    std::vector<TruthValue> values(leaves.size(), TruthValue::YES_NO_NULL);
    const UInt32 writer_version = reader.getPostScript().writer_version;
    const Proto::ColumnStatistics empty_statistics;

    UInt64 next_skipped_group = groups;
    for (UInt64 group = groups; group > 0;)
    {
        --group;
        for (size_t i = 0; i < leaves.size(); ++i)
        {
            UInt64 column = leaves[i].getColumnId();
            auto it = row_indexes.find(column);
            if (it == row_indexes.end())
            {
                values[i] = TruthValue::YES_NO_NULL;
                continue;
            }
            if (group >= it->second.entry.size())
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "The row index of ORC column {} has {} entries, less than the {} row groups of stripe {}",
                    column, it->second.entry.size(), groups, current_stripe);
            const auto & entry = it->second.entry[group];
            const BloomFilter * bloom_filter = nullptr;
            if (auto bf = bloom_filters.find(column); bf != bloom_filters.end() && group < bf->second.size())
                bloom_filter = bf->second[group].get();
            values[i] = leaves[i].evaluate(writer_version, entry.statistics ? *entry.statistics : empty_statistics, bloom_filter);
        }

        bool needed = isNeeded(options.search_argument->evaluate(values));
        if (!needed)
        {
            next_skipped_rows[group] = 0;
            next_skipped_group = group;
        }
        else
            next_skipped_rows[group] = (next_skipped_group == groups) ? rows_in_current_stripe : next_skipped_group * stride;
    }
}

UInt64 RowReader::advanceToNextRowGroup(UInt64 current_row) const
{
    const UInt64 groups = next_skipped_rows.size();
    if (groups == 0)
        return std::min(current_row, rows_in_current_stripe);

    const UInt64 stride = reader.getFooter().row_index_stride;
    UInt64 group = current_row / stride;
    if (group >= groups)
        return rows_in_current_stripe;
    if (next_skipped_rows[group] != 0)
        return current_row;
    while (group < groups && next_skipped_rows[group] == 0)
        ++group;
    if (group < groups)
        return group * stride;
    return rows_in_current_stripe;
}

UInt64 RowReader::computeBatchSize(UInt64 requested, UInt64 current_row) const
{
    UInt64 end_row = rows_in_current_stripe;
    const UInt64 groups = next_skipped_rows.size();
    if (groups > 0)
    {
        const UInt64 stride = reader.getFooter().row_index_stride;
        UInt64 group = current_row / stride;
        if (group >= groups)
            return 0;
        UInt64 next_skipped_row = next_skipped_rows[group];
        if (next_skipped_row == 0)
            return 0;
        end_row = next_skipped_row;
    }
    return std::min(requested, end_row - current_row);
}

void RowReader::seekToRowGroup(size_t row_group)
{
    std::unordered_map<UInt64, PositionProvider> positions;
    for (const auto & [column, index] : row_indexes)
    {
        if (row_group >= index.entry.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The row index of ORC column {} has {} entries, the row group {} is requested", column, index.entry.size(), row_group);
        positions.emplace(column, PositionProvider(index.entry[row_group].positions));
    }
    column_reader->seekToRowGroup(positions);
}

void RowReader::startNextStripe()
{
    column_reader.reset();
    stripe_streams.reset();
    row_indexes.clear();
    bloom_filters.clear();
    next_skipped_rows.clear();

    const auto & search_argument = options.search_argument;
    const auto & footer = reader.getFooter();

    if (search_argument && !file_statistics_evaluated)
    {
        file_statistics_result = footer.statistics.empty() || evaluateStatistics(footer.statistics);
        file_statistics_evaluated = true;
    }
    if (search_argument && !file_statistics_result)
    {
        current_stripe = last_stripe;
        return;
    }

    do
    {
        const auto & stripe = footer.stripes[current_stripe];
        rows_in_current_stripe = stripe.number_of_rows;

        bool is_stripe_needed = true;
        const auto & stripe_statistics = reader.getMetadata().stripe_stats;
        if (search_argument && stripe_statistics.size() == footer.stripes.size())
        {
            const auto & stats = stripe_statistics[current_stripe].col_stats;
            is_stripe_needed = stats.empty() || evaluateStatistics(stats);
        }

        if (is_stripe_needed)
        {
            auto stripe_footer = reader.readStripeFooter(current_stripe);
            const DateLUTImpl & writer_timezone = getWriterTimezone(stripe_footer);
            stripe_streams = std::make_unique<StripeStreamsImpl>(
                reader, current_stripe, std::move(stripe_footer), selected_columns, writer_timezone, reader_timezone);

            if (search_argument)
            {
                loadStripeIndex();
                pickRowGroups();
                bool has_selected = next_skipped_rows.empty();
                for (UInt64 group = current_row_in_stripe / footer.row_index_stride; !has_selected && group < next_skipped_rows.size(); ++group)
                    has_selected = next_skipped_rows[group] != 0;
                if (has_selected)
                    break;
                is_stripe_needed = false;
                stripe_streams.reset();
            }
        }

        if (!is_stripe_needed)
        {
            ++current_stripe;
            current_row_in_stripe = 0;
        }
    } while (search_argument && current_stripe < last_stripe);

    if (current_stripe >= last_stripe)
        return;

    column_reader = buildColumnReader(reader.getType(), *stripe_streams);

    if (search_argument)
    {
        current_row_in_stripe = advanceToNextRowGroup(current_row_in_stripe);
        if (current_row_in_stripe > 0)
            seekToRowGroup(current_row_in_stripe / footer.row_index_stride);
    }
}

bool RowReader::next(ColumnVectorBatch & batch)
{
    if (current_stripe >= last_stripe)
    {
        batch.numElements = 0;
        return false;
    }

    if (current_row_in_stripe == 0)
        startNextStripe();

    if (current_stripe >= last_stripe)
    {
        batch.numElements = 0;
        return false;
    }

    UInt64 rows_to_read = std::min<UInt64>(batch.capacity, rows_in_current_stripe - current_row_in_stripe);
    if (options.search_argument && rows_to_read > 0)
        rows_to_read = computeBatchSize(rows_to_read, current_row_in_stripe);

    batch.numElements = rows_to_read;
    if (rows_to_read == 0)
    {
        current_stripe = last_stripe;
        return false;
    }

    if (options.enable_lazy_decoding)
        column_reader->nextEncoded(batch, rows_to_read, nullptr);
    else
        column_reader->next(batch, rows_to_read, nullptr);

    current_row_in_stripe += rows_to_read;

    if (options.search_argument)
    {
        UInt64 next_row = advanceToNextRowGroup(current_row_in_stripe);
        if (current_row_in_stripe != next_row)
        {
            current_row_in_stripe = next_row;
            if (current_row_in_stripe < rows_in_current_stripe)
                seekToRowGroup(current_row_in_stripe / reader.getFooter().row_index_stride);
        }
    }

    if (current_row_in_stripe >= rows_in_current_stripe)
    {
        ++current_stripe;
        current_row_in_stripe = 0;
    }
    return true;
}

}
