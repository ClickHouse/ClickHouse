#include "SelectiveColumnReader.h"

#include <Columns/ColumnTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
extern const int PARQUET_EXCEPTION;
extern const int NOT_IMPLEMENTED;
extern const int BAD_ARGUMENTS;
}

void SelectiveColumnReader::readPageIfNeeded()
{
    initPageReaderIfNeed();
    skipPageIfNeed();
    while (!state.offsets.remain_rows)
    {
        if (!readPage())
            break;
    }
}

bool SelectiveColumnReader::readPage()
{
    state.rep_levels.resize(0);
    state.def_levels.resize(0);
    if (!page_reader->hasNext())
        return false;
    auto page_header = page_reader->peekNextPageHeader();
    auto page_type = page_header.type;
    if (page_type == parquet::format::PageType::DICTIONARY_PAGE)
    {
        auto dict_page = page_reader->nextPage();
        const parquet::DictionaryPage & dict_page1 = *std::static_pointer_cast<parquet::DictionaryPage>(dict_page);
        if (unlikely(dict_page1.encoding() != parquet::Encoding::PLAIN_DICTIONARY && dict_page1.encoding() != parquet::Encoding::PLAIN))
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported dictionary page encoding {}", dict_page1.encoding());
        }
        readDictPage(static_cast<const parquet::DictionaryPage &>(*dict_page));
    }
    else if (page_type == parquet::format::PageType::DATA_PAGE || page_type == parquet::format::PageType::DATA_PAGE_V2)
    {
        state.page_position++;
        auto rows = page_type == parquet::format::PageType::DATA_PAGE ? page_header.data_page_header.num_values
                                                                      : page_header.data_page_header_v2.num_values;
        state.offsets.reset(rows);
        state.page.reset();
        skipPageIfNeed();
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported page type {}", magic_enum::enum_name(page_type));
    }
    return true;
}

void SelectiveColumnReader::initDataPageDecoder(const parquet::Encoding::type encoding)
{
    if (encoding == parquet::Encoding::RLE_DICTIONARY || encoding == parquet::Encoding::PLAIN_DICTIONARY)
    {
        initIndexDecoderIfNeeded();
        createDictDecoder();
        plain = false;
    }
    else if (encoding == parquet::Encoding::PLAIN)
    {
        if (!plain)
        {
            downgradeToPlain();
            plain = true;
        }
        plain_decoder = std::make_unique<PlainDecoder>(state.data, state.offsets);
    }
    else
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported encoding type {}", magic_enum::enum_name(encoding));
    }
}

void SelectiveColumnReader::readDataPageV1(const parquet::DataPageV1 & page)
{
    parquet::LevelDecoder decoder;
    auto max_size = page.size();
    //    state.offsets.remain_rows = page.num_values();
    state.data.buffer = page.data();
    auto max_rep_level = scan_spec.column_desc->max_repetition_level();
    auto max_def_level = scan_spec.column_desc->max_definition_level();
    state.def_levels.resize(0);
    state.rep_levels.resize(0);
    if (max_rep_level > 0)
    {
        auto rep_bytes = decoder.SetData(
            page.repetition_level_encoding(), max_rep_level, static_cast<int>(state.offsets.remain_rows), state.data.buffer, max_size);
        max_size -= rep_bytes;
        state.data.buffer += rep_bytes;
        state.rep_levels.resize(state.offsets.remain_rows);
        decoder.Decode(static_cast<int>(state.offsets.remain_rows), state.rep_levels.data());
    }
    if (max_def_level > 0)
    {
        auto def_bytes = decoder.SetData(
            page.definition_level_encoding(), max_def_level, static_cast<int>(state.offsets.remain_rows), state.data.buffer, max_size);
        max_size -= def_bytes;
        state.data.buffer += def_bytes;
        state.def_levels.resize(state.offsets.remain_rows);
        decoder.Decode(static_cast<int>(state.offsets.remain_rows), state.def_levels.data());
    }
    state.data.buffer_size = max_size;
    initDataPageDecoder(page.encoding());
    state.lazy_skip_rows = skipValuesInCurrentPage(state.lazy_skip_rows);
    chassert(state.lazy_skip_rows == 0);
}

void SelectiveColumnReader::readDataPageV2(const parquet::DataPageV2 & page)
{
    state.data.buffer = page.data();
    parquet::LevelDecoder decoder;

    if (page.repetition_levels_byte_length() < 0 || page.definition_levels_byte_length() < 0)
    {
        throw Exception(
            ErrorCodes::PARQUET_EXCEPTION,
            "Either RL or DL is negative, this should not happen. Most likely corrupt file or parsing issue");
    }

    const int32_t total_levels_length = page.repetition_levels_byte_length() + page.definition_levels_byte_length();

    if (total_levels_length > page.size())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Data page too small for levels (corrupt header?)");
    }

    // ARROW-17453: Even if max_rep_level_ is 0, there may still be
    // repetition level bytes written and/or reported in the header by
    // some writers (e.g. Athena)
    auto max_rep_level = scan_spec.column_desc->max_repetition_level();
    auto max_def_level = scan_spec.column_desc->max_definition_level();
    state.def_levels.resize(0);
    state.rep_levels.resize(0);
    auto rl_bytes = page.repetition_levels_byte_length();
    if (max_rep_level > 0)
    {
        decoder.SetDataV2(rl_bytes, max_rep_level, static_cast<int>(state.offsets.remain_rows), state.data.buffer);
        state.data.buffer += rl_bytes;
        state.rep_levels.resize(state.offsets.remain_rows);
        decoder.Decode(static_cast<int>(state.offsets.remain_rows), state.rep_levels.data());
    }
    state.data.buffer += rl_bytes;

    auto dl_bytes = page.definition_levels_byte_length();
    if (max_def_level > 0)
    {
        decoder.SetDataV2(dl_bytes, max_def_level, static_cast<int>(state.offsets.remain_rows), state.data.buffer);
        state.data.buffer += dl_bytes;
        state.def_levels.resize(state.offsets.remain_rows);
        decoder.Decode(static_cast<int>(state.offsets.remain_rows), state.def_levels.data());
    }
    state.data.buffer_size = page.size() - total_levels_length;

    initDataPageDecoder(page.encoding());
    state.lazy_skip_rows = skipValuesInCurrentPage(state.lazy_skip_rows);
    chassert(state.lazy_skip_rows == 0);
}

void SelectiveColumnReader::decodePageIfNeed()
{
    if (state.page)
        return;
    state.page = page_reader->nextPage();
    switch (state.page->type())
    {
        case parquet::PageType::DATA_PAGE:
            readDataPageV1(static_cast<const parquet::DataPageV1 &>(*state.page));
            break;
        case parquet::PageType::DATA_PAGE_V2:
            readDataPageV2(static_cast<const parquet::DataPageV2 &>(*state.page));
            break;
        default:
            throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported page type {}", magic_enum::enum_name(state.page->type()));
    }
}

void SelectiveColumnReader::skipPageIfNeed()
{
    if (!state.page && state.offsets.remain_rows && state.offsets.remain_rows <= state.lazy_skip_rows)
    {
        // skip page
        state.lazy_skip_rows -= state.offsets.remain_rows;
        page_reader->skipNextPage();
        state.offsets.consume(state.offsets.remain_rows);
        return;
    }
    if (page_reader && !state.offsets.remain_rows && offset_index && static_cast<size_t>(state.page_position) < offset_index->page_locations().size() - 1)
    {
        while (state.lazy_skip_rows)
        {
            if (offset_index->page_locations().size() == static_cast<size_t>(state.page_position + 1))
                break;
            size_t next_page_rows = offset_index->page_locations().at(state.page_position + 1).first_row_index
                - offset_index->page_locations().at(state.page_position).first_row_index;
            if (next_page_rows <= state.lazy_skip_rows)
            {
                state.page_position++;
                chassert(page_reader);
                page_reader->seekFileOffset(offset_index->page_locations().at(state.page_position).offset);
                state.lazy_skip_rows -= next_page_rows;
            }
            else
                break;
        }
    }
}

void SelectiveColumnReader::skip(size_t rows)
{
    state.lazy_skip_rows += rows;
    state.lazy_skip_rows = skipValuesInCurrentPage(state.lazy_skip_rows);
    skipPageIfNeed();
}

void SelectiveColumnReader::skipNulls(size_t rows_to_skip)
{
    if (!rows_to_skip)
        return;
    auto skipped = std::min(rows_to_skip, state.offsets.remain_rows);
    state.offsets.consume(skipped);
    state.lazy_skip_rows += (rows_to_skip - skipped);
}

void SelectiveColumnReader::loadDictPageIfNeeded()
{
    if (!needReadDictPage())
        return;
    chassert(column_chunk_meta);
    initPageReaderIfNeed();
    auto original_offset = page_reader->getOffsetInFile();
    if (column_chunk_meta->has_dictionary_page())
    {
        page_reader->seekFileOffset(column_chunk_meta->dictionary_page_offset());
        // read dict page
        readPage();
    }
    else
    {
        // first page is dict page
        page_reader->seekFileOffset(column_chunk_meta->file_offset());
        readPage();
    }
    page_reader->seekFileOffset(original_offset);
}

Int32 loadLength(const uint8_t * data)
{
    auto value_len = arrow::util::SafeLoadAs<Int32>(data);
    if (unlikely(value_len < 0 || value_len > INT32_MAX - 4))
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Invalid or corrupted value_len '{}'", value_len);
    }
    return value_len;
}
void computeRowSetPlainString(const uint8_t * start, OptionalRowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read)
{
    if (!filter || !row_set.has_value())
        return;
    size_t offset = 0;
    auto & sets = row_set.value();
    for (size_t i = 0; i < rows_to_read; i++)
    {
        auto len = loadLength(start + offset);
        offset += 4;
        if (len == 0)
            sets.set(i, filter->testString(""));
        else
            sets.set(i, filter->testString(std::string_view(reinterpret_cast<const char *>(start + offset), len)));
        offset += len;
    }
}
void computeRowSetPlainStringSpace(
    const uint8_t * start, OptionalRowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read, PaddedPODArray<UInt8> & null_map)
{
    if (!filter || !row_set.has_value())
        return;
    size_t offset = 0;
    auto & sets = row_set.value();
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (null_map[i])
        {
            sets.set(i, filter->testNull());
            continue;
        }
        auto len = loadLength(start + offset);
        offset += 4;
        if (len == 0)
            sets.set(i, filter->testString(""));
        else
            sets.set(i, filter->testString(String(reinterpret_cast<const char *>(start + offset), len)));
        offset += len;
    }
}

}
