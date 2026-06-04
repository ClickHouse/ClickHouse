#include <Processors/Formats/Impl/ArrowIPC/RecordBatchDecoder.h>

#if USE_ARROW

#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::ArrowIPC
{

void DictionaryRegistry::set(int64_t id, ColumnPtr values, bool is_delta)
{
    auto it = dictionaries.find(id);
    if (is_delta && it != dictionaries.end())
    {
        auto merged = IColumn::mutate(std::move(it->second));
        merged->insertRangeFrom(*values, 0, values->size());
        it->second = std::move(merged);
    }
    else
    {
        dictionaries[id] = std::move(values);
    }
}

ColumnPtr DictionaryRegistry::get(int64_t id) const
{
    auto it = dictionaries.find(id);
    if (it == dictionaries.end())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch references unknown dictionary id {}", id);
    return it->second;
}

const flatbuf::FieldNode & RecordBatchDecoder::nextNode()
{
    const auto * nodes = current_batch->nodes();
    if (!nodes || node_index >= nodes->size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has fewer field nodes than the schema requires");
    return *nodes->Get(static_cast<flatbuffers::uoffset_t>(node_index++));
}

RecordBatchDecoder::Slice RecordBatchDecoder::nextBuffer()
{
    const auto * buffers = current_batch->buffers();
    if (!buffers || buffer_index >= buffers->size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC record batch has fewer buffers than the schema requires");

    const auto * buffer = buffers->Get(static_cast<flatbuffers::uoffset_t>(buffer_index++));
    const int64_t offset = buffer->offset();
    const int64_t length = buffer->length();
    const int64_t body_size = static_cast<int64_t>(current_body->size());

    if (offset < 0 || length < 0 || offset > body_size || length > body_size - offset)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC buffer (offset {}, length {}) is out of the message body of size {}",
            offset, length, body_size);

    return Slice{current_body->data() + offset, length};
}

namespace
{

void checkBufferSize(const RecordBatchDecoder::Slice & slice, size_t required, const char * what)
{
    if (static_cast<size_t>(slice.length) < required)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC {} buffer is too small: have {} bytes, need {}",
            what, slice.length, required);
}

/// Fills a fixed-width ClickHouse column (ColumnVector / ColumnDecimal) by copying `value_size`
/// bytes per row from the source buffer. For decimals `value_size` may be smaller than the Arrow
/// storage width, so the low (little-endian) bytes are taken per value.
template <typename Col>
void fillFixed(IColumn & column, size_t rows, const RecordBatchDecoder::Slice & values, size_t arrow_value_size)
{
    using V = typename Col::ValueType;
    checkBufferSize(values, rows * arrow_value_size, "values");
    auto & data = assert_cast<Col &>(column).getData();
    data.resize(rows);
    if (rows == 0)
        return;
    if (arrow_value_size == sizeof(V))
    {
        memcpy(data.data(), values.ptr, rows * sizeof(V));
    }
    else
    {
        /// Decimal stored wider in Arrow than in ClickHouse: take the low bytes of each value.
        chassert(arrow_value_size > sizeof(V));
        auto * dst = reinterpret_cast<char *>(data.data());
        for (size_t i = 0; i < rows; ++i)
            memcpy(dst + i * sizeof(V), values.ptr + i * arrow_value_size, sizeof(V));
    }
}

}

ColumnPtr RecordBatchDecoder::buildNullMap(const Slice & validity, size_t rows, int64_t null_count) const
{
    auto null_map = ColumnUInt8::create(rows);
    auto & data = null_map->getData();

    /// A field with no nulls may omit the validity bitmap (a zero-length buffer): everything is valid.
    if (validity.length == 0 || null_count == 0)
    {
        memset(data.data(), 0, rows);
        return null_map;
    }

    checkBufferSize(validity, (rows + 7) / 8, "validity");
    const auto * bits = reinterpret_cast<const uint8_t *>(validity.ptr);
    /// Arrow validity bitmap is LSB-first and uses 1 = valid; ClickHouse null map uses 1 = null.
    for (size_t i = 0; i < rows; ++i)
        data[i] = ((bits[i >> 3] >> (i & 7)) & 1) ? 0 : 1;

    return null_map;
}

ColumnPtr RecordBatchDecoder::decodeInner(const ArrowField & field, size_t rows)
{
    const ArrowType & type = field.type;
    DataTypePtr inner_type = fieldToCHType(field, settings, /*make_nullable=*/false);
    auto column = inner_type->createColumn();

    switch (type.kind)
    {
        case TypeKind::Int:
        {
            const Slice values = nextBuffer();
            if (type.is_signed)
            {
                switch (type.bit_width)
                {
                    case 8: fillFixed<ColumnInt8>(*column, rows, values, 1); break;
                    case 16: fillFixed<ColumnInt16>(*column, rows, values, 2); break;
                    case 32: fillFixed<ColumnInt32>(*column, rows, values, 4); break;
                    case 64: fillFixed<ColumnInt64>(*column, rows, values, 8); break;
                    default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow int bit width {}", type.bit_width);
                }
            }
            else
            {
                switch (type.bit_width)
                {
                    case 8: fillFixed<ColumnUInt8>(*column, rows, values, 1); break;
                    case 16: fillFixed<ColumnUInt16>(*column, rows, values, 2); break;
                    case 32: fillFixed<ColumnUInt32>(*column, rows, values, 4); break;
                    case 64: fillFixed<ColumnUInt64>(*column, rows, values, 8); break;
                    default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow int bit width {}", type.bit_width);
                }
            }
            break;
        }
        case TypeKind::FloatingPoint:
        {
            const Slice values = nextBuffer();
            if (type.float_precision == flatbuf::Precision_DOUBLE)
                fillFixed<ColumnFloat64>(*column, rows, values, 8);
            else if (type.float_precision == flatbuf::Precision_SINGLE)
                fillFixed<ColumnFloat32>(*column, rows, values, 4);
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Native Arrow IPC reader does not support half-float yet");
            break;
        }
        case TypeKind::Bool:
        {
            const Slice values = nextBuffer();
            checkBufferSize(values, (rows + 7) / 8, "bool");
            auto & data = assert_cast<ColumnUInt8 &>(*column).getData();
            data.resize(rows);
            const auto * bits = reinterpret_cast<const uint8_t *>(values.ptr);
            for (size_t i = 0; i < rows; ++i)
                data[i] = (bits[i >> 3] >> (i & 7)) & 1;
            break;
        }
        case TypeKind::Decimal:
        {
            const Slice values = nextBuffer();
            const size_t arrow_value_size = static_cast<size_t>(type.decimal_bit_width) / 8;
            switch (column->getDataType())
            {
                case TypeIndex::Decimal32: fillFixed<ColumnDecimal<Decimal32>>(*column, rows, values, arrow_value_size); break;
                case TypeIndex::Decimal64: fillFixed<ColumnDecimal<Decimal64>>(*column, rows, values, arrow_value_size); break;
                case TypeIndex::Decimal128: fillFixed<ColumnDecimal<Decimal128>>(*column, rows, values, arrow_value_size); break;
                case TypeIndex::Decimal256: fillFixed<ColumnDecimal<Decimal256>>(*column, rows, values, arrow_value_size); break;
                default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected decimal column type");
            }
            break;
        }
        case TypeKind::Date:
        {
            const Slice values = nextBuffer();
            if (type.unit == flatbuf::DateUnit_DAY)
            {
                /// date32: days since the epoch, maps to Date32 (Int32).
                fillFixed<ColumnInt32>(*column, rows, values, 4);
            }
            else
            {
                /// date64: milliseconds since the epoch, maps to DateTime (UInt32 seconds).
                checkBufferSize(values, rows * sizeof(int64_t), "date64");
                auto & data = assert_cast<ColumnUInt32 &>(*column).getData();
                data.resize(rows);
                const auto * src = reinterpret_cast<const int64_t *>(values.ptr);
                for (size_t i = 0; i < rows; ++i)
                    data[i] = static_cast<UInt32>(src[i] / 1000);
            }
            break;
        }
        case TypeKind::Timestamp:
        case TypeKind::Time:
        {
            const Slice values = nextBuffer();
            /// Both map to DateTime64(unit*3); the raw int64 is exactly the underlying value at that scale.
            fillFixed<ColumnDecimal<DateTime64>>(*column, rows, values, 8);
            break;
        }
        case TypeKind::Utf8:
        case TypeKind::Binary:
        case TypeKind::LargeUtf8:
        case TypeKind::LargeBinary:
        {
            const bool large = type.kind == TypeKind::LargeUtf8 || type.kind == TypeKind::LargeBinary;
            const Slice offsets_slice = nextBuffer();
            const Slice data_slice = nextBuffer();
            auto & string_column = assert_cast<ColumnString &>(*column);
            /// A zero-row column may omit its offsets buffer entirely; nothing to decode.
            if (rows == 0)
                break;
            string_column.reserve(rows);
            string_column.getChars().reserve(static_cast<size_t>(data_slice.length) + rows);

            const size_t offset_size = large ? sizeof(int64_t) : sizeof(int32_t);
            checkBufferSize(offsets_slice, (rows + 1) * offset_size, "offsets");

            auto read_offset = [&](size_t i) -> int64_t
            {
                if (large)
                    return reinterpret_cast<const int64_t *>(offsets_slice.ptr)[i];
                return reinterpret_cast<const int32_t *>(offsets_slice.ptr)[i];
            };

            int64_t prev = read_offset(0);
            if (prev != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC string column has a non-zero first offset {}", prev);
            for (size_t i = 0; i < rows; ++i)
            {
                const int64_t end = read_offset(i + 1);
                if (end < prev || end > data_slice.length)
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Arrow IPC string column has a corrupted offset (prev {}, end {}, data size {})",
                        prev, end, data_slice.length);
                string_column.insertData(data_slice.ptr + prev, static_cast<size_t>(end - prev));
                prev = end;
            }
            break;
        }
        case TypeKind::FixedSizeBinary:
        {
            const Slice values = nextBuffer();
            const size_t n = static_cast<size_t>(type.byte_width);
            checkBufferSize(values, rows * n, "fixed_size_binary");
            auto & fixed_column = assert_cast<ColumnFixedString &>(*column);
            auto & chars = fixed_column.getChars();
            chars.resize(rows * n);
            if (rows)
                memcpy(chars.data(), values.ptr, rows * n);
            break;
        }
        case TypeKind::List:
        case TypeKind::LargeList:
            return readOffsetsAndChild(field, rows, /*large=*/type.kind == TypeKind::LargeList);
        case TypeKind::FixedSizeList:
        {
            /// No offsets buffer: each row has exactly `list_size` elements.
            const size_t list_size = static_cast<size_t>(type.list_size);
            ColumnPtr child = decodeField(type.children.at(0));
            if (child->size() != rows * list_size)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Arrow IPC fixed-size-list child has {} rows, expected {}", child->size(), rows * list_size);
            auto offsets_col = ColumnUInt64::create(rows);
            auto & offs = offsets_col->getData();
            for (size_t i = 0; i < rows; ++i)
                offs[i] = (i + 1) * list_size;
            return ColumnArray::create(child, std::move(offsets_col));
        }
        case TypeKind::Struct:
        {
            Columns elements;
            elements.reserve(type.children.size());
            for (const ArrowField & child : type.children)
                elements.push_back(decodeField(child));
            return ColumnTuple::create(elements);
        }
        case TypeKind::Map:
        {
            /// Map is List<Struct<key, value>>: read the list offsets, then the entries struct.
            const Slice offsets_slice = nextBuffer();
            checkBufferSize(offsets_slice, (rows + 1) * sizeof(int32_t), "map offsets");
            const auto * arrow_offsets = reinterpret_cast<const int32_t *>(offsets_slice.ptr);
            const int64_t base = arrow_offsets[0];
            auto offsets_col = ColumnUInt64::create(rows);
            auto & offs = offsets_col->getData();
            for (size_t i = 0; i < rows; ++i)
            {
                const int64_t end = arrow_offsets[i + 1];
                if (end < base)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC map has non-monotonic offsets");
                offs[i] = static_cast<UInt64>(end - base);
            }

            ColumnPtr entries = decodeField(type.children.at(0));
            const auto & entries_tuple = assert_cast<const ColumnTuple &>(*entries);
            if (entries_tuple.tupleSize() != 2)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC map entries must be a struct of (key, value)");
            if (entries_tuple.size() != (rows ? offs.back() : 0))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC map entries size does not match offsets");
            return ColumnMap::create(entries_tuple.getColumnPtr(0), entries_tuple.getColumnPtr(1), std::move(offsets_col));
        }
        default:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Native Arrow IPC reader does not support decoding this type yet (field '{}')",
                field.name);
    }

    return column;
}

ColumnPtr RecordBatchDecoder::readOffsetsAndChild(const ArrowField & field, size_t rows, bool large)
{
    const Slice offsets_slice = nextBuffer();
    const size_t offset_size = large ? sizeof(int64_t) : sizeof(int32_t);
    checkBufferSize(offsets_slice, (rows + 1) * offset_size, "list offsets");

    auto read_offset = [&](size_t i) -> int64_t
    {
        if (large)
            return reinterpret_cast<const int64_t *>(offsets_slice.ptr)[i];
        return reinterpret_cast<const int32_t *>(offsets_slice.ptr)[i];
    };

    const int64_t base = read_offset(0);
    auto offsets_col = ColumnUInt64::create(rows);
    auto & offs = offsets_col->getData();
    for (size_t i = 0; i < rows; ++i)
    {
        const int64_t end = read_offset(i + 1);
        if (end < base)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC list has non-monotonic offsets");
        offs[i] = static_cast<UInt64>(end - base);
    }

    ColumnPtr child = decodeField(field.type.children.at(0));
    if (child->size() != (rows ? offs.back() : 0))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Arrow IPC list child has {} rows, expected {}", child->size(), rows ? offs.back() : 0);
    return ColumnArray::create(child, std::move(offsets_col));
}

ColumnPtr RecordBatchDecoder::decodeDictionary(const ArrowField & field, size_t rows, const Slice & validity, int64_t null_count)
{
    const Slice indices_slice = nextBuffer();
    ColumnPtr values = registry.get(field.dictionary->id);
    const size_t dict_size = values->size();

    const int bits = field.dictionary->index_bit_width;
    const size_t index_size = static_cast<size_t>(bits) / 8;
    checkBufferSize(indices_slice, rows * index_size, "dictionary indices");

    /// Materialize the indices (any width) into a UInt64 column so we can use IColumn::index to gather.
    auto indices = ColumnUInt64::create(rows);
    auto & idx = indices->getData();

    /// A row can be null either because the indices array marks it null (pyarrow style) or because it
    /// points at a null entry inside the dictionary values (the ClickHouse writer style); handle both.
    ColumnPtr index_null_map;
    const UInt8 * index_nulls = nullptr;
    if (field.nullable)
    {
        index_null_map = buildNullMap(validity, rows, null_count);
        index_nulls = assert_cast<const ColumnUInt8 &>(*index_null_map).getData().data();
    }

    for (size_t i = 0; i < rows; ++i)
    {
        UInt64 v = 0;
        switch (bits)
        {
            case 8: v = reinterpret_cast<const uint8_t *>(indices_slice.ptr)[i]; break;
            case 16: v = reinterpret_cast<const uint16_t *>(indices_slice.ptr)[i]; break;
            case 32: v = reinterpret_cast<const uint32_t *>(indices_slice.ptr)[i]; break;
            case 64: v = reinterpret_cast<const uint64_t *>(indices_slice.ptr)[i]; break;
            default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow dictionary index width {}", bits);
        }
        /// Null rows may carry an arbitrary (possibly out-of-range) index; gather position 0 and mask later.
        if (index_nulls && index_nulls[i])
            v = 0;
        else if (v >= dict_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow IPC dictionary index {} out of range (size {})", v, dict_size);
        idx[i] = v;
    }

    ColumnPtr full;
    if (dict_size == 0)
    {
        /// Empty dictionary: every row must be null; gather would be out of range.
        auto empty = values->cloneEmpty();
        empty->insertManyDefaults(rows);
        full = std::move(empty);
    }
    else
    {
        full = values->index(*indices, 0);
    }

    if (!field.nullable)
        return full;

    /// Ensure the result is Nullable and OR the index-validity nulls into whatever the gather produced.
    if (full->isNullable())
    {
        auto mutable_full = IColumn::mutate(std::move(full));
        auto & nm = assert_cast<ColumnNullable &>(*mutable_full).getNullMapData();
        for (size_t i = 0; i < rows; ++i)
            if (index_nulls[i])
                nm[i] = 1;
        return mutable_full;
    }
    return ColumnNullable::create(full, index_null_map);
}

ColumnPtr RecordBatchDecoder::decodeField(const ArrowField & field)
{
    const flatbuf::FieldNode & node = nextNode();
    const size_t rows = static_cast<size_t>(node.length());

    /// Every nullable-capable node carries a validity buffer slot first, then its value buffers.
    const Slice validity = nextBuffer();

    /// Dictionary-encoded fields carry indices here; the values come from a separate DictionaryBatch.
    if (field.dictionary)
        return decodeDictionary(field, rows, validity, node.null_count());

    ColumnPtr inner = decodeInner(field, rows);

    if (field.nullable)
    {
        ColumnPtr null_map = buildNullMap(validity, rows, node.null_count());
        return ColumnNullable::create(inner, null_map);
    }
    return inner;
}

std::vector<RecordBatchDecoder::DecodedColumn> RecordBatchDecoder::decodeColumns(
    const flatbuf::RecordBatch & batch, const PODArray<char> & body, const std::vector<ArrowField> & fields)
{
    if (batch.compression() != nullptr)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Compressed Arrow IPC bodies are not supported by the native reader yet");

    current_batch = &batch;
    current_body = &body;
    node_index = 0;
    buffer_index = 0;

    std::vector<DecodedColumn> result;
    result.reserve(fields.size());
    for (const ArrowField & field : fields)
    {
        DecodedColumn decoded;
        decoded.name = field.name;
        /// `fieldToCHType` ignores dictionary encoding, so it matches the materialized (full) column we build.
        decoded.type = fieldToCHType(field, settings, field.nullable);
        decoded.column = decodeField(field);
        result.push_back(std::move(decoded));
    }

    current_batch = nullptr;
    current_body = nullptr;
    return result;
}

std::vector<RecordBatchDecoder::DecodedColumn>
RecordBatchDecoder::decodeBatch(const flatbuf::RecordBatch & batch, const PODArray<char> & body)
{
    return decodeColumns(batch, body, schema.fields);
}

}

#endif
