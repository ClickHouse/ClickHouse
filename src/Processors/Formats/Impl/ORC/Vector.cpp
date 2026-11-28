#include <Processors/Formats/Impl/ORC/Vector.h>

#include <Common/Exception.h>
#include <Common/checkStackSize.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}
}

namespace DB::ORC
{

ColumnPtr decodeDictionary(const ColumnString & dictionary, const Int64 * index, const char * not_null, size_t num_values)
{
    const size_t dictionary_size = dictionary.size();
    const auto & dictionary_offsets = dictionary.getOffsets();
    const auto & dictionary_chars = dictionary.getChars();

    auto result = ColumnString::create();
    auto & chars = result->getChars();
    auto & offsets = result->getOffsets();
    offsets.resize(num_values);

    size_t total = 0;
    for (size_t i = 0; i < num_values; ++i)
    {
        if (!not_null || not_null[i])
        {
            Int64 entry = index[i];
            if (entry < 0 || static_cast<size_t>(entry) >= dictionary_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "ORC dictionary index {} at row {} is out of range [0, {})", entry, i, dictionary_size);
            total += dictionary_offsets[entry] - dictionary_offsets[entry - 1];
        }
        offsets[i] = total;
    }

    chars.resize(total);
    UInt8 * out = chars.data();
    for (size_t i = 0; i < num_values; ++i)
    {
        if (!not_null || not_null[i])
        {
            size_t begin = dictionary_offsets[index[i] - 1];
            size_t size = dictionary_offsets[index[i]] - begin;
            memcpy(out, dictionary_chars.data() + begin, size);
            out += size;
        }
    }
    return result;
}

ColumnPtr getStringColumn(const StringVectorBatch & batch)
{
    if (!batch.isEncoded)
        return batch.column;

    const auto & encoded = typeid_cast<const EncodedStringVectorBatch &>(batch);
    return decodeDictionary(
        assert_cast<const ColumnString &>(*encoded.dictionary->column),
        encoded.index.data(),
        batch.hasNulls ? batch.notNull.data() : nullptr,
        batch.numElements);
}

std::unique_ptr<ColumnVectorBatch> createRowBatch(const Type & type, bool encoded)
{
    checkStackSize();

    switch (type.getKind())
    {
        case BOOLEAN:
            return std::make_unique<BoolVectorBatch>();
        case BYTE:
            return std::make_unique<ByteVectorBatch>();
        case SHORT:
            return std::make_unique<ShortVectorBatch>();
        case INT:
            return std::make_unique<IntVectorBatch>();
        case LONG:
        case DATE:
            return std::make_unique<LongVectorBatch>();
        case FLOAT:
            return std::make_unique<FloatVectorBatch>();
        case DOUBLE:
            return std::make_unique<DoubleVectorBatch>();
        case STRING:
        case BINARY:
        case CHAR:
        case VARCHAR:
        case GEOMETRY:
        case GEOGRAPHY:
            if (encoded)
                return std::make_unique<EncodedStringVectorBatch>();
            return std::make_unique<StringVectorBatch>();
        case TIMESTAMP:
        case TIMESTAMP_INSTANT:
            return std::make_unique<TimestampVectorBatch>();
        case STRUCT:
        {
            auto result = std::make_unique<StructVectorBatch>();
            for (UInt64 i = 0; i < type.getSubtypeCount(); ++i)
                result->addField(createRowBatch(*type.getSubtype(i), encoded));
            return result;
        }
        case LIST:
        {
            auto result = std::make_unique<ListVectorBatch>();
            if (type.getSubtypeCount())
                result->elements = createRowBatch(*type.getSubtype(0), encoded);
            return result;
        }
        case MAP:
        {
            auto result = std::make_unique<MapVectorBatch>();
            if (type.getSubtypeCount() > 0)
                result->keys = createRowBatch(*type.getSubtype(0), encoded);
            if (type.getSubtypeCount() > 1)
                result->elements = createRowBatch(*type.getSubtype(1), encoded);
            return result;
        }
        case UNION:
        {
            auto result = std::make_unique<UnionVectorBatch>();
            for (UInt64 i = 0; i < type.getSubtypeCount(); ++i)
                result->addChild(createRowBatch(*type.getSubtype(i), encoded));
            return result;
        }
        case DECIMAL:
            if (type.getPrecision() == 0 || type.getPrecision() > 18)
                return std::make_unique<Decimal128VectorBatch>();
            return std::make_unique<Decimal64VectorBatch>();
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ORC type {} is not supported", type.toString());
}

}
