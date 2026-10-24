#include <Processors/Formats/Impl/ORC/Vector.h>

#include <Common/Exception.h>
#include <Common/checkStackSize.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::ORC
{

std::unique_ptr<ColumnVectorBatch> createRowBatch(const Type & type, size_t capacity, bool encoded)
{
    checkStackSize();

    switch (type.getKind())
    {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DATE:
            return std::make_unique<LongVectorBatch>(capacity);
        case FLOAT:
        case DOUBLE:
            return std::make_unique<DoubleVectorBatch>(capacity);
        case STRING:
        case BINARY:
        case CHAR:
        case VARCHAR:
        case GEOMETRY:
        case GEOGRAPHY:
            if (encoded)
                return std::make_unique<EncodedStringVectorBatch>(capacity);
            return std::make_unique<StringVectorBatch>(capacity);
        case TIMESTAMP:
        case TIMESTAMP_INSTANT:
            return std::make_unique<TimestampVectorBatch>(capacity);
        case STRUCT:
        {
            auto result = std::make_unique<StructVectorBatch>(capacity);
            for (UInt64 i = 0; i < type.getSubtypeCount(); ++i)
                result->addField(createRowBatch(*type.getSubtype(i), capacity, encoded));
            return result;
        }
        case LIST:
        {
            auto result = std::make_unique<ListVectorBatch>(capacity);
            if (type.getSubtypeCount())
                result->elements = createRowBatch(*type.getSubtype(0), capacity, encoded);
            return result;
        }
        case MAP:
        {
            auto result = std::make_unique<MapVectorBatch>(capacity);
            if (type.getSubtypeCount() > 0)
                result->keys = createRowBatch(*type.getSubtype(0), capacity, encoded);
            if (type.getSubtypeCount() > 1)
                result->elements = createRowBatch(*type.getSubtype(1), capacity, encoded);
            return result;
        }
        case UNION:
        {
            auto result = std::make_unique<UnionVectorBatch>(capacity);
            for (UInt64 i = 0; i < type.getSubtypeCount(); ++i)
                result->addChild(createRowBatch(*type.getSubtype(i), capacity, encoded));
            return result;
        }
        case DECIMAL:
            if (type.getPrecision() == 0 || type.getPrecision() > 18)
                return std::make_unique<Decimal128VectorBatch>(capacity);
            return std::make_unique<Decimal64VectorBatch>(capacity);
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ORC type {} is not supported", type.toString());
}

}
