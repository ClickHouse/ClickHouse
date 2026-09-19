#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>

#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

/// Type-checked once here, so readers can `assert_cast` to `ColumnUInt8`.
ssize_t resolveFilterColumnPosition(const Block & header, const std::optional<String> & filter_column_name)
{
    if (!filter_column_name)
        return -1;

    const size_t position = header.getPositionByName(*filter_column_name);
    const auto & filter_type = header.getByPosition(position).type;
    if (!WhichDataType(filter_type).isUInt8())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Illegal type {} of column for filter. Must be UInt8", filter_type->getName());

    return static_cast<ssize_t>(position);
}

}
