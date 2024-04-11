#include <optional>

#include <base/defines.h>

#include <Common/Exception.h>

#include <Core/Field.h>
#include <Core/Streaming/CursorData.h>
#include <Core/Streaming/CursorTree.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

const String kCursor = "cursor";
const String kKeeperKey = "keeper_key";

Map packCursorData(const CursorData & data)
{
    Map result{
        Tuple{kCursor, data.tree.collapse()},
    };

    if (data.keeper_key.has_value())
        result.push_back(Tuple{kKeeperKey, data.keeper_key.value()});

    return result;
}

CursorData unpackCursorData(const Map & packed_data)
{
    chassert(packed_data.size() <= 2);

    std::optional<String> keeper_key;
    std::optional<CursorTree> tree;

    for (const auto & raw_tuple : packed_data)
    {
        const auto & tuple = raw_tuple.safeGet<const Tuple &>();
        const auto & key = tuple.at(0).safeGet<String>();

        if (key == kKeeperKey)
            keeper_key = tuple.at(1).safeGet<String>();
        else if (key == kCursor)
            tree = CursorTree(tuple.at(1).safeGet<Map>());
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid packed cursor format");
    }

    chassert(tree.has_value());

    return CursorData{
        .tree = std::move(tree.value()),
        .keeper_key = std::move(keeper_key),
    };
}

}

void readBinary(CursorDataMap & data_map, ReadBuffer & buf)
{
    Map raw;
    readBinary(raw, buf);

    for (const auto & raw_tuple : raw)
    {
        const auto & tuple = raw_tuple.safeGet<const Tuple &>();
        const auto & table_name = tuple.at(0).safeGet<String>();
        const auto & data = tuple.at(1).safeGet<Map>();

        auto result = data_map.try_emplace(table_name, unpackCursorData(data));

        chassert(result.second);
    }
}

void writeBinary(const CursorDataMap & data_map, WriteBuffer & buf)
{
    Map raw;

    for (const auto & [table_name, data] : data_map)
        raw.push_back(Tuple{table_name, packCursorData(data)});

    writeBinary(raw, buf);
}

}
