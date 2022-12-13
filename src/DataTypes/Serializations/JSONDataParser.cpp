#include <DataTypes/Serializations/JSONDataParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/checkStackSize.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename ParserImpl>
std::optional<ParseResult> JSONDataParser<ParserImpl>::parse(const char * begin, size_t length)
{
    std::string_view json{begin, length};
    Element document;
    if (!parser.parse(json, document))
        return {};

    ParseContext context;
    traverse(document, context);

    ParseResult result;
    result.values = std::move(context.values);
    result.paths.reserve(context.paths.size());

    for (auto && path : context.paths)
        result.paths.emplace_back(std::move(path));

    return result;
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverse(const Element & element, ParseContext & ctx)
{
    checkStackSize();

    if (element.isObject())
    {
        traverseObject(element.getObject(), ctx);
    }
    else if (element.isArray())
    {
        traverseArray(element.getArray(), ctx);
    }
    else
    {
        ctx.paths.push_back(ctx.builder.getParts());
        ctx.values.push_back(getValueAsField(element));
    }
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverseObject(const JSONObject & object, ParseContext & ctx)
{
    ctx.paths.reserve(ctx.paths.size() + object.size());
    ctx.values.reserve(ctx.values.size() + object.size());

    for (auto it = object.begin(); it != object.end(); ++it)
    {
        const auto & [key, value] = *it;
        ctx.builder.append(key, false);
        traverse(value, ctx);
        ctx.builder.popBack();
    }
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverseArray(const JSONArray & array, ParseContext & ctx)
{
    /// Traverse elements of array and collect an array of fields by each path.
    ParseArrayContext array_ctx;
    array_ctx.total_size = array.size();

    for (auto it = array.begin(); it != array.end(); ++it)
    {
        traverseArrayElement(*it, array_ctx);
        ++array_ctx.current_size;
    }

    auto && arrays_by_path = array_ctx.arrays_by_path;

    if (arrays_by_path.empty())
    {
        ctx.paths.push_back(ctx.builder.getParts());
        ctx.values.push_back(Array());
    }
    else
    {
        ctx.paths.reserve(ctx.paths.size() + arrays_by_path.size());
        ctx.values.reserve(ctx.values.size() + arrays_by_path.size());

        for (auto && [_, value] : arrays_by_path)
        {
            auto && [path, path_array] = value;

            /// Merge prefix path and path of array element.
            ctx.paths.push_back(ctx.builder.append(path, true).getParts());
            ctx.values.push_back(std::move(path_array));
            ctx.builder.popBack(path.size());
        }
    }
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverseArrayElement(const Element & element, ParseArrayContext & ctx)
{
    ParseContext element_ctx;
    traverse(element, element_ctx);

    auto & [_, paths, values] = element_ctx;
    size_t size = paths.size();
    size_t keys_to_update = ctx.arrays_by_path.size();

    for (size_t i = 0; i < size; ++i)
    {
        if (values[i].isNull())
            continue;

        UInt128 hash = PathInData::getPartsHash(paths[i].begin(), paths[i].end());
        if (auto * found = ctx.arrays_by_path.find(hash))
        {
            auto & path_array = found->getMapped().second;
            assert(path_array.size() == ctx.current_size);

            /// If current element of array is part of Nested,
            /// collect its size or check it if the size of
            /// the Nested has been already collected.
            auto nested_hash = getHashOfNestedPath(paths[i], values[i]);
            if (nested_hash)
            {
                size_t array_size = values[i].template get<const Array &>().size();
                auto & current_nested_sizes = ctx.nested_sizes_by_path[*nested_hash];

                if (current_nested_sizes.size() == ctx.current_size)
                    current_nested_sizes.push_back(array_size);
                else if (array_size != current_nested_sizes.back())
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Array sizes mismatched ({} and {})", array_size, current_nested_sizes.back());
            }

            path_array.push_back(std::move(values[i]));
            --keys_to_update;
        }
        else
        {
            /// We found a new key. Add and empty array with current size.
            Array path_array;
            path_array.reserve(ctx.total_size);
            path_array.resize(ctx.current_size);

            auto nested_hash = getHashOfNestedPath(paths[i], values[i]);
            if (nested_hash)
            {
                size_t array_size = values[i].template get<const Array &>().size();
                auto & current_nested_sizes = ctx.nested_sizes_by_path[*nested_hash];

                if (current_nested_sizes.empty())
                {
                    current_nested_sizes.resize(ctx.current_size);
                }
                else
                {
                    /// If newly added element is part of the Nested then
                    /// resize its elements to keep correct sizes of Nested arrays.
                    for (size_t j = 0; j < ctx.current_size; ++j)
                        path_array[j] = Array(current_nested_sizes[j]);
                }

                if (current_nested_sizes.size() == ctx.current_size)
                    current_nested_sizes.push_back(array_size);
                else if (array_size != current_nested_sizes.back())
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Array sizes mismatched ({} and {})", array_size, current_nested_sizes.back());
            }

            path_array.push_back(std::move(values[i]));

            auto & elem = ctx.arrays_by_path[hash];
            elem.first = std::move(paths[i]);
            elem.second = std::move(path_array);
        }
    }

    /// If some of the keys are missed in current element,
    /// add default values for them.
    if (keys_to_update)
        fillMissedValuesInArrays(ctx);
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::fillMissedValuesInArrays(ParseArrayContext & ctx)
{
    for (auto & [_, value] : ctx.arrays_by_path)
    {
        auto & [path, path_array] = value;
        assert(path_array.size() == ctx.current_size || path_array.size() == ctx.current_size + 1);

        if (path_array.size() == ctx.current_size)
        {
            bool inserted = tryInsertDefaultFromNested(ctx, path, path_array);
            if (!inserted)
                path_array.emplace_back();
        }
    }
}

template <typename ParserImpl>
bool JSONDataParser<ParserImpl>::tryInsertDefaultFromNested(
    ParseArrayContext & ctx, const PathInData::Parts & path, Array & array)
{
    /// If there is a collected size of current Nested
    /// then insert array of this size as a default value.
    if (path.empty() || array.empty())
        return false;

    /// Last element is not Null, because otherwise this path wouldn't exist.
    auto hash = getHashOfNestedPath(path, array.back());
    if (!hash)
        return false;

    auto * mapped = ctx.nested_sizes_by_path.find(*hash);
    if (!mapped)
        return false;

    auto & current_nested_sizes = mapped->getMapped();
    assert(current_nested_sizes.size() == ctx.current_size || current_nested_sizes.size() == ctx.current_size + 1);

    /// If all keys of Nested were missed then add a zero length.
    if (current_nested_sizes.size() == ctx.current_size)
        current_nested_sizes.push_back(0);

    size_t array_size = current_nested_sizes.back();
    array.push_back(Array(array_size));
    return true;
}

template <typename ParserImpl>
Field JSONDataParser<ParserImpl>::getValueAsField(const Element & element)
{
    if (element.isBool())   return element.getBool();
    if (element.isInt64())  return element.getInt64();
    if (element.isUInt64()) return element.getUInt64();
    if (element.isDouble()) return element.getDouble();
    if (element.isString()) return element.getString();
    if (element.isNull())   return Field();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported type of JSON field");
}

template <typename ParserImpl>
std::optional<UInt128> JSONDataParser<ParserImpl>::getHashOfNestedPath(const PathInData::Parts & path, const Field & value)
{
    if (value.getType() != Field::Types::Array || path.empty())
        return {};

    /// Find first key that is marked as nested and return hash of its path.
    /// It's needed because we may have tuple of Nested and there could be
    /// several arrays with the same prefix, but with independent sizes.
    /// Consider we have array element with type `k2 Tuple(k3 Nested(...), k5 Nested(...))`
    /// Then subcolumns `k2.k3` and `k2.k5` may have indepented sizes and we should extract
    /// `k3` and `k5` keys instead of `k2`.

    for (size_t i = 0; i != path.size(); ++i)
        if (path[i].is_nested)
            return PathInData::getPartsHash(path.begin(), std::next(path.begin(), i + 1));

    return {};
}

#if USE_SIMDJSON
    template class JSONDataParser<SimdJSONParser>;
#endif

#if USE_RAPIDJSON
    template class JSONDataParser<RapidJSONParser>;
#endif

}
