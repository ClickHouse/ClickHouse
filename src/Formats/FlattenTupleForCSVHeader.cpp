#include <Formats/FlattenTupleForCSVHeader.h>

#include <Core/Block.h>
#include <Core/CaseAwareBlockNameMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/Exception.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/isValidUTF8.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{
void flattenField(const String & name, const DataTypePtr & type, Names & names, Names & type_names)
{
    /// Only a bare Tuple is flattened. A Nullable(Tuple(...)) is serialized as a single CSV field
    /// (SerializationNullable::serializeTextCSV writes it as one quoted value, matching the single
    /// \\N cell of a NULL row), so its header must stay one cell too. Do not unwrap Nullable here.
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & elems = tuple_type->getElements();
        /// An empty tuple still occupies exactly one field: the value serializer writes nothing
        /// between the surrounding delimiters. Emit one header cell so widths agree.
        if (elems.empty())
        {
            names.push_back(name);
            type_names.push_back(type->getName());
            return;
        }

        const auto & elem_names = tuple_type->getElementNames();
        for (size_t i = 0; i < elems.size(); ++i)
            flattenField(name + "." + elem_names[i], elems[i], names, type_names);
    }
    else
    {
        names.push_back(name);
        type_names.push_back(type->getName());
    }
}
}

void getCSVHeaderNamesAndTypes(const Block & sample, bool flatten, Names & names, Names & type_names)
{
    for (const auto & elem : sample)
    {
        if (flatten)
            flattenField(elem.name, elem.type, names, type_names);
        else
        {
            names.push_back(elem.name);
            type_names.push_back(elem.type->getName());
        }
    }
}

bool csvHeaderNamesMayProduceRawBytes(const Block & sample, bool flatten, bool with_names, bool with_types)
{
    if (!with_names && !with_types)
        return false;

    Names names;
    Names type_names;
    getCSVHeaderNamesAndTypes(sample, flatten, names, type_names);

    auto is_not_valid_utf8 = [](const std::string & s)
    {
        return !UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(s.data()), s.size());
    };

    if (with_names)
        for (const auto & name : names)
            if (is_not_valid_utf8(name))
                return true;

    if (with_types)
        for (const auto & type_name : type_names)
            if (is_not_valid_utf8(type_name))
                return true;

    return false;
}

void collapseCSVHeaderTupleNames(
    const Block & header,
    const CaseAwareBlockNameMap & column_indexes_by_names,
    VectorWithMemoryTracking<String> & names,
    VectorWithMemoryTracking<String> & type_names,
    bool check_types)
{
    struct Candidate
    {
        const ColumnWithTypeAndName * column;
        Names leaf_names;
        Names leaf_types;
    };

    VectorWithMemoryTracking<Candidate> candidates;
    for (const auto & column : header)
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(column.type.get());
        if (!tuple_type || tuple_type->getElements().empty())
            continue;
        auto & candidate = candidates.emplace_back(Candidate{&column, {}, {}});
        flattenField(column.name, column.type, candidate.leaf_names, candidate.leaf_types);
    }
    if (candidates.empty())
        return;

    UnorderedMapWithMemoryTracking<std::string_view, VectorWithMemoryTracking<size_t>> candidates_by_first_leaf;
    for (size_t i = 0; i < candidates.size(); ++i)
        candidates_by_first_leaf[candidates[i].leaf_names.front()].push_back(i);

    auto matches_at = [&](const Candidate & candidate, size_t pos)
    {
        const auto & leaves = candidate.leaf_names;
        if (pos + leaves.size() > names.size())
            return false;
        for (size_t k = 0; k < leaves.size(); ++k)
            if (names[pos + k] != leaves[k] || column_indexes_by_names.get(names[pos + k]) != CaseAwareBlockNameMap::NOT_FOUND)
                return false;
        return true;
    };

    const bool collapse_types = type_names.size() == names.size();
    VectorWithMemoryTracking<String> collapsed_names;
    VectorWithMemoryTracking<String> collapsed_type_names;
    for (size_t pos = 0; pos < names.size();)
    {
        const Candidate * match = nullptr;
        if (auto it = candidates_by_first_leaf.find(names[pos]); it != candidates_by_first_leaf.end())
            for (size_t i : it->second)
                if (matches_at(candidates[i], pos))
                {
                    match = &candidates[i];
                    break;
                }

        if (!match)
        {
            collapsed_names.push_back(std::move(names[pos]));
            if (collapse_types)
                collapsed_type_names.push_back(std::move(type_names[pos]));
            ++pos;
            continue;
        }

        if (collapse_types)
        {
            if (check_types)
                for (size_t k = 0; k < match->leaf_types.size(); ++k)
                    if (type_names[pos + k] != match->leaf_types[k])
                        throw Exception(
                            ErrorCodes::INCORRECT_DATA,
                            "Type of '{}' must be {}, not {}",
                            match->leaf_names[k], match->leaf_types[k], type_names[pos + k]);
            collapsed_type_names.push_back(match->column->type->getName());
        }
        collapsed_names.push_back(match->column->name);
        pos += match->leaf_names.size();
    }

    names = std::move(collapsed_names);
    if (collapse_types)
        type_names = std::move(collapsed_type_names);
}

}
