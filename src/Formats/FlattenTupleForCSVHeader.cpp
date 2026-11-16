#include <Formats/FlattenTupleForCSVHeader.h>

#include <Core/Block.h>
#include <Core/CaseAwareBlockNameMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Formats/FormatSettings.h>
#include <Common/Exception.h>
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
    const FormatSettings & format_settings,
    VectorWithMemoryTracking<String> & names,
    VectorWithMemoryTracking<String> & type_names,
    bool check_types)
{
    struct Candidate
    {
        const ColumnWithTypeAndName * column;
        size_t first_leaf;
        size_t num_leaves;
    };

    Names leaf_names; /// all candidates' leaves, contiguous per candidate
    Names leaf_types;
    VectorWithMemoryTracking<Candidate> candidates;
    VectorWithMemoryTracking<size_t> leaf_owner; /// leaf position -> candidate index
    for (const auto & column : header)
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(column.type.get());
        if (!tuple_type || tuple_type->getElements().empty())
            continue;
        size_t first_leaf = leaf_names.size();
        flattenField(column.name, column.type, leaf_names, leaf_types);
        candidates.push_back({&column, first_leaf, leaf_names.size() - first_leaf});
        leaf_owner.resize(leaf_names.size(), candidates.size() - 1);
    }
    if (candidates.empty())
        return;

    /// Built only after `leaf_names` is complete: the map keeps views into it.
    CaseAwareBlockNameMap leaf_indexes(format_settings.input_format_column_matching_case_sensitivity);
    leaf_indexes.setSize(leaf_names.size());
    for (size_t i = 0; i < leaf_names.size(); ++i)
        leaf_indexes.add(leaf_names[i], i);

    auto match_at = [&](size_t pos) -> const Candidate *
    {
        /// A name that is a column keeps mapping to it (checked first, before any leaf lookup).
        if (column_indexes_by_names.get(names[pos]) != CaseAwareBlockNameMap::NOT_FOUND)
            return nullptr;
        size_t leaf = leaf_indexes.get(names[pos]);
        if (leaf == CaseAwareBlockNameMap::NOT_FOUND)
            return nullptr;
        const auto & candidate = candidates[leaf_owner[leaf]];
        if (leaf != candidate.first_leaf || pos + candidate.num_leaves > names.size())
            return nullptr;
        for (size_t k = 1; k < candidate.num_leaves; ++k)
            if (column_indexes_by_names.get(names[pos + k]) != CaseAwareBlockNameMap::NOT_FOUND
                || leaf_indexes.get(names[pos + k]) != candidate.first_leaf + k)
                return nullptr;
        return &candidate;
    };

    const bool collapse_types = type_names.size() == names.size();
    VectorWithMemoryTracking<String> collapsed_names;
    VectorWithMemoryTracking<String> collapsed_type_names;
    for (size_t pos = 0; pos < names.size();)
    {
        const Candidate * match = match_at(pos);
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
                for (size_t k = 0; k < match->num_leaves; ++k)
                    if (type_names[pos + k] != leaf_types[match->first_leaf + k])
                        throw Exception(
                            ErrorCodes::INCORRECT_DATA,
                            "Type of '{}' must be {}, not {}",
                            leaf_names[match->first_leaf + k], leaf_types[match->first_leaf + k], type_names[pos + k]);
            collapsed_type_names.push_back(match->column->type->getName());
        }
        collapsed_names.push_back(match->column->name);
        pos += match->num_leaves;
    }

    names = std::move(collapsed_names);
    if (collapse_types)
        type_names = std::move(collapsed_type_names);
}

}
