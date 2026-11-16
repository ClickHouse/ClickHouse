#include <Formats/FlattenTupleForCSVHeader.h>

#include <Core/Block.h>
#include <Core/CaseAwareBlockNameMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Formats/FormatSettings.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
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
    bool check_types,
    bool header_detection)
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
    for (const auto & column : header)
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(column.type.get());
        if (!tuple_type || tuple_type->getElements().empty())
            continue;
        size_t first_leaf = leaf_names.size();
        flattenField(column.name, column.type, leaf_names, leaf_types);
        candidates.push_back({&column, first_leaf, leaf_names.size() - first_leaf});
    }
    if (candidates.empty())
        return;

    auto spells = [&](const Candidate & candidate, size_t pos, bool ignore_case)
    {
        if (pos + candidate.num_leaves > names.size())
            return false;
        for (size_t k = 0; k < candidate.num_leaves; ++k)
        {
            std::string_view name = names[pos + k];
            std::string_view leaf = leaf_names[candidate.first_leaf + k];
            if (ignore_case ? !equalsCaseInsensitive(name, leaf) : name != leaf)
                return false;
        }
        return true;
    };

    using MatchingMode = FormatSettings::InputFormatColumnMatchingCaseSensitivity;
    const auto matching_mode = format_settings.input_format_column_matching_case_sensitivity;

    /// A name that is a column keeps mapping to it, so a run holding one is not collapsed.
    auto names_a_column = [&](const Candidate & candidate, size_t pos)
    {
        for (size_t k = 0; k < candidate.num_leaves; ++k)
        {
            const auto & name = names[pos + k];
            if (header_detection ? header.has(name, matching_mode != MatchingMode::MATCH_CASE)
                                 : column_indexes_by_names.get(name) != CaseAwareBlockNameMap::NOT_FOUND)
                return true;
        }
        return false;
    };

    auto find_candidate = [&](size_t pos, bool ignore_case) -> const Candidate *
    {
        const Candidate * found = nullptr;
        for (const auto & candidate : candidates)
        {
            if (!spells(candidate, pos, ignore_case) || names_a_column(candidate, pos))
                continue;
            if (found)
            {
                if (header_detection)
                    return nullptr;
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Ambiguous field (`{}`) in format header: it can be an element of Tuple column `{}` or `{}`",
                    names[pos], found->column->name, candidate.column->name);
            }
            found = &candidate;
        }
        return found;
    };

    auto match_at = [&](size_t pos) -> const Candidate *
    {
        /// Like a column name: an exact match first, then a case-insensitive one.
        const Candidate * match = matching_mode == MatchingMode::IGNORE_CASE ? nullptr : find_candidate(pos, false);
        if (!match && matching_mode != MatchingMode::MATCH_CASE)
            match = find_candidate(pos, true);
        return match;
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
