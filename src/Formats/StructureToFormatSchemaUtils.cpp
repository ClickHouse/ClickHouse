#include <Formats/StructureToFormatSchemaUtils.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace StructureToFormatSchemaUtils
{

void writeIndent(WriteBuffer & buf, size_t indent)
{
    writeChar(' ', indent * 4, buf);
}

void startNested(WriteBuffer & buf, const String & nested_name, const String & nested_type, size_t indent)
{
    writeIndent(buf, indent);
    writeString(nested_type, buf);
    if (!nested_name.empty())
    {
        writeChar(' ', buf);
        writeString(nested_name, buf);
    }
    writeChar('\n', buf);
    writeIndent(buf, indent);
    writeCString("{\n", buf);
}

void endNested(WriteBuffer & buf, size_t indent)
{
    writeIndent(buf, indent);
    writeCString("}\n", buf);
}

String getSchemaFieldName(const String & column_name)
{
    String result = column_name;
    /// Replace all first uppercase letters to lower-case,
    /// because fields in CapnProto schema must begin with a lower-case letter.
    /// Don't replace all letters to lower-case to remain camelCase field names.
    for (auto & symbol : result)
    {
        if (islower(symbol))
            break;
        symbol = tolower(symbol);
    }
    return result;
}

String getSchemaMessageName(const String & column_name)
{
    String result = column_name;
    if (!column_name.empty() && isalpha(column_name[0]))
        result[0] = toupper(column_name[0]);
    return result;
}

namespace
{
    std::pair<String, String> splitName(const String & name, bool allow_split_by_underscore)
    {
        const auto * begin = name.data();
        const auto * end = name.data() + name.size();
        const char * it = nullptr;
        if (allow_split_by_underscore)
            it = find_first_symbols<'_', '.'>(begin, end);
        else
            it = find_first_symbols<'.'>(begin, end);
        String first = String(begin, it);
        String second = it == end ? "" : String(it + 1, end);
        return {std::move(first), std::move(second)};
    }
}

NamesAndTypesList collectNested(const NamesAndTypesList & names_and_types, bool allow_split_by_underscore, const String & format_name)
{
    /// Find all columns with dots '.' or underscores '_' (if allowed) and move them into a tuple.
    /// For example if we have columns 'a.b UInt32, a.c UInt32, x_y String' we will
    /// change it to 'a Tuple(b UInt32, c UInt32), x Tuple(y String)'
    NamesAndTypesList result;
    std::unordered_map<String, NamesAndTypesList> nested;
    for (const auto & [name, type] : names_and_types)
    {
        auto [field_name, nested_name] = splitName(name, allow_split_by_underscore);
        if (isdigit(field_name[0]))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Format {} doesn't support field names that starts with a digit: '{}'", format_name, field_name);

        if (nested_name.empty())
            result.emplace_back(name, type);
        else
            nested[field_name].emplace_back(nested_name, type);
    }

    /// Collect nested recursively.
    for (auto & [field_name, elements] : nested)
        elements = collectNested(elements, allow_split_by_underscore, format_name);

    for (const auto & [field_name, elements]: nested)
        result.emplace_back(field_name, std::make_shared<DataTypeTuple>(elements.getTypes(), elements.getNames()));

    return result;
}

NamesAndTypesList getCollectedTupleElements(const DataTypeTuple & tuple_type, bool allow_split_by_underscore, const String & format_name)
{
    const auto & nested_types = tuple_type.getElements();
    Names nested_names;
    if (tuple_type.haveExplicitNames())
    {
        nested_names = tuple_type.getElementNames();
    }
    else
    {
        nested_names.reserve(nested_types.size());
        for (size_t i = 0; i != nested_types.size(); ++i)
            nested_names.push_back("e" + std::to_string(i + 1));
    }

    NamesAndTypesList result;
    for (size_t i = 0; i != nested_names.size(); ++i)
        result.emplace_back(nested_names[i], nested_types[i]);

    return collectNested(result, allow_split_by_underscore, format_name);
}

}

}
