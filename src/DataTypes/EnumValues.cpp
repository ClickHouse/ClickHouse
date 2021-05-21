#include <DataTypes/EnumValues.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int EMPTY_DATA_PASSED;
    extern const int BAD_ARGUMENTS;
}

template <typename T>
EnumValues<T>::EnumValues(const Values & values_)
    : values(values_)
{
    if (values.empty())
        throw Exception{"DataTypeEnum enumeration cannot be empty", ErrorCodes::EMPTY_DATA_PASSED};

    std::sort(std::begin(values), std::end(values), [] (auto & left, auto & right)
    {
        return left.second < right.second;
    });

    fillMaps();
}

template <typename T>
void EnumValues<T>::fillMaps()
{
    for (const auto & name_and_value : values)
    {
        const auto inserted_value = name_to_value_map.insert(
            { StringRef{name_and_value.first}, name_and_value.second });

        if (!inserted_value.second)
            throw Exception{"Duplicate names in enum: '" + name_and_value.first + "' = " + toString(name_and_value.second)
                    + " and " + toString(inserted_value.first->getMapped()),
                ErrorCodes::SYNTAX_ERROR};

        const auto inserted_name = value_to_name_map.insert(
            { name_and_value.second, StringRef{name_and_value.first} });

        if (!inserted_name.second)
            throw Exception{"Duplicate values in enum: '" + name_and_value.first + "' = " + toString(name_and_value.second)
                    + " and '" + toString((*inserted_name.first).first) + "'",
                ErrorCodes::SYNTAX_ERROR};
    }
}

template <typename T>
T EnumValues<T>::getValue(StringRef field_name, bool try_treat_as_id) const
{
    const auto it = name_to_value_map.find(field_name);
    if (!it)
    {
        /// It is used in CSV and TSV input formats. If we fail to find given string in
        /// enum names, we will try to treat it as enum id.
        if (try_treat_as_id)
        {
            T x;
            ReadBufferFromMemory tmp_buf(field_name.data, field_name.size);
            readText(x, tmp_buf);
            /// Check if we reached end of the tmp_buf (otherwise field_name is not a number)
            /// and try to find it in enum ids
            if (tmp_buf.eof() && value_to_name_map.find(x) != value_to_name_map.end())
                return x;
        }
        auto hints = this->getHints(field_name.toString());
        auto hints_string = !hints.empty() ? ", maybe you meant: " + toString(hints) : "";
        throw Exception{"Unknown element '" + field_name.toString() + "' for enum" + hints_string, ErrorCodes::BAD_ARGUMENTS};
    }
    return it->getMapped();
}

template <typename T>
Names EnumValues<T>::getAllRegisteredNames() const
{
    Names result;
    for (const auto & value : values)
        result.emplace_back(value.first);
    return result;
}

template class EnumValues<Int8>;
template class EnumValues<Int16>;

}
