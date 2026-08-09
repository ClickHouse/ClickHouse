#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Core/Field.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Formats/JSONUtils.h>
#include <Formats/ParseError.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/WriteBufferFromString.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
}


static inline IColumn & extractElementColumn(IColumn & column, size_t idx)
{
    return assert_cast<ColumnTuple &>(column).getColumn(idx);
}

static inline const IColumn & extractElementColumn(const IColumn & column, size_t idx)
{
    return assert_cast<const ColumnTuple &>(column).getColumn(idx);
}

static std::optional<FormatSettings> getInteriorTupleCSVSettings(const FormatSettings & settings, size_t elements)
{
    if (elements < 2
        || (!settings.csv.force_quote_date_time_types
            && settings.csv.delimiter == settings.csv.tuple_delimiter
            && settings.csv.custom_delimiter.empty()))
        return std::nullopt;

    FormatSettings result = settings;
    result.csv.delimiter = settings.csv.tuple_delimiter;
    result.csv.custom_delimiter.clear();
    result.csv.force_quote_date_time_types = false;
    return result;
}

static bool tupleMayUseWholeCSVField(
    const FormatSettings & settings,
    const SerializationTuple::ElementSerializations & elements,
    const std::optional<FormatSettings> & interior_settings)
{
    if (settings.csv.tuple_delimiter_matches_field_delimiter || !interior_settings)
        return false;

    FormatSettings quote_check_settings = *interior_settings;
    quote_check_settings.csv.quote_date_time_types = false;
    quote_check_settings.csv.force_quote_date_time_types = false;

    for (const auto & element : elements)
    {
        if (element->textCSVMayNeedQuotes(quote_check_settings))
            return true;
    }
    return false;
}

static bool tupleNeedsWholeCSVField(
    const FormatSettings & settings,
    const SerializationTuple::ElementSerializations & elements,
    const std::optional<FormatSettings> & interior_settings,
    const IColumn & column,
    size_t row_num)
{
    if (settings.csv.tuple_delimiter_matches_field_delimiter || !interior_settings)
        return false;

    FormatSettings quote_check_settings = *interior_settings;
    quote_check_settings.csv.quote_date_time_types = false;
    quote_check_settings.csv.force_quote_date_time_types = false;

    for (size_t i = 0; i < elements.size(); ++i)
    {
        if (elements[i]->textCSVNeedsQuotes(extractElementColumn(column, i), row_num, quote_check_settings))
            return true;
    }
    return false;
}

UInt128 SerializationTuple::getHash(const ElementSerializations & elems_, bool has_explicit_names_)
{
    SipHash hash;
    hash.update("Tuple");
    hash.update(has_explicit_names_);
    for (const auto & elem : elems_)
    {
        if (has_explicit_names_)
        {
            hash.update(elem->getElementName().size());
            hash.update(elem->getElementName());
        }
        hash.update(elem->getNested()->getHash());
    }
    return hash.get128();
}

void SerializationTuple::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & tuple = field.safeGet<Tuple>();
    for (size_t element_index = 0; element_index < elems.size(); ++element_index)
    {
        const auto & serialization = elems[element_index];
        serialization->serializeBinary(tuple[element_index], ostr, settings);
    }
}

void SerializationTuple::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    const size_t size = elems.size();

    field = Tuple();
    Tuple & tuple = field.safeGet<Tuple>();
    tuple.reserve(size);
    for (size_t i = 0; i < size; ++i)
        elems[i]->deserializeBinary(tuple.emplace_back(), istr, settings);
}

void SerializationTuple::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    for (size_t element_index = 0; element_index < elems.size(); ++element_index)
    {
        const auto & serialization = elems[element_index];
        serialization->serializeBinary(extractElementColumn(column, element_index), row_num, ostr, settings);
    }
}

void SerializationTuple::serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    for (size_t element_index = 0; element_index < elems.size(); ++element_index)
    {
        const auto & serialization = elems[element_index];
        serialization->serializeForHashCalculation(extractElementColumn(column, element_index), row_num, ostr);
    }
}


template <typename ReturnType, typename F>
static ReturnType addElementSafe(size_t num_elems, IColumn & column, F && impl)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// We use the assumption that tuples of zero size do not exist.
    size_t old_size = column.size();

    auto restore_elements = [&]()
    {
        for (size_t i = 0; i < num_elems; ++i)
        {
            auto & element_column = extractElementColumn(column, i);
            if (element_column.size() > old_size)
            {
                chassert(element_column.size() - old_size == 1);
                element_column.popBack(1);
            }
        }
    };

    try
    {
        if (!impl())
        {
            restore_elements();
            return ReturnType(false);
        }

        assert_cast<ColumnTuple &>(column).addSize(1);


        // Check that all columns now have the same size.
        size_t new_size = column.size();
        for (size_t i = 1; i < num_elems; ++i)
        {
            const auto & element_column = extractElementColumn(column, i);
            if (element_column.size() != new_size)
            {
                restore_elements();
                // This is not a logical error because it may work with
                // user-supplied data.
                if constexpr (throw_exception)
                    throw Exception(ErrorCodes::SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH,
                        "Cannot read a tuple because not all elements are present");
                return ReturnType(false);
            }
        }
    }
    catch (...)
    {
        restore_elements();
        if constexpr (throw_exception)
            throw;
        /// Only a genuine parse failure means "this value did not parse"; other errors
        /// (e.g. MEMORY_LIMIT_EXCEEDED) must propagate instead of being reported as a
        /// failed parse and silently turned into a default/skip.
        rethrowIfNotParseError();
        return ReturnType(false);
    }

    return ReturnType(true);
}

void SerializationTuple::readElementsSafe(DB::IColumn & column, std::function<void()> && read_func)
{
    addElementSafe<void>(assert_cast<ColumnTuple &>(column).getColumns().size(), column, [&](){ read_func(); return true; });
}

SerializationPtr SerializationTuple::create(ElementSerializations elems_, bool has_explicit_names_)
{
    for (const auto & elem : elems_)
    {
        if (!elem->supportsPooling())
            return std::shared_ptr<ISerialization>(new SerializationTuple(std::move(elems_), has_explicit_names_));
    }
    auto hash = getHash(elems_, has_explicit_names_);
    return ISerialization::pooled(hash, [e = std::move(elems_), has_explicit_names_]() mutable { return new SerializationTuple(std::move(e), has_explicit_names_); });
}

void SerializationTuple::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    addElementSafe<void>(elems.size(), column, [&]
    {
        for (size_t i = 0; i < elems.size(); ++i)
            elems[i]->deserializeBinary(extractElementColumn(column, i), istr, settings);
        return true;
    });
}

void SerializationTuple::serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const size_t level = settings.hive_text.nesting_level;
    const char separator = getHiveTextDelimiter(settings, level);

    auto child_settings = settings;
    child_settings.hive_text.nesting_level = level + 1;

    for (size_t i = 0; i < elems.size(); ++i)
    {
        if (i != 0)
            writeChar(separator, ostr);

        elems[i]->serializeTextHive(extractElementColumn(column, i), row_num, ostr, child_settings);
    }
}

void SerializationTuple::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.pretty_format && settings.pretty.named_tuples_as_json && has_explicit_names)
    {
        serializeTextJSONPretty(column, row_num, ostr, settings, 1);
    }
    else
    {
        writeChar('(', ostr);
        for (size_t i = 0; i < elems.size(); ++i)
        {
            if (i != 0)
                writeChar(',', ostr);
            elems[i]->serializeTextQuoted(extractElementColumn(column, i), row_num, ostr, settings);
        }
        writeChar(')', ostr);
    }
}

template <typename ReturnType>
ReturnType SerializationTuple::deserializeTextImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    const size_t size = elems.size();
    if constexpr (throw_exception)
        assertChar('(', istr);
    else if (!checkChar('(', istr))
        return ReturnType(false);

    auto impl = [&]()
    {
        for (size_t i = 0; i < size; ++i)
        {
            skipWhitespaceIfAny(istr);
            if (i != 0)
            {
                if constexpr (throw_exception)
                    assertChar(',', istr);
                else if (!checkChar(',', istr))
                    return false;

                skipWhitespaceIfAny(istr);
            }

            auto & element_column = extractElementColumn(column, i);
            if constexpr (throw_exception)
            {
                if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(element_column))
                    SerializationNullable::deserializeNullAsDefaultOrNestedTextQuoted(element_column, istr, settings, elems[i]);
                else
                    elems[i]->deserializeTextQuoted(element_column, istr, settings);
            }
            else
            {
                bool ok = false;
                if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(element_column))
                    ok = SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextQuoted(element_column, istr, settings, elems[i]);
                else
                    ok = elems[i]->tryDeserializeTextQuoted(element_column, istr, settings);

                if (!ok)
                    return false;
            }
        }

        // Special format for one element tuple (1,)
        if (1 == elems.size())
        {
            skipWhitespaceIfAny(istr);
            // Allow both (1) and (1,)
            checkChar(',', istr);
        }

        skipWhitespaceIfAny(istr);
        if constexpr (throw_exception)
            assertChar(')', istr);
        else if (!checkChar(')', istr))
            return false;

        if (whole && !istr.eof())
        {
            if constexpr (throw_exception)
            {
                /// If empty tuple, temporarily increase size to make sure we can read the parsed
                /// value via serializeText.
                if (elems.empty())
                    assert_cast<ColumnTuple &>(column).addSize(1);
                WriteBufferFromOwnString ostr;
                serializeText(column, column.size() - 1, ostr, settings);

                /// Revert the temporarily added size increment for empty tuple.
                if (elems.empty())
                    assert_cast<ColumnTuple &>(column).popBack(1);

                throw Exception(
                    ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE,
                    "Unexpected data '{}' after parsed Tuple value '{}'",
                    std::string(istr.position(), std::min(size_t(10), istr.available())),
                    ostr.str());
            }
            return false;
        }

        return true;
    };

    return addElementSafe<ReturnType>(elems.size(), column, impl);
}

void SerializationTuple::deserializeText(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, bool whole) const
{
    deserializeTextImpl(column, istr, settings, whole);
}

bool SerializationTuple::tryDeserializeText(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, bool whole) const
{
    return deserializeTextImpl<bool>(column, istr, settings, whole);
}

void SerializationTuple::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.json.write_named_tuples_as_objects
        && has_explicit_names)
    {
        writeChar('{', ostr);

        bool first = true;
        for (size_t i = 0; i < elems.size(); ++i)
        {
            const auto & element_column = extractElementColumn(column, i);
            if (settings.json.skip_null_value_in_named_tuples && element_column.isNullAt(row_num))
                continue;

            if (!first)
                writeChar(',', ostr);

            writeJSONString(elems[i]->getElementName(), ostr, settings);
            writeChar(':', ostr);
            elems[i]->serializeTextJSON(element_column, row_num, ostr, settings);
            first = false;
        }

        writeChar('}', ostr);
    }
    else
    {
        writeChar('[', ostr);
        for (size_t i = 0; i < elems.size(); ++i)
        {
            if (i != 0)
                writeChar(',', ostr);
            elems[i]->serializeTextJSON(extractElementColumn(column, i), row_num, ostr, settings);
        }
        writeChar(']', ostr);
    }
}

void SerializationTuple::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    if (settings.json.write_named_tuples_as_objects
        && has_explicit_names)
    {
        writeCString("{\n", ostr);

        bool first = true;
        for (size_t i = 0; i < elems.size(); ++i)
        {
            const auto & element_column = extractElementColumn(column, i);
            if (settings.json.skip_null_value_in_named_tuples && element_column.isNullAt(row_num))
                continue;

            if (!first)
                writeCString(",\n", ostr);

            writeChar(settings.json.pretty_print_indent, (indent + 1) * settings.json.pretty_print_indent_multiplier, ostr);
            writeJSONString(elems[i]->getElementName(), ostr, settings);
            writeCString(": ", ostr);
            elems[i]->serializeTextJSONPretty(extractElementColumn(column, i), row_num, ostr, settings, indent + 1);
            first = false;
        }

        writeChar('\n', ostr);
        const auto final_indent = indent * settings.json.pretty_print_indent_multiplier;
        if (final_indent > 1)
            writeChar(settings.json.pretty_print_indent, final_indent, ostr);
        writeChar('}', ostr);
    }
    else
    {
        writeCString("[\n", ostr);
        for (size_t i = 0; i < elems.size(); ++i)
        {
            if (i != 0)
                writeCString(",\n", ostr);
            writeChar(settings.json.pretty_print_indent, (indent + 1) * settings.json.pretty_print_indent_multiplier, ostr);
            elems[i]->serializeTextJSONPretty(extractElementColumn(column, i), row_num, ostr, settings, indent + 1);
        }
        writeChar('\n', ostr);
        writeChar(settings.json.pretty_print_indent, indent * settings.json.pretty_print_indent_multiplier, ostr);
        writeChar(']', ostr);
    }
}

template <typename ReturnType>
ReturnType SerializationTuple::deserializeTupleJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, auto && deserialize_element) const
{
    static constexpr auto throw_exception = std::is_same_v<ReturnType, void>;

    if (settings.json.read_named_tuples_as_objects
        && has_explicit_names)
    {
        skipWhitespaceIfAny(istr);
        if constexpr (throw_exception)
            assertChar('{', istr);
        else if (!checkChar('{', istr))
            return ReturnType(false);
        skipWhitespaceIfAny(istr);

        auto impl = [&]()
        {
            std::vector<UInt8> seen_elements(elems.size(), 0);
            size_t processed = 0;
            size_t skipped = 0;
            while (!istr.eof() && *istr.position() != '}')
            {
                if (!settings.json.ignore_unknown_keys_in_named_tuple && processed == elems.size())
                {
                    if constexpr (throw_exception)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected number of elements in named tuple. Expected no more than {} (consider enabling input_format_json_ignore_unknown_keys_in_named_tuple setting)", elems.size());
                    return false;
                }

                if (processed + skipped > 0)
                {
                    if constexpr (throw_exception)
                        assertChar(',', istr);
                    else if (!checkChar(',', istr))
                        return false;
                    skipWhitespaceIfAny(istr);
                }

                std::string name;
                if constexpr (throw_exception)
                    readDoubleQuotedString(name, istr);
                else if (!tryReadDoubleQuotedString(name, istr))
                    return false;

                skipWhitespaceIfAny(istr);
                if constexpr (throw_exception)
                    assertChar(':', istr);
                else if (!checkChar(':', istr))
                    return false;
                skipWhitespaceIfAny(istr);

                const size_t element_pos = getPositionByName(name);
                if (element_pos == std::numeric_limits<size_t>::max())
                {
                    if (settings.json.ignore_unknown_keys_in_named_tuple)
                    {
                        if constexpr (throw_exception)
                            skipJSONField(istr, name, settings.json);
                        else if (!trySkipJSONField(istr, name, settings.json))
                            return false;

                        skipWhitespaceIfAny(istr);
                        ++skipped;
                        continue;
                    }

                    if constexpr (throw_exception)
                        throw Exception(
                            ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                            "Tuple doesn't have element with name '{}', enable setting "
                            "input_format_json_ignore_unknown_keys_in_named_tuple",
                            name);
                    return false;
                }

                if (seen_elements[element_pos])
                    throw Exception(ErrorCodes::INCORRECT_DATA, "JSON object contains duplicate key '{}'", name);

                seen_elements[element_pos] = 1;
                auto & element_column = extractElementColumn(column, element_pos);

                if constexpr (throw_exception)
                {
                    try
                    {
                        deserialize_element(element_column, element_pos);
                    }
                    catch (Exception & e)
                    {
                        e.addMessage("(while reading the value of nested key " + name + ")");
                        throw;
                    }
                }
                else
                {
                    if (!deserialize_element(element_column, element_pos))
                        return false;
                }

                skipWhitespaceIfAny(istr);
                ++processed;
            }

            if constexpr (throw_exception)
                assertChar('}', istr);
            else if (!checkChar('}', istr))
                return false;

            /// Check if we have missing elements.
            if (processed != elems.size())
            {
                for (size_t element_pos = 0; element_pos != seen_elements.size(); ++element_pos)
                {
                    if (seen_elements[element_pos])
                        continue;

                    if (!settings.json.defaults_for_missing_elements_in_named_tuple)
                    {
                        if constexpr (throw_exception)
                            throw Exception(
                                ErrorCodes::INCORRECT_DATA,
                                "JSON object doesn't contain tuple element {}. If you want to insert defaults in case of missing elements, "
                                "enable setting input_format_json_defaults_for_missing_elements_in_named_tuple",
                                elems[element_pos]->getElementName());
                        return false;
                    }

                    auto & element_column = extractElementColumn(column, element_pos);
                    element_column.insertDefault();
                }
            }

            return true;
        };

        return addElementSafe<ReturnType>(elems.size(), column, impl);
    }

    skipWhitespaceIfAny(istr);
    if constexpr (throw_exception)
        assertChar('[', istr);
    else if (!checkChar('[', istr))
        return false;
    skipWhitespaceIfAny(istr);

    auto impl = [&]()
    {
        for (size_t i = 0; i < elems.size(); ++i)
        {
            skipWhitespaceIfAny(istr);
            if (i != 0)
            {
                if constexpr (throw_exception)
                    assertChar(',', istr);
                else if (!checkChar(',', istr))
                    return false;
                skipWhitespaceIfAny(istr);
            }

            auto & element_column = extractElementColumn(column, i);

            if constexpr (throw_exception)
                deserialize_element(element_column, i);
            else if (!deserialize_element(element_column, i))
                return false;
        }

        skipWhitespaceIfAny(istr);
        if constexpr (throw_exception)
            assertChar(']', istr);
        else if (!checkChar(']', istr))
            return false;

        return true;
    };

    return addElementSafe<ReturnType>(elems.size(), column, impl);
}

template <typename ReturnType>
ReturnType SerializationTuple::deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto deserialize_nested = [&settings](IColumn & nested_column, ReadBuffer & buf, const SerializationPtr & nested_column_serialization) -> ReturnType
    {
        if constexpr (std::is_same_v<ReturnType, void>)
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
                SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(nested_column, buf, settings, nested_column_serialization);
            else
                nested_column_serialization->deserializeTextJSON(nested_column, buf, settings);
        }
        else
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
                return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextJSON(nested_column, buf, settings, nested_column_serialization);
            return nested_column_serialization->tryDeserializeTextJSON(nested_column, buf, settings);
        }
    };

    if (settings.json.empty_as_default)
        return deserializeTupleJSONImpl<ReturnType>(column, istr, settings,
            [&deserialize_nested, &istr, this](IColumn & nested_column, size_t element_pos) -> ReturnType
            {
                return JSONUtils::deserializeEmpyStringAsDefaultOrNested<ReturnType>(nested_column, istr,
                    [&deserialize_nested, element_pos, this](IColumn & nested_column_, ReadBuffer & buf) -> ReturnType
                    {
                        return deserialize_nested(nested_column_, buf, elems[element_pos]);
                    });
            });
    return deserializeTupleJSONImpl<ReturnType>(
        column,
        istr,
        settings,
        [&deserialize_nested, &istr, this](IColumn & nested_column, size_t element_pos) -> ReturnType
        { return deserialize_nested(nested_column, istr, elems[element_pos]); });
}

void SerializationTuple::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextJSONImpl<void>(column, istr, settings);
}

bool SerializationTuple::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return deserializeTextJSONImpl<bool>(column, istr, settings);
}


void SerializationTuple::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeCString("<tuple>", ostr);
    for (size_t i = 0; i < elems.size(); ++i)
    {
        writeCString("<elem>", ostr);
        elems[i]->serializeTextXML(extractElementColumn(column, i), row_num, ostr, settings);
        writeCString("</elem>", ostr);
    }
    writeCString("</tuple>", ostr);
}

void SerializationTuple::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const size_t size = elems.size();
    const auto interior_settings = getInteriorTupleCSVSettings(settings, size);
    if (settings.csv.serialize_tuple_into_separate_columns
        && (settings.csv.quote_date_time_types
            || !tupleNeedsWholeCSVField(settings, elems, interior_settings, column, row_num)))
    {
        for (size_t i = 0; i < size; ++i)
        {
            if (i != 0)
                writeChar(settings.csv.tuple_delimiter, ostr);

            const auto & element_settings = interior_settings && i + 1 < size ? *interior_settings : settings;
            elems[i]->serializeTextCSV(extractElementColumn(column, i), row_num, ostr, element_settings);
        }
    }
    else
    {
        WriteBufferFromOwnString wb;
        serializeText(column, row_num, wb, settings);
        writeCSV(wb.str(), ostr);
    }
}

template <typename ReturnType>
ReturnType SerializationTuple::deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    const auto deserialize = [&](ReadBuffer & buf, bool whole_tuple) -> ReturnType
    {
        if (whole_tuple || !settings.csv.deserialize_separate_columns_into_tuple)
        {
            String value;
            if constexpr (throw_exception)
                readCSV(value, buf, settings.csv);
            else if (!tryReadCSV(value, buf, settings.csv))
                return false;

            ReadBufferFromString value_buf(value);
            if constexpr (throw_exception)
            {
                deserializeText(column, value_buf, settings, true);
                return;
            }
            else
                return tryDeserializeText(column, value_buf, settings, true);
        }

        const size_t size = elems.size();
        const auto interior_settings = getInteriorTupleCSVSettings(settings, size);
        return addElementSafe<ReturnType>(size, column, [&]()
        {
            for (size_t i = 0; i < size; ++i)
            {
                if (i != 0)
                {
                    skipWhitespaceIfAny(buf);
                    if constexpr (throw_exception)
                        assertChar(settings.csv.tuple_delimiter, buf);
                    else if (!checkChar(settings.csv.tuple_delimiter, buf))
                        return false;
                    skipWhitespaceIfAny(buf);
                }

                auto & element_column = extractElementColumn(column, i);
                const auto & element_settings = interior_settings && i + 1 < size ? *interior_settings : settings;
                if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(element_column))
                {
                    if constexpr (throw_exception)
                        SerializationNullable::deserializeNullAsDefaultOrNestedTextCSV(element_column, buf, element_settings, elems[i]);
                    else if (!SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextCSV(
                                 element_column, buf, element_settings, elems[i]))
                        return false;
                }
                else
                {
                    if constexpr (throw_exception)
                        elems[i]->deserializeTextCSV(element_column, buf, element_settings);
                    else if (!elems[i]->tryDeserializeTextCSV(element_column, buf, element_settings))
                        return false;
                }
            }
            return true;
        });
    };

    const size_t size = elems.size();
    const auto interior_settings = getInteriorTupleCSVSettings(settings, size);
    if (!settings.csv.deserialize_separate_columns_into_tuple
        || !tupleMayUseWholeCSVField(settings, elems, interior_settings))
        return deserialize(istr, false);

    PeekableReadBuffer peekable_buf(istr, true);
    peekable_buf.setCheckpoint();

    String value;
    bool whole_tuple = tryReadCSV(value, peekable_buf, settings.csv);
    if (whole_tuple && !peekable_buf.eof()
        && *peekable_buf.position() == settings.csv.tuple_delimiter
        && !settings.csv.tuple_delimiter_matches_field_delimiter)
    {
        whole_tuple = false;
    }

    if (whole_tuple && !peekable_buf.eof() && !settings.csv.force_quote_date_time_types)
    {
        if (!settings.csv.custom_delimiter.empty())
        {
            peekable_buf.setCheckpoint();
            whole_tuple = checkString(settings.csv.custom_delimiter, peekable_buf);
            peekable_buf.rollbackToCheckpoint();
            peekable_buf.dropCheckpoint();
        }
        else
            whole_tuple = *peekable_buf.position() == settings.csv.delimiter
                || *peekable_buf.position() == '\r'
                || *peekable_buf.position() == '\n';
    }

    if (whole_tuple)
    {
        ReadBufferFromString value_buf(value);
        whole_tuple = tryDeserializeText(column, value_buf, settings, true);
    }

    if (whole_tuple)
    {
        peekable_buf.dropCheckpoint();
        return ReturnType(true);
    }

    peekable_buf.rollbackToCheckpoint();
    peekable_buf.dropCheckpoint();
    return deserialize(peekable_buf, false);
}

void SerializationTuple::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextCSVImpl<void>(column, istr, settings);
}

bool SerializationTuple::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return deserializeTextCSVImpl<bool>(column, istr, settings);
}

bool SerializationTuple::textCSVMayNeedQuotes(const FormatSettings & settings) const
{
    for (const auto & element : elems)
    {
        if (element->textCSVMayNeedQuotes(settings))
            return true;
    }
    return false;
}

bool SerializationTuple::textCSVNeedsQuotes(
    const IColumn & column, size_t row_num, const FormatSettings & settings) const
{
    for (size_t i = 0; i < elems.size(); ++i)
    {
        if (elems[i]->textCSVNeedsQuotes(extractElementColumn(column, i), row_num, settings))
            return true;
    }
    return false;
}

struct SerializeBinaryBulkStateTuple : public ISerialization::SerializeBinaryBulkState
{
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> states;
};

struct DeserializeBinaryBulkStateTuple : public ISerialization::DeserializeBinaryBulkState
{
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> states;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateTuple>();
        new_state->states.reserve(states.size());
        for (const auto & state : states)
            new_state->states.push_back(state ? state->clone() : nullptr);

        return new_state;
    }

    void forEachNestedState(const std::function<void(const ISerialization::DeserializeBinaryBulkStatePtr &)> & callback) const override
    {
        for (const auto & state : states)
        {
            if (state)
                callback(state);
        }
    }
};

void SerializationTuple::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    if (elems.empty())
    {
        ISerialization::enumerateStreams(settings, callback, data);
        return;
    }

    const auto * type_tuple = data.type ? &assert_cast<const DataTypeTuple &>(*data.type) : nullptr;
    const auto * column_tuple = data.column ? &assert_cast<const ColumnTuple &>(*data.column) : nullptr;
    const auto * info_tuple = data.serialization_info ? &assert_cast<const SerializationInfoTuple &>(*data.serialization_info) : nullptr;
    const auto * tuple_deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateTuple>(data.deserialize_state) : nullptr;

    for (size_t i = 0; i < elems.size(); ++i)
    {
        auto next_data = SubstreamData(elems[i])
            .withType(type_tuple ? type_tuple->getElement(i) : nullptr)
            .withColumn(column_tuple ? column_tuple->getColumnPtr(i) : nullptr)
            .withSerializationInfo(info_tuple ? info_tuple->getElementInfo(i) : nullptr)
            .withDeserializeState(tuple_deserialize_state ? tuple_deserialize_state->states[i] : nullptr);

        elems[i]->enumerateStreams(settings, callback, next_data);
    }
}

void SerializationTuple::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto tuple_state = std::make_shared<SerializeBinaryBulkStateTuple>();
    tuple_state->states.resize(elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->serializeBinaryBulkStatePrefix(extractElementColumn(column, i), settings, tuple_state->states[i]);

    state = std::move(tuple_state);
}

void SerializationTuple::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * tuple_state = checkAndGetState<SerializeBinaryBulkStateTuple>(state);

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->serializeBinaryBulkStateSuffix(settings, tuple_state->states[i]);
}

void SerializationTuple::deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const
{
    auto tuple_state = std::make_shared<DeserializeBinaryBulkStateTuple>();
    tuple_state->states.resize(elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->deserializeBinaryBulkStatePrefix(settings, tuple_state->states[i], cache);

    state = std::move(tuple_state);
}

void SerializationTuple::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (elems.empty())
    {
        if (WriteBuffer * stream = settings.getter(settings.path))
        {
            size_t size = column.size();

            if (limit == 0 || offset + limit > size)
                limit = size - offset;

            for (size_t i = 0; i < limit; ++i)
                stream->write('0');
        }

        return;
    }

    auto * tuple_state = checkAndGetState<SerializeBinaryBulkStateTuple>(state);

    for (size_t i = 0; i < elems.size(); ++i)
    {
        const auto & element_col = extractElementColumn(column, i);
        elems[i]->serializeBinaryBulkWithMultipleStreams(element_col, offset, limit, settings, tuple_state->states[i]);
    }
}

void SerializationTuple::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (elems.empty())
    {
        if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
        {
            /// Data was inserted from substreams cache.
        }
        else if (ReadBuffer * stream = settings.getter(settings.path))
        {
            size_t prev_size = column->size();
            auto mutable_column = column->assumeMutable();
            auto ignored_size = stream->tryIgnore(rows_offset + limit);
            auto delta = ignored_size < rows_offset ? 0 : ignored_size - rows_offset;
            typeid_cast<ColumnTuple &>(*mutable_column).addSize(delta);
            column = std::move(mutable_column);
            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column, column->size() - prev_size);
        }

        return;
    }

    auto * tuple_state = checkAndGetState<DeserializeBinaryBulkStateTuple>(state);

    auto mutable_column = column->assumeMutable();
    auto & column_tuple = assert_cast<ColumnTuple &>(*mutable_column);

    for (size_t i = 0; i < elems.size(); ++i)
    {
        elems[i]->deserializeBinaryBulkWithMultipleStreams(
            column_tuple.getColumnPtr(i), rows_offset, limit, settings, tuple_state->states[i], cache);
    }

    /// Verify that all Tuple elements have the same size.
    size_t expected_size = column_tuple.getColumn(0).size();
    for (size_t i = 1; i < elems.size(); ++i)
    {
        if (column_tuple.getColumn(i).size() != expected_size)
            throw Exception(settings.native_format ? ErrorCodes::INCORRECT_DATA : ErrorCodes::LOGICAL_ERROR, "Unexpected size of tuple element {}: {}. Expected size: {}", i, column_tuple.getColumn(i).size(), expected_size);
    }

    typeid_cast<ColumnTuple &>(*mutable_column).addSize(column_tuple.getColumn(0).size());
}

size_t SerializationTuple::getPositionByName(const String & name) const
{
    size_t size = elems.size();
    for (size_t i = 0; i < size; ++i)
        if (elems[i]->getElementName() == name)
            return i;
    return std::numeric_limits<size_t>::max();
}

size_t SerializationTuple::allocatedBytes() const
{
    size_t bytes = sizeof(*this);
    bytes += elems.capacity() * sizeof(ElementSerializationPtr);
    return bytes;
}

bool SerializationTuple::supportsPooling() const
{
    for (const auto & elem : elems)
        if (!elem->supportsPooling())
            return false;
    return true;
}

}
