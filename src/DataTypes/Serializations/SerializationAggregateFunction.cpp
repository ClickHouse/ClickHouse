#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationAggregateFunction.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <absl/container/inlined_vector.h>
#include <optional>
#include <Common/Arena.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

UInt128 SerializationAggregateFunction::getHash(const AggregateFunctionPtr & function_, const String & type_name_, size_t version_)
{
    SipHash hash;
    hash.update("AggregateFunction");
    auto state_type_name = function_->getStateType()->getName();
    hash.update(state_type_name.size());
    hash.update(state_type_name);
    hash.update(type_name_.size());
    hash.update(type_name_);
    hash.update(version_);
    hash.update(static_cast<UInt8>(function_->getStateVariant()));
    return hash.get128();
}

namespace
{
void deserializeFromString(const AggregateFunctionPtr & function, IColumn & column, ReadBuffer & read_buf, size_t version, bool check_buffer_consumed = false)
{
    ColumnAggregateFunction & column_concrete = assert_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());

    function->create(place);

    try
    {
        function->deserialize(place, read_buf, version, &arena);

        if (check_buffer_consumed && !read_buf.eof())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "AggregateFunction state for `{}` has {} trailing byte(s) after deserialization",
                function->getName(),
                read_buf.available());
        }
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }

    column_concrete.getData().push_back(place);
}

void createStateFromValues(const AggregateFunctionPtr & function, IColumn & column, const IColumn ** arg_columns, size_t num_rows)
{
    ColumnAggregateFunction & column_concrete = assert_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alignedAlloc(size_of_state, function->alignOfData());

    function->create(place);

    try
    {
        function->addBatchSinglePlace(0, num_rows, place, arg_columns, &arena);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }
    column_concrete.getData().push_back(place);
}

/// Backward compatibility: released parsed the single-argument `value` payload with the argument type's
/// `deserializeTextCSV`, which consults `input_format_csv_enum_as_number` for an `Enum` argument and never
/// looked at `input_format_tsv_enum_as_number`. The unified path parses the payload with the escaped or
/// whole-text serialization instead, and those two consult `input_format_tsv_enum_as_number`: with it
/// enabled they read the payload as a number and reject the enum names released accepted here (verified on
/// released `26.7.1`: with `input_format_tsv_enum_as_number = 1` and `aggregate_function_input_format =
/// 'value'`, the name `a` was accepted for `AggregateFunction(any, Enum8('a' = 1))` in `TabSeparated`,
/// `TSVRaw` and the string-wrapped `JSONEachRow` form). So disable the TSV setting for that payload.
/// Both forms keep working afterwards, because `EnumValues::getValue` falls back to parsing the name as a
/// number - which is also why the numeric form was never lost in the first place, regardless of
/// `input_format_csv_enum_as_number`. Composite argument types containing an `Enum` are unaffected: their
/// nested elements are parsed with the quoted serialization on both the released and the unified path.
std::optional<FormatSettings> legacyEnumValueSettings(const DataTypePtr & argument_type, const FormatSettings & settings)
{
    if (!settings.tsv.enum_as_number || !isEnum(removeNullable(removeLowCardinality(argument_type))))
        return {};

    FormatSettings result = settings;
    result.tsv.enum_as_number = false;
    return result;
}

using DeserializeMethod = void(SerializationPtr, IColumn &, ReadBuffer &, const FormatSettings &);
#define DESERIALIZE_METHOD(method) [] (SerializationPtr serde, auto column_, auto istr_, auto settings_) { \
    serde->method(column_, istr_, settings_); \
}

template<DeserializeMethod Method>
void deserializeFromValues(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const AggregateFunctionPtr & function)
{
    chassert(settings.aggregate_function_input_format != FormatSettings::AggregateFunctionInputFormat::State);

    const auto & argument_types = function->getArgumentTypes();
    const auto value_type = argument_types.size() == 1 ? argument_types[0] : std::make_shared<DataTypeTuple>(argument_types);

    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::Value)
    {
        const auto tmp_column = value_type->createColumn();
        const auto legacy_enum_settings
            = argument_types.size() == 1 ? legacyEnumValueSettings(argument_types[0], settings) : std::nullopt;
        Method(
            value_type->getDefaultSerialization(),
            *tmp_column,
            istr,
            legacy_enum_settings ? *legacy_enum_settings : settings);

        absl::InlinedVector<const IColumn *, 7> columns_ptrs;
        if (argument_types.size() == 1)
            columns_ptrs.push_back(tmp_column.get());
        else
            for (const auto & col : assert_cast<const ColumnTuple*>(tmp_column.get())->getColumns())
                columns_ptrs.push_back(col.get());

        createStateFromValues(function, column, columns_ptrs.data(), 1);
    }
    else
    {
        auto array_type = DataTypeArray(value_type);
        const auto tmp_column = array_type.createColumn();
        Method(array_type.getDefaultSerialization(), *tmp_column, istr, settings);

        const auto & array_column = assert_cast<const ColumnArray&>(*tmp_column);
        absl::InlinedVector<const IColumn *, 7> columns_ptrs;
        if (argument_types.size() == 1)
            columns_ptrs.push_back(array_column.getDataPtr().get());
        else if (!argument_types.empty())
            for (const auto & col : assert_cast<const ColumnTuple&>(array_column.getData()).getColumns())
                columns_ptrs.push_back(col.get());

        createStateFromValues(function, column, columns_ptrs.data(), array_column.getData().size());
    }
}

/// Backward compatibility: when `aggregate_function_input_format` was first released (in `v25.12` and `v26.1`),
/// `array` mode parsed single-argument elements with the argument type's `deserializeTextCSV`, which accepts
/// double-quoted representations, e.g. `["apple","banana"]` for `AggregateFunction(uniq, String)` or
/// `["a","b"]` for an `Enum` argument. The generic `SerializationArray` text path parses elements with
/// `deserializeTextQuoted`, which rejects double quotes, so single-argument text arrays are parsed here instead:
/// single-quoted `String`-like and `Enum` elements with the quoted serialization (the native form) and everything
/// else with the CSV serialization (the released form; the scalar CSV parse itself accepts both quote kinds).
/// `Variant` and `Dynamic` arguments also take this path: their `deserializeTextCSV` reads a whole CSV field
/// and tries the variants (or infers the type) from it, so released input accepted double-quoted strings and
/// bareword scalars for them too. Composite argument types take it as well: released fed their elements
/// through the same per-element CSV parse, which accepted a double-quoted representation of the whole
/// composite value, e.g. `["[1,2]","[3]"]` for `AggregateFunction(groupArray, Array(UInt64))`,
/// `["{'a':1}"]` for a `Map` argument and `["{""a"":1}"]` for a `JSON` one (verified on released `26.7.1`).
bool useLegacyTextArrayParsing(const AggregateFunctionPtr & function, const FormatSettings & settings)
{
    if (settings.aggregate_function_input_format != FormatSettings::AggregateFunctionInputFormat::Array)
        return false;

    const auto & argument_types = function->getArgumentTypes();
    if (argument_types.size() != 1)
        return false;

    WhichDataType which(removeNullable(removeLowCardinality(argument_types[0])));
    return !(which.isAggregateFunction() || which.isNothing());
}

void deserializeFromSingleArgumentTextArray(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const AggregateFunctionPtr & function)
{
    const auto & argument_types = function->getArgumentTypes();
    chassert(argument_types.size() == 1);

    const auto & value_type = argument_types[0];
    const auto tmp_column = value_type->createColumn();
    const auto elem_serialization = value_type->getDefaultSerialization();

    /// Types parsed from CSV via the CSV string reader lose single-quoted elements when
    /// `format_csv_allow_single_quotes` is disabled (the default), so the native single-quoted form is
    /// dispatched to the quoted serialization for them. Other scalar types (numbers, dates, `UUID`, ...)
    /// handle both quote kinds in `deserializeTextCSV` itself, and their `deserializeTextQuoted` would
    /// reject quotes, so they always take the CSV branch. For `Nullable` types with a non-string-like
    /// nested type, an element starting with `N`/`n` is dispatched to the quoted serialization, which
    /// recognizes the `NULL` keyword of the native form with rollback (and otherwise falls through to the
    /// nested quoted parse, e.g. `NaN` for floats); the released CSV element parse rejected `NULL`/`null`
    /// for these types, so this is purely additive. String-like `Nullable` arguments must NOT take that
    /// dispatch: the released CSV element parse accepted arbitrary unquoted words as string values,
    /// including ones starting with `N`/`n` (`[NaN,"a"]` produced the string 'NaN', and `[NULL,"a"]`
    /// produced the STRING 'NULL', not a null), so they always take the CSV branch too.
    /// `Variant` and `Dynamic` elements use the CSV parse (the released form: double-quoted strings and
    /// bareword scalars worked, and bareword `NULL` parsed as the STRING 'NULL' when a `String`-like
    /// variant is present) except when the element
    /// starts with `'` (the native single-quoted string form, mirroring the `String` dispatch above) or
    /// with `[`, `(` or `{` (native composite forms: the released CSV field parse stops at the first comma,
    /// so it either threw or silently split them into fragment strings such as '[1' and '2]' when a
    /// `String`-like variant could absorb the pieces — the quoted parse deliberately replaces that
    /// degenerate behavior with the native composite form). A bareword
    /// starting with `N`/`n` must stay on the CSV branch: for a `Variant` with a `String`-like variant
    /// (and for `Dynamic`), released input parsed e.g. `[NaN,"a"]` as the string 'NaN'.
    /// Composite argument types (`Array`, `Tuple`, `Map`, `Object`) are dispatched the same way as
    /// `Variant`/`Dynamic`: an element opening with `[`, `(` or `{` is the native form added here and is
    /// parsed with the quoted serialization (which is what the generic array text path would do), while
    /// anything else keeps the released per-element CSV parse. That parse is how released accepted the
    /// double-quoted representation of a whole composite element (`["[1,2]","[3]"]`, `["{'a':1}"]`,
    /// `["{""a"":1}"]`) and also the flattened `Tuple` form, where the tuple's CSV representation is the
    /// bare comma-separated list of its elements (`[1,"a"]` for `Tuple(UInt64, String)`).
    const auto unwrapped_type = removeNullable(removeLowCardinality(value_type));
    const bool quoted_form_is_string = isStringOrFixedString(unwrapped_type) || isEnum(unwrapped_type);
    const bool is_variant_or_dynamic = isVariant(unwrapped_type) || isDynamic(unwrapped_type);
    const bool is_nullable = value_type->isNullable() || value_type->isLowCardinalityNullable();
    WhichDataType which_unwrapped(unwrapped_type);
    const bool is_composite = which_unwrapped.isArray() || which_unwrapped.isTuple()
        || which_unwrapped.isMap() || which_unwrapped.isObject();

    assertChar('[', istr);
    bool first = true;
    while (!istr.eof())
    {
        skipWhitespaceIfAny(istr);
        if (!istr.eof() && *istr.position() == ']')
            break;
        if (!first)
        {
            assertChar(',', istr);
            skipWhitespaceIfAny(istr);
        }
        first = false;
        const char first_char = istr.eof() ? 0 : *istr.position();
        if ((quoted_form_is_string && first_char == '\'')
            || (is_variant_or_dynamic && (first_char == '\'' || first_char == '[' || first_char == '(' || first_char == '{'))
            || (is_composite && (first_char == '[' || first_char == '(' || first_char == '{'))
            || (!quoted_form_is_string && !is_variant_or_dynamic && is_nullable && (first_char == 'N' || first_char == 'n')))
            elem_serialization->deserializeTextQuoted(*tmp_column, istr, settings);
        else
            elem_serialization->deserializeTextCSV(*tmp_column, istr, settings);
    }
    assertChar(']', istr);

    const IColumn * arg_columns[1] = {tmp_column.get()};
    createStateFromValues(function, column, arg_columns, tmp_column->size());
}

/// Backward compatibility: released read the whole field of a `value` payload as a string and parsed it with
/// the argument type's `deserializeTextCSV`, which strips a pair of surrounding CSV quotes. The unified path
/// parses the field with the escaped or whole-text serialization instead, and those keep the quotes as part of
/// the payload, so a quoted form released accepted is either rejected or read literally. Verified on released
/// `26.7.1` with `aggregate_function_input_format = 'value'` in `TSVRaw` and `TabSeparated`: `'42'` for
/// `AggregateFunction(any, UInt64)` inserted `42`, `'2020-01-01'` for a `Date` argument inserted the date, and
/// `"abc"` for a `String` argument inserted `abc`. So when the whole field starts with a quote, keep parsing it
/// exactly as released did. Note that the quote kinds are not interchangeable there, and the released
/// resolution is reproduced for both: the CSV parse of a `String`-like argument treats only `"` as a quote, so
/// `'abc'` stays the string `'abc'`, while for the other scalar types `readCSVSimple` strips `'` and `"` alike.
/// The unquoted forms are unaffected and keep the unified parse.
bool isSingleArgumentValueMode(const AggregateFunctionPtr & function, const FormatSettings & settings)
{
    return settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::Value
        && function->getArgumentTypes().size() == 1;
}

bool useLegacyQuotedValueParsing(const AggregateFunctionPtr & function, const FormatSettings & settings, ReadBuffer & istr)
{
    if (!isSingleArgumentValueMode(function, settings))
        return false;

    return !istr.eof() && (*istr.position() == '\'' || *istr.position() == '"');
}

void deserializeFromSingleArgumentLegacyQuotedValue(
    IColumn & column, const String & value, const FormatSettings & settings, const AggregateFunctionPtr & function)
{
    const auto & argument_types = function->getArgumentTypes();
    chassert(argument_types.size() == 1);

    const auto & value_type = argument_types[0];
    const auto tmp_column = value_type->createColumn();
    ReadBufferFromString buf(value);
    /// The released implementation did not check that the CSV parse consumed the whole field, so neither do we.
    value_type->getDefaultSerialization()->deserializeTextCSV(*tmp_column, buf, settings);

    const IColumn * arg_columns[1] = {tmp_column.get()};
    createStateFromValues(function, column, arg_columns, 1);
}

/// Backward compatibility for the `value` forms that released read as a whole string and parsed with the
/// argument type's `deserializeTextCSV`: the string-wrapped JSON form `{"x": "\\N"}` (see the comment in
/// `deserializeTextJSON`), the quoted `VALUES` form `('\\N')` (see the comment in `deserializeTextQuoted`)
/// and the whole field of the raw text formats (see the comment in `deserializeWholeText`).
/// For a `Nullable` argument that CSV parse recognizes forms the parse of the unwrapped content through the
/// argument type does not: see `deserializeFromSingleNullableArgumentLegacyValue`.
bool useLegacyNullableValueParsing(const AggregateFunctionPtr & function, const FormatSettings & settings)
{
    if (settings.aggregate_function_input_format != FormatSettings::AggregateFunctionInputFormat::Value)
        return false;

    const auto & argument_types = function->getArgumentTypes();
    if (argument_types.size() != 1)
        return false;

    return argument_types[0]->isNullable() || argument_types[0]->isLowCardinalityNullable();
}

void deserializeFromSingleNullableArgumentLegacyValue(
    IColumn & column, const String & value, const FormatSettings & settings, const AggregateFunctionPtr & function)
{
    const auto & argument_types = function->getArgumentTypes();
    chassert(argument_types.size() == 1);

    const auto & value_type = argument_types[0];
    const auto tmp_column = value_type->createColumn();
    const auto nested_type = removeNullable(removeLowCardinality(value_type));

    /// See `legacyEnumValueSettings`: the whole-text parses below must not read a `Nullable(Enum)` payload
    /// as a number only.
    const auto legacy_enum_settings = legacyEnumValueSettings(value_type, settings);
    const FormatSettings & value_settings = legacy_enum_settings ? *legacy_enum_settings : settings;

    if (value == settings.csv.null_representation)
    {
        /// The released CSV parse of the unwrapped content produced a null for the CSV null representation
        /// (`\N` by default), which the whole-text parse of a `Nullable` does not recognize.
        tmp_column->insert(Null());
    }
    else if (isStringOrFixedString(nested_type) || isEnum(nested_type))
    {
        /// The released CSV parse only ever produced a null for the CSV null representation handled above:
        /// for string-like nested types every other content was a plain value, including `NULL` and `null`
        /// (`{"x": "NULL"}` inserted the string 'NULL'). The whole-text parse of a `Nullable` would turn
        /// those two words into a null instead, so parse the content as the nested type here.
        const auto nested_column = nested_type->createColumn();
        ReadBufferFromString buf(value);
        nested_type->getDefaultSerialization()->deserializeWholeText(*nested_column, buf, value_settings);
        tmp_column->insert((*nested_column)[0]);
    }
    else
    {
        /// For the remaining nested types the released CSV parse of `NULL` produced a default value rather
        /// than a null, so accepting the `NULL` keyword of the whole-text parse only adds a form.
        ReadBufferFromString buf(value);
        value_type->getDefaultSerialization()->deserializeWholeText(*tmp_column, buf, value_settings);
    }

    const IColumn * arg_columns[1] = {tmp_column.get()};
    createStateFromValues(function, column, arg_columns, 1);
}

}

void SerializationAggregateFunction::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    const AggregateFunctionStateData & state = field.safeGet<AggregateFunctionStateData>();
    writeBinary(state.data, ostr);
}

void SerializationAggregateFunction::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    field = AggregateFunctionStateData();
    AggregateFunctionStateData & s = field.safeGet<AggregateFunctionStateData>();
    s.name = type_name;

    /// This is the representation used when an `AggregateFunction` value is nested inside another value,
    /// such as `argMax`. It is length-prefixed by `serializeBinary(Field, ...)`.
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        readBinary(s.data, istr);
        return;
    }

    /// The value mode uses the argument type's binary representation.
    auto tmp_column = ColumnAggregateFunction::create(function, version);
    deserializeBinary(*tmp_column, istr, settings);

    WriteBufferFromString buf(s.data);
    function->serialize(tmp_column->getData()[0], buf, version);
}

void SerializationAggregateFunction::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    function->serialize(assert_cast<const ColumnAggregateFunction &>(column).getData()[row_num], ostr, version);
}

void SerializationAggregateFunction::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        deserializeFromString(function, column, istr, version);
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeBinary);
    deserializeFromValues<method>(column, istr, settings, function);
}

void SerializationAggregateFunction::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnAggregateFunction & real_column = typeid_cast<const ColumnAggregateFunction &>(column);
    const ColumnAggregateFunction::Container & vec = real_column.getData();

    size_t end = vec.size();
    if (limit)
        end = std::min(end, offset + limit);

    function->serializeBatch(vec, offset, end, ostr, version);
}

void SerializationAggregateFunction::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    ColumnAggregateFunction & real_column = typeid_cast<ColumnAggregateFunction &>(column);
    ColumnAggregateFunction::Container & vec = real_column.getData();

    Arena & arena = real_column.createOrGetArena();
    real_column.set(function, version);
    vec.reserve(vec.size() + limit);

    size_t size_of_state = function->sizeOfData();
    size_t align_of_state = function->alignOfData();

    /// Adjust the size of state to make all states aligned in vector.
    size_t total_size_of_state = (size_of_state + align_of_state - 1) / align_of_state * align_of_state;
    char * place = arena.alignedAlloc(total_size_of_state * limit, align_of_state);

    function->createAndDeserializeBatch(vec, place, total_size_of_state, limit, istr, version, &arena);
}

static String serializeToString(const AggregateFunctionPtr & function, const IColumn & column, size_t row_num, size_t version)
{
    WriteBufferFromOwnString buffer;
    function->serialize(assert_cast<const ColumnAggregateFunction &>(column).getData()[row_num], buffer, version);
    return buffer.str();
}


SerializationPtr SerializationAggregateFunction::create(const AggregateFunctionPtr & function_, String type_name_, size_t version_)
{
    return ISerialization::pooled(getHash(function_, type_name_, version_), [&] { return new SerializationAggregateFunction(function_, std::move(type_name_), version_); });
}

void SerializationAggregateFunction::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::serializeTextEscaped(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        String s;
        settings.tsv.crlf_end_of_line_input ? readEscapedStringCRLF(s, istr) : readEscapedString(s, istr);
        ReadBufferFromString str_buf(s);
        deserializeFromString(function, column, str_buf, version, /* check_buffer_consumed */ true);
        return;
    }

    if (useLegacyTextArrayParsing(function, settings))
    {
        /// Decode the escaped field first (as the released implementation did), so escape sequences
        /// inside the elements, e.g. `["a\tb"]` in `TabSeparated`, become real characters before the
        /// per-element parse.
        String s;
        settings.tsv.crlf_end_of_line_input ? readEscapedStringCRLF(s, istr) : readEscapedString(s, istr);
        ReadBufferFromString str_buf(s);
        deserializeFromSingleArgumentTextArray(column, str_buf, settings, function);
        if (!str_buf.eof())
            throwUnexpectedDataAfterParsedValue(column, str_buf, settings, "Array");
        return;
    }

    /// Backward compatibility, see `useLegacyQuotedValueParsing`. An unescaped quote can be recognized
    /// before the escaped read, because it cannot be the start of an escape sequence.
    if (useLegacyQuotedValueParsing(function, settings, istr))
    {
        String s;
        settings.tsv.crlf_end_of_line_input ? readEscapedStringCRLF(s, istr) : readEscapedString(s, istr);
        deserializeFromSingleArgumentLegacyQuotedValue(column, s, settings, function);
        return;
    }

    /// The released implementation decoded the whole escaped field before that CSV parse, so the leading
    /// quote may itself be escaped in the input: `\"42\"` for `AggregateFunction(any, UInt64)` in
    /// `TabSeparated` inserted `42` on released `26.7.1`. Telling that apart from any other escape sequence
    /// (e.g. the `\N` null marker of a `Nullable` argument) needs two characters of lookahead, so for a field
    /// that starts with a backslash read the raw field first and either take the compatibility path or replay
    /// the raw bytes through the unified escaped parse, which then sees exactly the same field.
    if (isSingleArgumentValueMode(function, settings) && !istr.eof() && *istr.position() == '\\')
    {
        String raw_field;
        settings.tsv.crlf_end_of_line_input ? readTSVFieldCRLF(raw_field, istr) : readTSVField(raw_field, istr);
        ReadBufferFromString raw_buf(raw_field);

        if (raw_field.size() >= 2 && (raw_field[1] == '\'' || raw_field[1] == '"'))
        {
            String s;
            settings.tsv.crlf_end_of_line_input ? readEscapedStringCRLF(s, raw_buf) : readEscapedString(s, raw_buf);
            deserializeFromSingleArgumentLegacyQuotedValue(column, s, settings, function);
            return;
        }

        auto method = DESERIALIZE_METHOD(deserializeTextEscaped);
        deserializeFromValues<method>(column, raw_buf, settings, function);
        if (!raw_buf.eof())
            throwUnexpectedDataAfterParsedValue(column, raw_buf, settings, "Value");
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeTextEscaped);
    deserializeFromValues<method>(column, istr, settings, function);
}


void SerializationAggregateFunction::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto str = serializeToString(function, column, row_num, version);
    if (settings.values.escape_quote_with_quote)
        writeQuotedStringPostgreSQL(str, ostr);
    else
        writeQuotedString(str, ostr);
}


void SerializationAggregateFunction::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        String s;
        readQuotedStringWithSQLStyle(s, istr);
        ReadBufferFromString str_buf(s);
        deserializeFromString(function, column, str_buf, version, /* check_buffer_consumed */ true);
        return;
    }

    /// Backward compatibility: the released implementation always read the whole quoted token as a string
    /// and parsed its content, so every `Quoted` caller accepted a quoted payload: `'[1, 2]'` in `array`
    /// mode and `'42'` or `'\\N'` in `value` mode. `VALUES` masks a failure here with its expression
    /// fallback, but the other callers - `CustomSeparated` with `format_custom_escaping_rule = 'Quoted'`,
    /// `MySQLDump` - do not. So unwrap the token and parse the payload with the whole-text path, which is
    /// the unified equivalent of the released per-value parse and also keeps the CSV null representation
    /// working for a single `Nullable` argument (`SerializationNullable::deserializeTextQuoted` does not
    /// recognize it). Native unquoted forms, e.g. `[1, 2]` in `array` mode or the `NULL` keyword, do not
    /// start with a quote and fall through to the branches below.
    if (!istr.eof() && *istr.position() == '\'')
    {
        String s;
        readQuotedStringWithSQLStyle(s, istr);
        ReadBufferFromString str_buf(s);
        deserializeWholeText(column, str_buf, settings);
        return;
    }

    if (useLegacyTextArrayParsing(function, settings))
    {
        deserializeFromSingleArgumentTextArray(column, istr, settings, function);
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeTextQuoted);
    deserializeFromValues<method>(column, istr, settings, function);
}


void SerializationAggregateFunction::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        deserializeFromString(function, column, istr, version, /* check_buffer_consumed */ true);
        return;
    }

    if (useLegacyTextArrayParsing(function, settings))
    {
        deserializeFromSingleArgumentTextArray(column, istr, settings, function);
        if (!istr.eof())
            throwUnexpectedDataAfterParsedValue(column, istr, settings, "Array");
        return;
    }

    /// Backward compatibility, see `useLegacyQuotedValueParsing`. This has to come before the `Nullable`
    /// branch below: released parsed a quoted payload of a `Nullable` argument with the very same CSV parse,
    /// so `'42'` for `AggregateFunction(any, Nullable(UInt64))` inserted `42`.
    if (useLegacyQuotedValueParsing(function, settings, istr))
    {
        String s;
        readStringUntilEOF(s, istr);
        deserializeFromSingleArgumentLegacyQuotedValue(column, s, settings, function);
        return;
    }

    /// Backward compatibility: released read the whole field as a string and parsed its content with the
    /// argument type's `deserializeTextCSV`, so the CSV null representation (`\\N` by default) built a null
    /// state for a single `Nullable` argument. `SerializationNullable::deserializeWholeText` recognizes only
    /// the `NULL` and `ᴺᵁᴸᴸ` keywords, so route the field of a single `Nullable` argument through the same
    /// helper as the quoted `VALUES` and the string-wrapped JSON forms. This path is what the formats with
    /// `EscapingRule::Raw` use — `TSVRaw`, the `Raw*` family and `CustomSeparated` with
    /// `format_custom_escaping_rule = 'Raw'` — because `ISerialization::deserializeTextRaw` reads the field
    /// and forwards it here.
    if (useLegacyNullableValueParsing(function, settings))
    {
        String s;
        readStringUntilEOF(s, istr);
        deserializeFromSingleNullableArgumentLegacyValue(column, s, settings, function);
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeWholeText);
    deserializeFromValues<method>(column, istr, settings, function);
}


void SerializationAggregateFunction::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(serializeToString(function, column, row_num, version), ostr, settings);
}


void SerializationAggregateFunction::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        String s;
        readJSONString(s, istr, settings.json);
        ReadBufferFromString str_buf(s);
        deserializeFromString(function, column, str_buf, version, /* check_buffer_consumed */ true);
        return;
    }

    /// Backward compatibility: `aggregate_function_input_format` was released in `v25.12` and `v26.1` accepting the
    /// value/array as a JSON string holding its textual representation, e.g. `{"x": "[1,2,3]"}` in `array` mode.
    /// The native JSON form `{"x": [1,2,3]}` added by this change reads `[` directly, so a string-wrapped array would
    /// otherwise be rejected. Keep accepting the legacy string-wrapped form alongside the native one.
    /// The string content uses the same whole-text path as other legacy text representations. In particular,
    /// this keeps composite values intact instead of treating their commas as CSV delimiters. Native unquoted
    /// forms (`{"x": 42}`, `{"x": [1, 2]}`) are new and reach the JSON parser as usual.
    skipWhitespaceIfAny(istr);
    if (!istr.eof() && *istr.position() == '"')
    {
        String s;
        readJSONString(s, istr, settings.json);

        ReadBufferFromString str_buf(s);
        deserializeWholeText(column, str_buf, settings);
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeTextJSON);
    deserializeFromValues<method>(column, istr, settings, function);
}


void SerializationAggregateFunction::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeXMLStringForTextElement(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSV(serializeToString(function, column, row_num, version), ostr);
}


void SerializationAggregateFunction::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        String s;
        readCSV(s, istr, settings.csv);
        ReadBufferFromString str_buf(s);
        deserializeFromString(function, column, str_buf, version, /* check_buffer_consumed */ true);
        return;
    }

    if (useLegacyTextArrayParsing(function, settings))
    {
        String s;
        readCSV(s, istr, settings.csv);
        ReadBufferFromString str_buf(s);
        deserializeFromSingleArgumentTextArray(column, str_buf, settings, function);
        return;
    }

    if (useLegacyNullableValueParsing(function, settings))
    {
        /// Released `CSV` parsing unwrapped the complete field before parsing a single `Nullable` argument
        /// with its CSV serialization. Preserve its handling of a quoted CSV null representation such as
        /// `"\\N"`, which otherwise reaches `SerializationNullable::deserializeTextCSV` with the quotes.
        String s;
        readCSV(s, istr, settings.csv);
        /// Backward compatibility, see `useLegacyQuotedValueParsing`. As in `deserializeWholeText`, this has
        /// to come before the `Nullable` helper: released parsed an inner-quoted payload, e.g. `"""42"""`
        /// unwrapped to `"42"`, with the very same CSV parse, which strips the inner quotes, while the
        /// whole-text parse of the helper would reject `"42"` for a numeric argument and keep the quotes
        /// for a string one.
        if (!s.empty() && (s[0] == '\'' || s[0] == '"'))
        {
            deserializeFromSingleArgumentLegacyQuotedValue(column, s, settings, function);
            return;
        }
        deserializeFromSingleNullableArgumentLegacyValue(column, s, settings, function);
        return;
    }

    auto method = DESERIALIZE_METHOD(deserializeTextCSV);
    deserializeFromValues<method>(column, istr, settings, function);
}

size_t SerializationAggregateFunction::allocatedBytes() const
{
    return sizeof(*this) + type_name.capacity();
}

}
