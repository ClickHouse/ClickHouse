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
#include <Common/Arena.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
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
        Method(value_type->getDefaultSerialization(), *tmp_column, istr, settings);

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
/// bareword scalars for them too. Composite argument types stay on the unified path: their quoted
/// elements start with `[`, `(` or `{`, which the released per-element CSV parse could not handle anyway
/// (the CSV string parse stops at the first comma), so no released form is lost for them.
bool useLegacyTextArrayParsing(const AggregateFunctionPtr & function, const FormatSettings & settings)
{
    if (settings.aggregate_function_input_format != FormatSettings::AggregateFunctionInputFormat::Array)
        return false;

    const auto & argument_types = function->getArgumentTypes();
    if (argument_types.size() != 1)
        return false;

    WhichDataType which(removeNullable(removeLowCardinality(argument_types[0])));
    return !(which.isArray() || which.isTuple() || which.isMap() || which.isObject()
        || which.isAggregateFunction() || which.isNothing());
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
    const auto unwrapped_type = removeNullable(removeLowCardinality(value_type));
    const bool quoted_form_is_string = isStringOrFixedString(unwrapped_type) || isEnum(unwrapped_type);
    const bool is_variant_or_dynamic = isVariant(unwrapped_type) || isDynamic(unwrapped_type);
    const bool is_nullable = value_type->isNullable() || value_type->isLowCardinalityNullable();

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
            || (!quoted_form_is_string && !is_variant_or_dynamic && is_nullable && (first_char == 'N' || first_char == 'n')))
            elem_serialization->deserializeTextQuoted(*tmp_column, istr, settings);
        else
            elem_serialization->deserializeTextCSV(*tmp_column, istr, settings);
    }
    assertChar(']', istr);

    const IColumn * arg_columns[1] = {tmp_column.get()};
    createStateFromValues(function, column, arg_columns, tmp_column->size());
}

/// Backward compatibility for the `value` forms that released read as a quoted string and parsed with the
/// argument type's `deserializeTextCSV`: the string-wrapped JSON form `{"x": "\\N"}` (see the comment in
/// `deserializeTextJSON`) and the quoted `VALUES` form `('\\N')` (see the comment in `deserializeTextQuoted`).
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
        nested_type->getDefaultSerialization()->deserializeWholeText(*nested_column, buf, settings);
        tmp_column->insert((*nested_column)[0]);
    }
    else
    {
        /// For the remaining nested types the released CSV parse of `NULL` produced a default value rather
        /// than a null, so accepting the `NULL` keyword of the whole-text parse only adds a form.
        ReadBufferFromString buf(value);
        value_type->getDefaultSerialization()->deserializeWholeText(*tmp_column, buf, settings);
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

    if (settings.aggregate_function_input_format == FormatSettings::AggregateFunctionInputFormat::State)
    {
        readBinary(s.data, istr);
        return;
    }

    /// This method must honor `aggregate_function_input_format`: `BinaryRowInputFormat::skipField` uses it
    /// to consume unknown columns of `RowBinary(WithNamesAndTypes)` input, so it has to read exactly the same
    /// bytes as the column-based `deserializeBinary` does, otherwise all following columns are misaligned.
    /// Delegate to the column-based path to guarantee that: parsing the value with the argument type's
    /// Field-based deserializer would diverge for types whose two paths read different representations
    /// (e.g. `SerializationObject` reads a length-prefixed string into a column when
    /// `input_format_binary_read_json_as_string` is enabled, but always the structured form into a `Field`).
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

void SerializationAggregateFunction::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double /*avg_value_size_hint*/) const
{
    if (rows_offset)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Method deserializeBinaryBulk of SerializationAggregateFunction does not support cases where rows_offset {} is non-zero",
                        rows_offset);

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

    if (useLegacyTextArrayParsing(function, settings))
    {
        deserializeFromSingleArgumentTextArray(column, istr, settings, function);
        return;
    }

    /// Backward compatibility: released read the whole quoted token as a string and parsed its content with
    /// the argument type's `deserializeTextCSV`, so `INSERT ... VALUES ('\\N')` built a null state for a
    /// single `Nullable` argument. `SerializationNullable::deserializeTextQuoted` does not recognize the CSV
    /// null representation: it would store the literal `\\N` for a string-like nested type and throw for a
    /// numeric one, so route quoted tokens of a single `Nullable` argument through the same helper as the
    /// string-wrapped JSON form. The native unquoted `NULL` keyword is unaffected: it does not start with a
    /// quote and stays on the unified path below.
    if (useLegacyNullableValueParsing(function, settings) && !istr.eof() && *istr.position() == '\'')
    {
        String s;
        readQuotedStringWithSQLStyle(s, istr);
        deserializeFromSingleNullableArgumentLegacyValue(column, s, settings, function);
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
    /// Note that this does not shadow any native quoted form for fixed scalar argument types: they parse
    /// identically through the whole-text parse of the unwrapped content (JSON escapes are decoded by
    /// `readJSONString`), and the `JSON` type itself has no native quoted-token form (a JSON document root
    /// must be an object, so e.g. `{"x": "hello"}` is rejected for plain `JSON` columns as well), while its
    /// released string-wrapped object form `{"x": "{\"a\":1}"}` only works through this branch.
    /// The self-describing `Dynamic` and `Variant` argument types also deliberately resolve a quoted token as
    /// the released string-wrapped form rather than as a JSON string scalar: released resolved the unwrapped
    /// content by its own text, so `{"x": "42"}` for a `Dynamic` argument produced the number `42` and
    /// `{"x": "2020-01-01"}` a `Date`, and routing the quoted token to `deserializeTextJSON` (which would keep
    /// the `String` alternative) would change that. The native unquoted forms (`{"x": 42}`, `{"x": [1, 2]}`)
    /// are new here and reach the JSON parser as usual.
    /// The one released resolution the whole-text parse does not reproduce for these types is the degenerate
    /// output of the released per-value `deserializeTextCSV` when a composite alternative is present: for
    /// `Variant(String, Array(UInt64))` released turned `{"x": "42"}` into the array `[42]` and `{"x": "NULL"}`
    /// into `[0]`, and truncated `{"x": "[1, 2]"}` at the comma into the string `'[1'`. That is the same
    /// degenerate CSV-field behavior the `array` mode drops for composite element forms, so the whole-text
    /// parse of the content is used instead (the string `'42'`, a `NULL` variant and the array `[1, 2]`).
    /// The one released form the whole-text parse of the unwrapped content does not reproduce is a
    /// `Nullable` single argument: released parsed the content with `deserializeTextCSV`, which recognizes
    /// the CSV null representation (`\N` by default) and never treats `NULL` as a null for string-like
    /// nested types, so those two forms are handled separately below.
    skipWhitespaceIfAny(istr);
    if (!istr.eof() && *istr.position() == '"')
    {
        String s;
        readJSONString(s, istr, settings.json);

        if (useLegacyNullableValueParsing(function, settings))
        {
            deserializeFromSingleNullableArgumentLegacyValue(column, s, settings, function);
            return;
        }

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

    auto method = DESERIALIZE_METHOD(deserializeTextCSV);
    deserializeFromValues<method>(column, istr, settings, function);
}

size_t SerializationAggregateFunction::allocatedBytes() const
{
    return sizeof(*this) + type_name.capacity();
}

}
