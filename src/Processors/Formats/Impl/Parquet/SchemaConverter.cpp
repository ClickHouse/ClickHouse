#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <Processors/Formats/Impl/Parquet/Decoding.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int DUPLICATE_COLUMN;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
    extern const int TYPE_MISMATCH;
    extern const int TOO_DEEP_RECURSION;
    extern const int NOT_IMPLEMENTED;
}

namespace DB::Parquet
{

SchemaConverter::SchemaConverter(const parq::FileMetaData & file_metadata_, const ReadOptions & options_, const Block * sample_block_) : file_metadata(file_metadata_), options(options_), sample_block(sample_block_), levels {LevelInfo {.def = 0, .rep = 0, .is_array = true}} {}

void SchemaConverter::checkHasColumns()
{
    if (file_metadata.schema.size() < 2)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet file has no columns");
    if (file_metadata.schema.at(0).num_children <= 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Schema root has no children");
}

void SchemaConverter::prepareForReading()
{
    chassert(sample_block);
    checkHasColumns();

    /// DFS the schema tree.
    size_t top_level_columns = size_t(file_metadata.schema.at(0).num_children);
    for (size_t i = 0; i < top_level_columns; ++i)
        processSubtree("", /*requested*/ false, /*type_hint*/ nullptr, SchemaContext::None);

    /// Check that all requested columns were found.
    std::vector<UInt8> found_columns(sample_block->columns());
    for (const auto & col : output_columns)
    {
        if (!col.idx_in_output_block.has_value())
            continue;
        size_t idx = col.idx_in_output_block.value();
        if (found_columns.at(idx))
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "There are multiple columns with name `{}` in the parquet file", sample_block->getByPosition(idx).name);
        found_columns[idx] = true;
    }
    for (size_t i = 0; i < found_columns.size(); ++i)
    {
        if (!found_columns[i])
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Column {} was not found in parquet schema", sample_block->getByPosition(i).name);
    }

    if (primitive_columns.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "No columns to read");
}

NamesAndTypesList SchemaConverter::inferSchema()
{
    chassert(!sample_block);
    checkHasColumns();
    size_t top_level_columns = size_t(file_metadata.schema.at(0).num_children);
    NamesAndTypesList res;
    for (size_t i = 0; i < top_level_columns; ++i)
    {
        std::optional<size_t> idx = processSubtree("", /*requested*/ true, /*type_hint*/ nullptr, SchemaContext::None);
        const OutputColumnInfo & col = output_columns.at(idx.value());
        res.emplace_back(col.name, col.final_type);
    }
    return res;
}

std::optional<size_t> SchemaConverter::processSubtree(String name, bool requested, DataTypePtr type_hint, SchemaContext context)
{
    if (type_hint)
        chassert(requested);
    if (schema_idx >= file_metadata.schema.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid parquet schema tree");
    const parq::SchemaElement & element = file_metadata.schema.at(schema_idx);
    schema_idx += 1;

    if (element.__isset.num_children && element.num_children <= 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Empty tuple in parquet schema");

    std::optional<size_t> idx_in_output_block;
    size_t output_idx = UINT64_MAX; // index in output_columns
    size_t wrap_in_arrays = 0;

    if (context == SchemaContext::None)
    {
        if (name != "")
            name += ".";
        name += element.name;

        if (sample_block)
        {
            /// Doing this lookup on each schema element to support reading individual tulple elements.
            /// E.g.:
            ///   insert into function file('t.parquet') select [(10,20,30)] as x;
            ///   select * from file('t.parquet', Parquet, '`x.2` Array(UInt8)'); -- outputs [20]
            std::optional<size_t> pos = sample_block->findPositionByName(name, options.case_insensitive_column_matching);
            if (pos.has_value())
            {
                if (requested)
                    throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Requested column {} is part of another requested column", name);

                requested = true;
                type_hint = sample_block->getByPosition(pos.value()).type;

                for (size_t i = 1; i < levels.size(); ++i)
                {
                    if (levels[i].is_array)
                    {
                        auto array = typeid_cast<const DataTypeArray *>(type_hint.get());
                        if (!array)
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of nested column {} doesn't match parquet schema: parquet type is Array, requested type is {}", name, type_hint->getName());
                        type_hint = array->getNestedType();
                        wrap_in_arrays += 1;
                    }
                }
                chassert(wrap_in_arrays == levels.back().rep);

                idx_in_output_block = pos;
            }
        }
    }

    size_t prev_levels_size = levels.size();
    SCOPE_EXIT(
        {
            chassert(levels.size() >= prev_levels_size);
            levels.resize(prev_levels_size);
        });

    if (element.repetition_type != parq::FieldRepetitionType::REQUIRED)
    {
        auto prev = levels.back();
        if (prev.def == UINT8_MAX)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Parquet column {} has extremely deeply nested (>255 levels) arrays or nullables", name);
        auto level = LevelInfo {.def = UInt8(prev.def + 1), .rep = prev.rep};
        if (element.repetition_type == parq::FieldRepetitionType::REPEATED)
        {
            level.rep += 1; // no overflow, rep <= def
            level.is_array = true;

            /// We'll first process schema for array element type, then wrap it in Array type.
            if (type_hint)
            {
                auto array_type = typeid_cast<const DataTypeArray *>(type_hint.get());
                if (!array_type)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Array, requested type is {}", name, type_hint->getName());
                type_hint = array_type->getNestedType();
            }
        }
        chassert(level.def == levels.size());
        levels.push_back(level);
    }

    /// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
    ///
    /// Primitive column:
    ///   (required|optional) leaf `name`
    /// Array:
    ///   required group `name` (List):
    ///     repeated group "list":
    ///       <recurse> "element"
    /// Array (old style):
    ///   repeated <recurse> `name`
    /// Map:
    ///   required group `name` (MAP or MAP_KEY_VALUE):
    ///     repeated group "key_value" (maybe MAP_KEY_VALUE):
    ///       reqiured <recurse> "key"
    ///       <recurse> "value"
    /// Tuple:
    ///   (required|optional) group `name`:
    ///     <recurse> `name1`
    ///     <recurse> `name2`
    ///     ...

    auto check_map = [&]
    {
        if (element.converted_type != parq::ConvertedType::MAP && element.converted_type != parq::ConvertedType::MAP_KEY_VALUE && !element.logicalType.__isset.MAP)
            return false;
        /// If an element is declared as MAP, but doesn't have the expected structure of children
        /// and grandchildren, we fall back to interpreting it as a tuple, as if there were no MAP
        /// annotation on it. Or if Tuple type was requested.
        if (context != SchemaContext::None && context != SchemaContext::ListElement)
            return false;
        if (typeid_cast<const DataTypeTuple *>(type_hint.get()))
            return false;
        if (element.num_children != 1)
            return false;
        const parq::SchemaElement & child = file_metadata.schema.at(schema_idx);
        if (child.repetition_type != parq::FieldRepetitionType::REPEATED || child.num_children != 2)
            return false;
        return true;
    };

    auto check_list = [&]
    {
        if (element.converted_type != parq::ConvertedType::LIST && !element.logicalType.__isset.LIST)
            return false;
        if (context != SchemaContext::None && context != SchemaContext::ListElement)
            return false;
        if (element.num_children != 1)
            return false;
        const parq::SchemaElement & child = file_metadata.schema.at(schema_idx);
        if (child.repetition_type != parq::FieldRepetitionType::REPEATED || child.num_children != 1)
            return false;
        return true;
    };

    if (!element.__isset.num_children)
    {
        /// Primitive column.
        primitive_column_idx += 1;
        if (!requested)
            return std::nullopt;
        if (!element.__isset.type)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet metadata is missing physical type for column {}", element.name);

        size_t primitive_idx = primitive_columns.size();
        PrimitiveColumnInfo & primitive = primitive_columns.emplace_back();
        primitive.column_idx = primitive_column_idx - 1;
        primitive.name = name;
        primitive.levels = levels;

        output_idx = output_columns.size();
        OutputColumnInfo & output = output_columns.emplace_back();
        output.name = name;
        output.primitive_start = primitive_idx;
        output.primitive_end = primitive_idx + 1;

        const IDataType * primitive_type_hint = type_hint.get();
        if (primitive_type_hint)
        {
            if (primitive_type_hint->lowCardinality())
            {
                primitive_type_hint = assert_cast<const DataTypeLowCardinality &>(*primitive_type_hint).getDictionaryType().get();
            }
            if (primitive_type_hint->isNullable())
            {
                primitive.output_nullable = true;
                primitive_type_hint = assert_cast<const DataTypeNullable &>(*primitive_type_hint).getNestedType().get();
            }
        }
        else
        {
            if ((primitive.levels.back().is_array == false || options.schema_inference_force_nullable) && !options.schema_inference_force_not_nullable)
            {
                /// This schema element is OPTIONAL or inside an OPTIONAL tuple.
                primitive.output_nullable = true;
            }
        }

        DataTypePtr inferred_type;
        processPrimitiveColumn(element, primitive_type_hint, primitive.decoder, primitive.raw_decoded_type, inferred_type);
        if (!primitive.raw_decoded_type)
            primitive.raw_decoded_type = inferred_type;

        primitive.intermediate_type = primitive.raw_decoded_type;
        if (primitive.output_nullable)
        {
            primitive.intermediate_type = std::make_shared<DataTypeNullable>(primitive.intermediate_type);
            inferred_type = std::make_shared<DataTypeNullable>(inferred_type);
        }

        primitive.final_type = type_hint ? type_hint : inferred_type;
        primitive.needs_cast = !primitive.final_type->equals(*primitive.intermediate_type);

        output.type = primitive.final_type;
    }
    else if (check_map())
    {
        /// Map, aka Array(Tuple(2)).
        DataTypePtr tuple_type_hint;
        if (type_hint)
        {
            auto map_type = typeid_cast<const DataTypeMap *>(type_hint.get());
            if (!map_type)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Map, requested type is {}", name, type_hint->getName());
            tuple_type_hint = map_type->getNestedType();
        }
        auto tuple_idx = processSubtree(name, requested, tuple_type_hint, SchemaContext::MapTuple);

        if (!requested)
            return std::nullopt;

        output_idx = output_columns.size();
        OutputColumnInfo & output = output_columns.emplace_back();
        const OutputColumnInfo & tuple = output_columns.at(tuple_idx.value());

        output.name = name;
        output.primitive_start = tuple.primitive_start;
        output.primitive_end = tuple.primitive_end;
        output.type = std::make_shared<DataTypeMap>(tuple.type);
        output.nested_columns = {tuple_idx.value()};
    }
    else if (check_list())
    {
        /// Array (outer schema element).
        auto array_idx = processSubtree(name, requested, type_hint, SchemaContext::ListTuple);

        if (!requested)
            return std::nullopt;

        output_idx = array_idx.value();
    }
    else if (context == SchemaContext::ListTuple)
    {
        /// Array (middle schema element).
        chassert(element.repetition_type == parq::FieldRepetitionType::REPEATED &&
                 element.num_children == 1); // caller checked this
        /// (type_hint is already unwrapped to be element type, because of REPEATED)
        auto element_idx = processSubtree(name, requested, type_hint, SchemaContext::ListElement);

        if (!requested)
            return std::nullopt;

        output_idx = element_idx.value();
    }
    else
    {
        /// Tuple. (Possibly a Map key_value tuple.)
        /// TODO: Test type hint having different element names.
        auto tuple_type_hint = typeid_cast<const DataTypeTuple *>(type_hint.get());
        if (type_hint && !tuple_type_hint)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Tuple, requested type is {}", name, type_hint->getName());

        bool lookup_by_name = false;
        std::vector<size_t> elements;
        if (type_hint)
        {
            if (tuple_type_hint->haveExplicitNames() && !tuple_type_hint->getElements().empty())
            {
                /// Allow reading a subset of tuple elements, matched by name, possibly reordered.
                lookup_by_name = true;
                elements.resize(tuple_type_hint->getElements().size(), UINT64_MAX);
            }
            else
            {
                if (tuple_type_hint->getElements().size() != size_t(element.num_children))
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Tuple with {} elements, requested type is Tuple with {} elements", name, element.num_children, tuple_type_hint->getElements().size());
            }
        }
        if (!lookup_by_name && requested)
            elements.resize(size_t(element.num_children), UINT64_MAX);

        Strings names;
        DataTypes types;
        if (!type_hint && requested)
        {
            names.resize(elements.size());
            types.resize(elements.size());
        }

        size_t primitive_start = primitive_columns.size();
        for (size_t i = 0; i < size_t(element.num_children); ++i)
        {
            const String & element_name = file_metadata.schema.at(schema_idx).name;
            std::optional<size_t> idx_in_output_tuple = i;
            if (lookup_by_name)
            {
                idx_in_output_tuple = tuple_type_hint->tryGetPositionByName(element_name, options.case_insensitive_column_matching);

                if (idx_in_output_tuple.has_value() && elements.at(idx_in_output_tuple.value()) != UINT64_MAX)
                    throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Parquet tuple {} has multiple elements with name `{}`", name, element_name);
            }

            DataTypePtr element_type_hint;
            if (type_hint && idx_in_output_tuple.has_value())
                element_type_hint = tuple_type_hint->getElement(idx_in_output_tuple.value());

            bool element_requested = requested && idx_in_output_tuple.has_value();
            auto element_idx = processSubtree(name, element_requested, element_type_hint, SchemaContext::None);

            if (element_requested)
            {
                elements.at(idx_in_output_tuple.value()) = element_idx.value();

                auto & type = output_columns.at(element_idx.value()).final_type;

                if (type_hint)
                {
                    chassert(type->equals(*element_type_hint));
                }
                else
                {
                    names.at(idx_in_output_tuple.value()) = element_name;
                    types.at(idx_in_output_tuple.value()) = type;
                }
            }
        }

        if (!requested)
            return std::nullopt;

        output_idx = output_columns.size();
        OutputColumnInfo & output = output_columns.emplace_back();
        output.name = name;
        output.primitive_start = primitive_start;
        output.primitive_end = primitive_columns.size();

        if (type_hint)
        {
            chassert(elements.size() == tuple_type_hint->getElements().size());
            for (size_t i = 0; i < elements.size(); ++i)
            {
                if (elements[i] == UINT64_MAX)
                    throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Requested tuple element {} of column {} was not found in parquet schema", tuple_type_hint->getNameByPosition(i), name);
            }
            output.type = type_hint;
        }
        else
        {
            output.type = std::make_shared<DataTypeTuple>(types, names);
        }

        output.nested_columns = elements;
    }

    if (!requested)
        return std::nullopt; // we just needed to recurse to children, not interested in output_idx

    chassert(output_idx != UINT64_MAX);

    if (element.repetition_type == parq::FieldRepetitionType::REPEATED)
    {
        /// Array of some kind. Can be a child of List or Map, or a standalone repeated field.
        /// We dispatch all 3 cases to this one code path to minimize probability of bugs.
        size_t array_idx = output_columns.size();
        OutputColumnInfo & array = output_columns.emplace_back();
        const OutputColumnInfo & array_element = output_columns.at(output_idx);
        array.name = name;
        array.primitive_start = array_element.primitive_start;
        array.primitive_end = primitive_columns.size();
        array.type = std::make_shared<DataTypeArray>(array_element.type);
        array.nested_columns = {output_idx};
        chassert(levels.size() == prev_levels_size + 1);
        array.rep = levels.back().rep;

        output_idx = array_idx;
    }

    if (idx_in_output_block.has_value())
    {
        /// If the requested column is inside some arrays of tuples (requested using `arr.elem`
        /// syntax), add intermediate OutputColumnInfo-s to create those arrays.
        for (size_t i = 0; i < wrap_in_arrays; ++i)
        {
            size_t array_idx = output_columns.size();
            OutputColumnInfo & array = output_columns.emplace_back();
            const OutputColumnInfo & array_element = output_columns.at(output_idx);
            array.name = name;
            array.primitive_start = array_element.primitive_start;
            array.primitive_end = primitive_columns.size();
            array.type = std::make_shared<DataTypeArray>(array_element.type);
            array.nested_columns = {output_idx};
            array.rep = levels[prev_levels_size - 1].rep - i;

            output_idx = array_idx;
        }
        output_columns[output_idx].idx_in_output_block = idx_in_output_block;
    }

    return output_idx;
}

void SchemaConverter::processPrimitiveColumn(
    const parq::SchemaElement & element, const IDataType * /*type_hint*/,
    std::unique_ptr<ValueDecoder> & out_decoder, DataTypePtr & out_decoded_type,
    DataTypePtr & out_inferred_type)
{
    /// Inputs:
    ///  * Parquet Type ("physical type"),
    ///  * Parquet ConvertedType (deprecated, but we have to support it),
    ///  * Parquet LogicalType,
    ///  * ClickHouse type hint (e.g. if the user specified column types explicitly).
    ///
    /// Outputs:
    ///  * out_decoder - how to decode the column (it then separately further dispatches to
    ///    different code paths depending on page encoding and nullability),
    ///  * out_inferred_type - data type most closely matching the parquet logical type, used for
    ///    schema inference.
    ///  * out_decoded_type - data type of decoding result, chosen for decoding convenience or speed
    ///    (e.g. matching the parquet physical type). If nullptr, equal to out_inferred_type.
    /// After parsing, columns are converted (using castColumn) from out_decoded_type to the final
    /// data type. E.g. maybe out_decoded_type is Int32 based on parquet physical type, but
    /// out_inferred_type is Int16 based on schema inference, and castColumn does the conversion.

    parq::Type::type type = element.type;
    parq::ConvertedType::type converted =
        element.__isset.converted_type ? element.converted_type : parq::ConvertedType::type(-1);
    const parq::LogicalType & logical = element.logicalType;
    using CONV = parq::ConvertedType;
    chassert(!out_inferred_type && !out_decoded_type);

    if (logical.__isset.STRING || logical.__isset.JSON || logical.__isset.BSON ||
        logical.__isset.ENUM || converted == CONV::UTF8 || converted == CONV::JSON ||
        converted == CONV::BSON || converted == CONV::ENUM)
    {
        if (type != parq::Type::BYTE_ARRAY && type != parq::Type::FIXED_LEN_BYTE_ARRAY)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected non-string physical type for string logical type: {}", thriftToString(element));
        /// Fall through to dispatch by physical type only.
    }
    else if (logical.__isset.INTEGER || (converted >= CONV::UINT_8 && converted <= CONV::INT_64))
    {
        const parq::IntType & integer = logical.INTEGER;
        bool is_signed = integer.isSigned;
        size_t bits = integer.bitWidth;
        if (!logical.__isset.INTEGER)
        {
            switch (converted)
            {
                case CONV::UINT_8: is_signed = false; bits = 8; break;
                case CONV::UINT_16: is_signed = false; bits = 16; break;
                case CONV::UINT_32: is_signed = false; bits = 32; break;
                case CONV::UINT_64: is_signed = false; bits = 64; break;
                case CONV::INT_8: is_signed = true; bits = 8; break;
                case CONV::INT_16: is_signed = true; bits = 16; break;
                case CONV::INT_32: is_signed = true; bits = 32; break;
                case CONV::INT_64: is_signed = true; bits = 64; break;
                default:
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected integer logical type: {}", thriftToString(element));
            }
        }
        if (!is_signed && bits == 8)
            out_inferred_type = std::make_shared<DataTypeUInt8>();
        else if (!is_signed && bits == 16)
            out_inferred_type = std::make_shared<DataTypeUInt16>();
        else if (!is_signed && bits == 32)
            out_inferred_type = std::make_shared<DataTypeUInt32>();
        else if (!is_signed && bits == 64)
            out_inferred_type = std::make_shared<DataTypeUInt64>();
        else if (is_signed && bits == 8)
            out_inferred_type = std::make_shared<DataTypeInt8>();
        else if (is_signed && bits == 16)
            out_inferred_type = std::make_shared<DataTypeInt16>();
        else if (is_signed && bits == 32)
            out_inferred_type = std::make_shared<DataTypeInt32>();
        else if (is_signed && bits == 64)
            out_inferred_type = std::make_shared<DataTypeInt64>();
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected integer logical type: {}", thriftToString(element));

        /// Can't leave the signed->unsigned conversion to castColumn.
        /// E.g. if parquet type is UINT64, and the requested clickhouse type is Int128,
        /// casting Int64 -> UInt64 -> Int128 produces different result from Int64 -> Int128.
        if (type == parq::Type::INT32)
        {
            if (bits == 8)
                out_decoder = std::make_unique<ShortIntDecoder<UInt8>>();
            else if (bits == 16)
                out_decoder = std::make_unique<ShortIntDecoder<UInt16>>();
            else
                out_decoder = std::make_unique<FixedSizeValueDecoder>(4);
        }
        else if (type == parq::Type::INT64)
        {
            out_decoder = std::make_unique<FixedSizeValueDecoder>(8);
            out_decoded_type = is_signed
                ? std::static_pointer_cast<IDataType>(std::make_shared<DataTypeInt64>())
                : std::static_pointer_cast<IDataType>(std::make_shared<DataTypeUInt64>());
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected non-int physical type for int logical type: {}", thriftToString(element));
        }

        return;
    }
    else if (logical.__isset.TIME || converted == CONV::TIME_MILLIS || converted == CONV::TIME_MICROS)
    {
        /// ClickHouse doesn't have data types for time of day.
        /// Fall through to dispatch by physical type only (as plain integer).
    }
    else if (logical.__isset.TIMESTAMP || converted == CONV::TIMESTAMP_MILLIS || converted == CONV::TIMESTAMP_MICROS)
    {
        UInt32 scale;
        if (logical.TIMESTAMP.unit.__isset.MILLIS || converted == CONV::TIMESTAMP_MILLIS)
            scale = 3;
        else if (logical.TIMESTAMP.unit.__isset.MICROS || converted == CONV::TIMESTAMP_MICROS)
            scale = 6;
        else if (logical.TIMESTAMP.unit.__isset.NANOS)
            scale = 9;
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected timestamp units: {}", thriftToString(element));

        if (type != parq::Type::INT64)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for timestamp logical type: {}", thriftToString(element));

        /// Can't leave int -> DateTime64 conversion to castColumn as it interprets the integer as seconds.
        out_decoder = std::make_unique<FixedSizeValueDecoder>(8);
        out_inferred_type = std::make_shared<DataTypeDateTime64>(scale);

        return;
    }
    else if (logical.__isset.DATE || converted == CONV::DATE)
    {
        if (type != parq::Type::INT32)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for date logical type: {}", thriftToString(element));

        out_decoder = std::make_unique<FixedSizeValueDecoder>(4);
        out_inferred_type = std::make_shared<DataTypeDate32>();

        return;
    }
    else if (logical.__isset.DECIMAL || converted == CONV::DECIMAL)
    {
        //TODO
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "decimal not implemented yet");
        // if physical is INT32:
        //     require that precision <= 9
        //     parse as Decimal32(scale) (memcpy)
        // else if physical is INT64:
        //     require that precision <= 18
        //     parse as Decimal64(scale) (memcpy)
        // else if physical is FIXED_LEN_BYTE_ARRAY:
        //     pick type:
        //         precision <= 9, length <= 4: Decimal32
        //         precision <= 18, length <= 8: Decimal64
        //         precision <= 38, length <= 16: Decimal128
        //         precision <= 76, length <= 32: Decimal256
        //         else: error
        //     parse (not memcpy, parquet data is big-endian, reverse bytes after reading; can do it in place though)
        // return
    }
    else if (logical.__isset.MAP || logical.__isset.LIST || converted == CONV::MAP ||
             converted == CONV::MAP_KEY_VALUE || converted == CONV::LIST)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected compound logical type for leaf column: {}", thriftToString(element));
    }
    else if (logical.__isset.UNKNOWN)
    {
        //TODO: DataTypeNothing (for now fall through to dispatch by physical type)
    }
    else if (logical.__isset.UUID)
    {
        if (type != parq::Type::FIXED_LEN_BYTE_ARRAY || element.type_length != 16)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for UUID column: {}", thriftToString(element));
        //TODO: check if byte order is correct
        out_decoder = std::make_unique<FixedSizeValueDecoder>(16);
        out_inferred_type = std::make_shared<DataTypeUUID>();
    }
    else if (logical.__isset.FLOAT16)
    {
        /// TODO: Support. For now, fall through to reading as FixedString(2).
    }
    else if (converted == CONV::INTERVAL)
    {
        /// TODO: Support. For now, fall through to reading as FixedString(12).
    }
    else if (element.__isset.logicalType || element.__isset.converted_type)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected logical/converted type: {}", thriftToString(element));
    }

    // If we didn't `return` above, dispatch by physical type.
    switch (type)
    {
        case parq::Type::BOOLEAN:
            //TODO
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BOOLEAN not implemented");
            //return;
        case parq::Type::INT32:
            out_decoder = std::make_unique<FixedSizeValueDecoder>(4);
            out_inferred_type = std::make_shared<DataTypeInt32>();
            return;
        case parq::Type::INT64:
            out_decoder = std::make_unique<FixedSizeValueDecoder>(8);
            out_inferred_type = std::make_shared<DataTypeInt64>();
            return;
        case parq::Type::INT96:
            //TODO
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "INT96 not implemented");
            //return;
        case parq::Type::FLOAT:
            out_decoder = std::make_unique<FixedSizeValueDecoder>(4);
            out_inferred_type = std::make_shared<DataTypeFloat32>();
            return;
        case parq::Type::DOUBLE:
            out_decoder = std::make_unique<FixedSizeValueDecoder>(8);
            out_inferred_type = std::make_shared<DataTypeFloat64>();
            return;
        case parq::Type::BYTE_ARRAY:
            out_decoder = std::make_unique<StringDecoder>();
            out_inferred_type = std::make_shared<DataTypeString>();
            return;
        case parq::Type::FIXED_LEN_BYTE_ARRAY:
            out_decoder = std::make_unique<FixedSizeValueDecoder>(size_t(element.type_length));
            out_inferred_type = std::make_shared<DataTypeFixedString>(size_t(element.type_length));
            return;
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type: {}", thriftToString(element));
}

}
