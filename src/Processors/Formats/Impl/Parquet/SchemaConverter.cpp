#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <Processors/Formats/Impl/Parquet/Decoding.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeFactory.h>

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
    for (const String & name : external_columns)
    {
        size_t idx = sample_block->getPositionByName(name, /* case_insensitive= */ false);
        if (found_columns.at(idx))
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Name clash between PREWHERE condition and a column in parquet file: {}", name);
        found_columns[idx] = true;
    }
    for (size_t i = 0; i < found_columns.size(); ++i)
    {
        if (!found_columns[i])
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Column {} was not found in parquet schema", sample_block->getByPosition(i).name);
    }
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
        if (idx.has_value())
        {
            const OutputColumnInfo & col = output_columns.at(idx.value());
            res.emplace_back(col.name, col.type);
        }
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

    /// `parquet.thrift` says "[num_children] is not set when the element is a primitive type".
    /// If it's set but has value 0, logically it would make sense to interpret it as empty tuple/struct.
    /// But in practice some writers are sloppy about it and set this field to 0 (rather than unset)
    /// for primitive columns. E.g.
    /// tests/queries/0_stateless/data_hive/partitioning/non_existing_column=Elizabeth/sample.parquet
    bool is_primitive = !element.__isset.num_children || (element.num_children == 0 && element.__isset.type);

    std::optional<size_t> idx_in_output_block;
    size_t output_idx = UINT64_MAX; // index in output_columns
    size_t wrap_in_arrays = 0;

    if (context == SchemaContext::None)
    {
        if (!name.empty())
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
                name = sample_block->getByPosition(pos.value()).name; // match case
                type_hint = sample_block->getByPosition(pos.value()).type;

                for (size_t i = 1; i < levels.size(); ++i)
                {
                    if (levels[i].is_array)
                    {
                        const DataTypeArray * array = typeid_cast<const DataTypeArray *>(type_hint.get());
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
        LevelInfo prev = levels.back();
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
                const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(type_hint.get());
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

    if (is_primitive)
    {
        /// Primitive column.
        primitive_column_idx += 1;
        if (!requested)
            return std::nullopt;
        if (!element.__isset.type)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet metadata is missing physical type for column {}", element.name);

        const IDataType * primitive_type_hint = type_hint.get();
        bool output_nullable = false;
        if (primitive_type_hint)
        {
            if (primitive_type_hint->lowCardinality())
            {
                primitive_type_hint = assert_cast<const DataTypeLowCardinality &>(*primitive_type_hint).getDictionaryType().get();
            }
            if (primitive_type_hint->isNullable())
            {
                output_nullable = true;
                primitive_type_hint = assert_cast<const DataTypeNullable &>(*primitive_type_hint).getNestedType().get();
            }
        }
        else
        {
            if ((levels.back().is_array == false || options.schema_inference_force_nullable) && !options.schema_inference_force_not_nullable)
            {
                /// This schema element is OPTIONAL or inside an OPTIONAL tuple.
                output_nullable = true;
            }
        }

        DataTypePtr inferred_type;
        DataTypePtr raw_decoded_type;
        PageDecoderInfo decoder;
        try
        {
            processPrimitiveColumn(element, primitive_type_hint, decoder, raw_decoded_type, inferred_type);
        }
        catch (Exception & e)
        {
            if (options.schema_inference_skip_unsupported_columns && (e.code() == ErrorCodes::INCORRECT_DATA || e.code() == ErrorCodes::NOT_IMPLEMENTED))
            {
                return std::nullopt;
            }
            else
            {
                e.addMessage("column '" + name + "'");
                throw;
            }
        }

        size_t primitive_idx = primitive_columns.size();
        PrimitiveColumnInfo & primitive = primitive_columns.emplace_back();
        primitive.column_idx = primitive_column_idx - 1;
        primitive.schema_idx = schema_idx - 1;
        primitive.name = name;
        primitive.levels = levels;
        primitive.output_nullable = output_nullable;
        primitive.decoder = std::move(decoder);
        primitive.raw_decoded_type = raw_decoded_type;
        for (const auto & level : levels)
            if (level.is_array)
                primitive.max_array_def = level.def;

        output_idx = output_columns.size();
        OutputColumnInfo & output = output_columns.emplace_back();
        output.name = name;
        output.primitive_start = primitive_idx;
        output.primitive_end = primitive_idx + 1;
        output.is_primitive = true;

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
        DataTypePtr array_type_hint;
        bool no_map = false; // return plain Array(Tuple) instead of Map
        if (type_hint)
        {
            if (const DataTypeMap * map_type = typeid_cast<const DataTypeMap *>(type_hint.get()))
            {
                array_type_hint = map_type->getNestedType();
            }
            else if (typeid_cast<const DataTypeArray *>(type_hint.get()))
            {
                array_type_hint = type_hint;
                no_map = true;
            }
            else
            {
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Map, requested type is {}", name, type_hint->getName());
            }
        }
        auto array_idx = processSubtree(name, requested, array_type_hint, SchemaContext::MapTuple);

        if (!requested || !array_idx.has_value())
            return std::nullopt;

        /// Support explicitly requesting Array(Tuple) type for map columns. Useful if the map key
        /// type is something that's not allowed as Map key in clickhouse.
        if (no_map)
            return array_idx;

        output_idx = output_columns.size();
        OutputColumnInfo & output = output_columns.emplace_back();
        const OutputColumnInfo & array = output_columns.at(array_idx.value());

        output.name = name;
        output.primitive_start = array.primitive_start;
        output.primitive_end = array.primitive_end;
        output.type = std::make_shared<DataTypeMap>(array.type);
        output.nested_columns = {array_idx.value()};
    }
    else if (check_list())
    {
        /// Array (outer schema element).
        auto array_idx = processSubtree(name, requested, type_hint, SchemaContext::ListTuple);

        if (!requested || !array_idx.has_value())
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

        if (!requested || !element_idx.has_value())
            return std::nullopt;

        output_idx = element_idx.value();
    }
    else
    {
        /// Tuple. (Possibly a Map key_value tuple.)
        const DataTypeTuple * tuple_type_hint = typeid_cast<const DataTypeTuple *>(type_hint.get());
        if (type_hint && !tuple_type_hint)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Requested type of column {} doesn't match parquet schema: parquet type is Tuple, requested type is {}", name, type_hint->getName());

        /// 3 modes:
        ///  * If type_hint has element names, we match elements from parquet to elements from type
        ///    hint tuple by name. If some elements are not in type hint, we skip them.
        ///    If elements are in different order, we reorder them to match type_hint.
        ///  * If type_hint has no names, we match elements sequentially and preserve order.
        ///  * If there's no type_hint, we preserve order, produce tuple with names.
        ///    Only in this mode, we allow skipping unsupported elements if
        ///    schema_inference_skip_unsupported_columns is true. In other modes, we skip the whole
        ///    tuple if any element is unsupported.

        bool lookup_by_name = false;
        std::vector<size_t> elements;
        if (type_hint)
        {
            if (tuple_type_hint->hasExplicitNames() && !tuple_type_hint->getElements().empty())
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
        size_t output_start = output_columns.size();
        size_t skipped_unsupported_columns = 0;
        for (size_t i = 0; i < size_t(element.num_children); ++i)
        {
            const String & element_name = file_metadata.schema.at(schema_idx).name;
            std::optional<size_t> idx_in_output_tuple = i - skipped_unsupported_columns;
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
                if (!element_idx.has_value())
                {
                    if (type_hint || context == SchemaContext::MapTuple)
                    {
                        /// If one of the elements is skipped, skip the whole tuple.
                        /// Remove previous elements.
                        primitive_columns.resize(primitive_start);
                        output_columns.resize(output_start);
                        return std::nullopt;
                    }
                    else
                    {
                        skipped_unsupported_columns += 1;
                        elements.pop_back();
                        names.pop_back();
                        types.pop_back();
                        continue;
                    }
                }

                elements.at(idx_in_output_tuple.value()) = element_idx.value();

                const auto & type = output_columns.at(element_idx.value()).type;
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
    const parq::SchemaElement & element, const IDataType * type_hint,
    PageDecoderInfo & out_decoder, DataTypePtr & out_decoded_type,
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
    ///    different code paths depending on page encoding),
    ///  * out_inferred_type - data type most closely matching the parquet logical type, used for
    ///    schema inference.
    ///  * out_decoded_type - data type of decoding result, chosen for decoding convenience or speed
    ///    (e.g. matching the parquet physical type). If nullptr, equal to out_inferred_type.
    /// After parsing, columns are converted (using castColumn) from out_decoded_type to the final
    /// data type. E.g. maybe out_decoded_type is Int32 based on parquet physical type, but
    /// out_inferred_type is Int16 based on schema inference, and castColumn does the conversion.

    parq::Type::type type = element.type;
    std::optional<parq::ConvertedType::type> converted =
        element.__isset.converted_type ? std::make_optional(element.converted_type) : std::nullopt;
    const parq::LogicalType & logical = element.logicalType;
    using CONV = parq::ConvertedType;
    chassert(!out_inferred_type && !out_decoded_type);
    out_decoder.physical_type = type;

    auto get_output_type_index = [&]
    {
        chassert(out_inferred_type);
        return type_hint ? type_hint->getTypeId() : out_inferred_type->getTypeId();
    };

    auto dispatch_int_stats_converter = [&](bool allow_datetime_and_ipv4, IntConverter & converter) -> bool
    {
        WhichDataType which(get_output_type_index());
        if (which.isNativeInteger())
            converter.field_signed = which.isNativeInt();
        else switch (which.idx)
        {
            case TypeIndex::IPv4:
                if (allow_datetime_and_ipv4)
                {
                    converter.field_ipv4 = true;
                    converter.field_signed = false;
                }
                else
                    return false;
                break;
            case TypeIndex::Date:
            case TypeIndex::Date32:
                converter.field_signed = false;
                break;
            case TypeIndex::DateTime:
                if (!allow_datetime_and_ipv4)
                    return false;
                converter.field_signed = false;
                break;
            case TypeIndex::Enum8:
            case TypeIndex::Enum16:
                break;
            /// Not supported: DateTime64, Decimal*, Float*
            /// Not possible (in most cases): String, FixedString
            default:
                return false;
        }
        return true;
    };

    auto is_output_type_decimal = [&](size_t expected_size, UInt32 expected_scale) -> bool
    {
        const IDataType * output_type = type_hint ? type_hint : out_inferred_type.get();
        WhichDataType which(output_type->getTypeId());
        if (which.isDecimal())
            return output_type->getSizeOfValueInMemory() == expected_size && getDecimalScale(*output_type) == expected_scale;
        else if (which.isDateTime64())
            return 8 == expected_size && assert_cast<const DataTypeDateTime64 *>(output_type)->getScale() == expected_scale;
        return false;
    };

    auto is_output_type_float = [&](size_t expected_size) -> bool
    {
        size_t size = 0;
        switch (get_output_type_index())
        {
            case TypeIndex::Float32: size = 4; break;
            case TypeIndex::Float64: size = 8; break;
            default: return false;
        }
        return size == expected_size;
    };

    auto is_output_type_fixed_string = [&](size_t expected_size) -> bool
    {
        const IDataType * output_type = type_hint ? type_hint : out_inferred_type.get();
        const DataTypeFixedString * fixed_string_type = typeid_cast<const DataTypeFixedString *>(output_type);
        return fixed_string_type && fixed_string_type->getN() == expected_size;
    };

    auto is_output_type_string = [&]() -> bool
    {
        return get_output_type_index() == TypeIndex::String;
    };

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
        bool is_signed = logical.INTEGER.isSigned;
        size_t bits = size_t(UInt8(logical.INTEGER.bitWidth));
        if (!logical.__isset.INTEGER)
        {
            switch (converted.value())
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

        size_t physical_bits;
        if (type == parq::Type::INT32)
            physical_bits = 32;
        else if (type == parq::Type::INT64)
            physical_bits = 64;
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected non-int physical type for int logical type: {}", thriftToString(element));

        bits = std::min(bits, physical_bits);

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
        auto converter = std::make_shared<IntConverter>();
        converter->input_size = physical_bits / 8;
        converter->input_signed = is_signed;
        if (physical_bits == 64)
        {
            out_decoded_type = is_signed
                ? std::static_pointer_cast<IDataType>(std::make_shared<DataTypeInt64>())
                : std::static_pointer_cast<IDataType>(std::make_shared<DataTypeUInt64>());
        }
        else if (bits == 8)
            converter->truncate_output = 1;
        else if (bits == 16)
            converter->truncate_output = 2;

        out_decoder.allow_stats = dispatch_int_stats_converter(/*allow_datetime_and_ipv4=*/ true, *converter);
        out_decoder.fixed_size_converter = std::move(converter);

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
        out_inferred_type = std::make_shared<DataTypeDateTime64>(scale);
        auto converter = std::make_shared<IntConverter>();
        converter->input_size = 8;
        if (is_output_type_decimal(8, scale))
        {
            converter->field_decimal_scale = scale;
            out_decoder.allow_stats = true;
        }
        else if (scale == 3 && type_hint && type_hint->getTypeId() == TypeIndex::DateTime)
        {
            /// We generally don't use stats when nontrivial type cast is required, but we make an
            /// exception for casting DateTime64(3) to DateTime. This comes up when round-tripping
            /// DateTime values through parquet. Our writer writes DateTime (seconds) as
            /// TIMESTAMP_MILLIS (milliseconds) because parquet doesn't have a more suitable type.
            /// It's probably common to then read it back with DateTime type hint. It's pretty
            /// important for min/max stats to work with timestamps, so we add this special case.
            ///
            /// We could generalize it and allow arbitrary Decimal scale and signedness conversions,
            /// but it doesn't seem worth the complexity and risk of bugs.
            converter->field_timestamp_from_millis = true;
            converter->field_signed = false;
            out_decoder.allow_stats = true;
        }
        out_decoder.fixed_size_converter = std::move(converter);

        return;
    }
    else if (logical.__isset.DATE || converted == CONV::DATE)
    {
        if (type != parq::Type::INT32)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for date logical type: {}", thriftToString(element));

        out_inferred_type = std::make_shared<DataTypeDate32>();
        auto converter = std::make_shared<IntConverter>();
        converter->input_size = 4;

        out_decoder.allow_stats = dispatch_int_stats_converter(/*allow_datetime_and_ipv4=*/ false, *converter);
        out_decoder.fixed_size_converter = std::move(converter);

        return;
    }
    else if (logical.__isset.DECIMAL || converted == CONV::DECIMAL)
    {
        UInt32 precision = logical.__isset.DECIMAL ? logical.DECIMAL.precision : element.precision;
        UInt32 scale = logical.__isset.DECIMAL ? logical.DECIMAL.scale : element.scale;
        precision = std::max(precision, scale);

        UInt32 max_precision = 0;
        if (type == parq::Type::INT32 || type == parq::Type::INT64)
        {
            max_precision = type == parq::Type::INT32 ? 9 : 18;
            auto converter = std::make_shared<IntConverter>();
            converter->input_size = type == parq::Type::INT32 ? 4 : 8;
            converter->field_decimal_scale = scale;
            out_decoder.fixed_size_converter = std::move(converter);
        }
        else if (type == parq::Type::FIXED_LEN_BYTE_ARRAY)
        {
            size_t input_size = element.type_length;

            if (input_size > 32)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet decimal value too long: {} bytes (at most 32 is supported)", input_size);

            if (precision <= 9 && input_size <= 4)
            {
                max_precision = 9;
                /// FIXED_LEN_BYTE_ARRAY decimals are big endian, while INT32/INT64 are little-endian.
                out_decoder.fixed_size_converter = std::make_shared<BigEndianDecimalFixedSizeConverter<Int32>>(input_size, scale);
            }
            else if (precision <= 18 && input_size <= 8)
            {
                max_precision = 18;
                out_decoder.fixed_size_converter = std::make_shared<BigEndianDecimalFixedSizeConverter<Int64>>(input_size, scale);
            }
            else if (precision <= 36 && input_size <= 16)
            {
                max_precision = 36;
                out_decoder.fixed_size_converter = std::make_shared<BigEndianDecimalFixedSizeConverter<Int128>>(input_size, scale);
            }
            else
            {
                max_precision = 76;
                out_decoder.fixed_size_converter = std::make_shared<BigEndianDecimalFixedSizeConverter<Int256>>(input_size, scale);
            }
        }
        else if (type == parq::Type::BYTE_ARRAY)
        {
            if (precision <= 9)
            {
                max_precision = 9;
                out_decoder.string_converter = std::make_shared<BigEndianDecimalStringConverter<Int32>>(scale);
            }
            else if (precision <= 18)
            {
                max_precision = 18;
                out_decoder.string_converter = std::make_shared<BigEndianDecimalStringConverter<Int64>>(scale);
            }
            else if (precision <= 36)
            {
                max_precision = 36;
                out_decoder.string_converter = std::make_shared<BigEndianDecimalStringConverter<Int128>>(scale);
            }
            else
            {
                max_precision = 76;
                out_decoder.string_converter = std::make_shared<BigEndianDecimalStringConverter<Int256>>(scale);
            }
        }
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for Decimal column: {}", thriftToString(type));

        if (precision > max_precision)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet decimal type precision or scale is too big ({} digits) for physical type {}", precision, thriftToString(type));

        out_inferred_type = createDecimal<DataTypeDecimal>(max_precision, scale);
        size_t output_size = out_inferred_type->getSizeOfValueInMemory();
        out_decoder.allow_stats = is_output_type_decimal(output_size, scale);

        return;
    }
    else if (logical.__isset.MAP || logical.__isset.LIST || converted == CONV::MAP ||
             converted == CONV::MAP_KEY_VALUE || converted == CONV::LIST)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected compound logical type for leaf column: {}", thriftToString(element));
    }
    else if (logical.__isset.UNKNOWN)
    {
        /// This means all values are nulls. We could use DataTypeNothing here, or fall through to
        /// dispatch by physical type. Either way seems fine, currently we do the latter.
    }
    else if (logical.__isset.UUID)
    {
        if (type != parq::Type::FIXED_LEN_BYTE_ARRAY || element.type_length != 16)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type for UUID column: {}", thriftToString(element));

        /// TODO [parquet]: Support UUIDs. Make sure to get the byte order right, it seems tricky.
        /// For now, fall through to reading as FixedString(16).
    }
    else if (logical.__isset.FLOAT16)
    {
        /// TODO [parquet]: Support. For now, fall through to reading as FixedString(2).
    }
    else if (converted == CONV::INTERVAL)
    {
        /// TODO [parquet]: Support. For now, fall through to reading as FixedString(12).
    }
    else if (element.__isset.logicalType || element.__isset.converted_type)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected logical/converted type: {}", thriftToString(element));
    }

    // If we didn't `return` above, dispatch by physical type.
    switch (type)
    {
        case parq::Type::BOOLEAN:
        {
            out_inferred_type = DataTypeFactory::instance().get("Bool");
            auto converter = std::make_shared<IntConverter>();
            converter->input_size = 1;
            out_decoder.allow_stats = dispatch_int_stats_converter(/*allow_datetime_and_ipv4=*/ false, *converter);
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
        case parq::Type::INT32:
        {
            out_inferred_type = std::make_shared<DataTypeInt32>();
            auto converter = std::make_shared<IntConverter>();
            converter->input_size = 4;
            out_decoder.allow_stats = dispatch_int_stats_converter(/*allow_datetime_and_ipv4=*/ true, *converter);
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
        case parq::Type::INT64:
        {
            out_inferred_type = std::make_shared<DataTypeInt64>();
            auto converter = std::make_shared<IntConverter>();
            converter->input_size = 8;
            out_decoder.allow_stats = dispatch_int_stats_converter(/*allow_datetime_and_ipv4=*/ false, *converter);
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
        case parq::Type::INT96:
            /// TODO [parquet]:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "INT96 not implemented");
            //(leave allow_stats == false, INT96 sort order is undefined)
            //return;
        case parq::Type::FLOAT:
        {
            out_inferred_type = std::make_shared<DataTypeFloat32>();
            auto converter = std::make_shared<FloatConverter<float>>();
            out_decoder.allow_stats = is_output_type_float(converter->input_size);
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
        case parq::Type::DOUBLE:
        {
            out_inferred_type = std::make_shared<DataTypeFloat64>();
            auto converter = std::make_shared<FloatConverter<double>>();
            out_decoder.allow_stats = is_output_type_float(converter->input_size);
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
        case parq::Type::BYTE_ARRAY:
        {
            out_inferred_type = std::make_shared<DataTypeString>();
            auto converter = std::make_shared<TrivialStringConverter>();
            out_decoder.allow_stats = is_output_type_string();
            out_decoder.string_converter = std::move(converter);
            return;
        }
        case parq::Type::FIXED_LEN_BYTE_ARRAY:
        {
            out_inferred_type = std::make_shared<DataTypeFixedString>(size_t(element.type_length));
            auto converter = std::make_shared<FixedStringConverter>();
            converter->input_size = size_t(element.type_length);
            out_decoder.allow_stats = is_output_type_fixed_string(converter->input_size) || is_output_type_string();
            out_decoder.fixed_size_converter = std::move(converter);
            return;
        }
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected physical type: {}", thriftToString(element));
}

}
