#include <Processors/Formats/Impl/Parquet/ParquetFilterCondition.h>

#if USE_PARQUET

#include <Functions/IFunction.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/HilbertUtils.h>
#include <Common/MortonUtils.h>
#include <Processors/Formats/Impl/Parquet/ParquetBloomFilterCondition.h>
#include <parquet/bloom_filter.h>
#include <parquet/xxhasher.h>

namespace DB
{

static Field applyFunctionForField(
    const FunctionBasePtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value)
{
    ColumnsWithTypeAndName columns
        {
            { arg_type->createColumnConst(1, arg_value), arg_type, "x" },
        };

    auto col = func->execute(columns, func->getResultType(), 1);
    return (*col)[0];
}

/// applyFunction will execute the function with one `field` or the column which `field` refers to.
static FieldRef applyFunction(const FunctionBasePtr & func, const DataTypePtr & current_type, const FieldRef & field)
{
    chassert(func != nullptr);
    /// Fallback for fields without block reference.
    if (field.isExplicit())
        return applyFunctionForField(func, current_type, field);

    /// We will cache the function result inside `field.columns`, because this function will call many times
    /// from many fields from same column. When the column is huge, for example there are thousands of marks, we need a cache.
    /// The cache key is like `_[function_pointer]_[param_column_id]` to identify a unique <function, param> pair.
    WriteBufferFromOwnString buf;
    writeText("_", buf);
    writePointerHex(func.get(), buf);
    writeText("_" + toString(field.column_idx), buf);
    String result_name = buf.str();
    const auto & columns = field.columns;
    size_t result_idx = columns->size();

    for (size_t i = 0; i < result_idx; ++i)
    {
        if ((*columns)[i].name == result_name)
            result_idx = i;
    }

    if (result_idx == columns->size())
    {
        /// When cache is missed, we calculate the whole column where the field comes from. This will avoid repeated calculation.
        ColumnsWithTypeAndName args{(*columns)[field.column_idx]};
        field.columns->emplace_back(ColumnWithTypeAndName {nullptr, func->getResultType(), result_name});
        (*columns)[result_idx].column = func->execute(args, (*columns)[result_idx].type, columns->front().column->size());
    }

    return {field.columns, field.row_idx, result_idx};
}

std::optional<Range> applyMonotonicFunctionsChainToRange(
    Range key_range,
    const KeyCondition::MonotonicFunctionsChain & functions,
    DataTypePtr current_type,
    bool single_point)
{
    for (const auto & func : functions)
    {
        /// We check the monotonicity of each function on a specific range.
        /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
        IFunction::Monotonicity monotonicity = single_point
            ? IFunction::Monotonicity{true}
            : func->getMonotonicityForRange(*current_type.get(), key_range.left, key_range.right);

        if (!monotonicity.is_monotonic)
        {
            return {};
        }

        /// If we apply function to open interval, we can get empty intervals in result.
        /// E.g. for ('2020-01-03', '2020-01-20') after applying 'toYYYYMM' we will get ('202001', '202001').
        /// To avoid this we make range left and right included.
        /// Any function that treats NULL specially is not monotonic.
        /// Thus we can safely use isNull() as an -Inf/+Inf indicator here.
        if (!key_range.left.isNull())
        {
            key_range.left = applyFunction(func, current_type, key_range.left);
            key_range.left_included = true;
        }

        if (!key_range.right.isNull())
        {
            key_range.right = applyFunction(func, current_type, key_range.right);
            key_range.right_included = true;
        }

        current_type = func->getResultType();

        if (!monotonicity.is_positive)
            key_range.invert();
    }
    return key_range;
}

const parquet::ColumnDescriptor * getColumnDescriptorIfBloomFilterIsPresent(
    const std::unique_ptr<parquet::RowGroupMetaData> & parquet_rg_metadata,
    const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
    std::size_t clickhouse_column_index)
{
    if (clickhouse_column_index_to_parquet_index.size() <= clickhouse_column_index)
    {
        return nullptr;
    }

    const auto & parquet_indexes = clickhouse_column_index_to_parquet_index[clickhouse_column_index].parquet_indexes;

    // complex types like structs, tuples and maps will have more than one index.
    // we don't support those for now
    if (parquet_indexes.size() > 1)
    {
        return nullptr;
    }

    if (parquet_indexes.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Something bad happened, raise an issue and try the query with `input_format_parquet_bloom_filter_push_down=false`");
    }

    auto parquet_column_index = parquet_indexes[0];

    const auto * parquet_column_descriptor = parquet_rg_metadata->schema()->Column(parquet_column_index);

    bool column_has_bloom_filter = parquet_rg_metadata->ColumnChunk(parquet_column_index)->bloom_filter_offset().has_value();
    if (!column_has_bloom_filter)
    {
        return nullptr;
    }

    return parquet_column_descriptor;
}


bool isParquetStringTypeSupportedForBloomFilters(
    const std::shared_ptr<const parquet::LogicalType> & logical_type,
    parquet::ConvertedType::type converted_type)
{
    if (logical_type &&
        !logical_type->is_none()
        && !(logical_type->is_string() || logical_type->is_BSON() || logical_type->is_JSON()))
    {
        return false;
    }

    if (parquet::ConvertedType::type::NONE != converted_type &&
        !(converted_type == parquet::ConvertedType::JSON || converted_type == parquet::ConvertedType::UTF8
          || converted_type == parquet::ConvertedType::BSON))
    {
        return false;
    }

    return true;
}

bool isParquetIntegerTypeSupportedForBloomFilters(const std::shared_ptr<const parquet::LogicalType> & logical_type, parquet::ConvertedType::type converted_type)
{
    if (logical_type && !logical_type->is_none() && !logical_type->is_int())
    {
        return false;
    }

    if (parquet::ConvertedType::type::NONE != converted_type && !(converted_type == parquet::ConvertedType::INT_8 || converted_type == parquet::ConvertedType::INT_16
                                                                  || converted_type == parquet::ConvertedType::INT_32 || converted_type == parquet::ConvertedType::INT_64
                                                                  || converted_type == parquet::ConvertedType::UINT_8 || converted_type == parquet::ConvertedType::UINT_16
                                                                  || converted_type == parquet::ConvertedType::UINT_32 || converted_type == parquet::ConvertedType::UINT_64))
    {
        return false;
    }

    return true;
}

template <typename T>
uint64_t hashSpecialFLBATypes(const Field & field)
{
    const T & value = field.safeGet<T>();

    parquet::FLBA flba(reinterpret_cast<const uint8_t*>(&value));

    parquet::XxHasher hasher;

    return hasher.Hash(&flba, sizeof(T));
};

std::optional<uint64_t> tryHashStringWithoutCompatibilityCheck(const Field & field)
{
    const auto field_type = field.getType();

    if (field_type != Field::Types::Which::String)
    {
        return std::nullopt;
    }

    parquet::XxHasher hasher;
    parquet::ByteArray ba { field.safeGet<std::string>() };

    return hasher.Hash(&ba);
}

std::optional<uint64_t> tryHashString(
    const Field & field,
    const std::shared_ptr<const parquet::LogicalType> & logical_type,
    parquet::ConvertedType::type converted_type)
{
    if (!isParquetStringTypeSupportedForBloomFilters(logical_type, converted_type))
    {
        return std::nullopt;
    }

    return tryHashStringWithoutCompatibilityCheck(field);
}

std::optional<uint64_t> tryHashFLBA(
    const Field & field,
    const std::shared_ptr<const parquet::LogicalType> & logical_type,
    parquet::ConvertedType::type converted_type,
    std::size_t parquet_column_length)
{
    if (!isParquetStringTypeSupportedForBloomFilters(logical_type, converted_type))
    {
        return std::nullopt;
    }

    const auto field_type = field.getType();

    if (field_type == Field::Types::Which::IPv6 && parquet_column_length == sizeof(IPv6))
    {
        return hashSpecialFLBATypes<IPv6>(field);
    }

    return tryHashStringWithoutCompatibilityCheck(field);
}

template <typename ParquetPhysicalType>
std::optional<uint64_t> tryHashInt(const Field & field, const std::shared_ptr<const parquet::LogicalType> & logical_type, parquet::ConvertedType::type converted_type)
{
    if (!isParquetIntegerTypeSupportedForBloomFilters(logical_type, converted_type))
    {
        return std::nullopt;
    }

    parquet::XxHasher hasher;

    if (field.getType() == Field::Types::Which::Int64)
    {
        return hasher.Hash(static_cast<ParquetPhysicalType>(field.safeGet<int64_t>()));
    }
    else if (field.getType() == Field::Types::Which::UInt64)
    {
        return hasher.Hash(static_cast<ParquetPhysicalType>(field.safeGet<uint64_t>()));
    }
    else if (field.getType() == Field::Types::IPv4)
    {
        /*
         * In theory, we could accept IPv4 over 64 bits variables. It would only be a problem in case it was hashed using the byte array api
         * with a zero-ed buffer that had a 32 bits variable copied into it.
         *
         * To be on the safe side, accept only in case physical type is 32 bits.
         * */
        if constexpr (std::is_same_v<int32_t, ParquetPhysicalType>)
        {
            return hasher.Hash(static_cast<ParquetPhysicalType>(field.safeGet<IPv4>()));
        }
    }

    return std::nullopt;
}

std::optional<uint64_t> tryHash(const Field & field, const parquet::ColumnDescriptor * parquet_column_descriptor)
{
    const auto physical_type = parquet_column_descriptor->physical_type();
    const auto & logical_type = parquet_column_descriptor->logical_type();
    const auto converted_type = parquet_column_descriptor->converted_type();

    switch (physical_type)
    {
        case parquet::Type::type::INT32:
            return tryHashInt<int32_t>(field, logical_type, converted_type);
        case parquet::Type::type::INT64:
            return tryHashInt<int64_t>(field, logical_type, converted_type);
        case parquet::Type::type::BYTE_ARRAY:
            return tryHashString(field, logical_type, converted_type);
        case parquet::Type::type::FIXED_LEN_BYTE_ARRAY:
            return tryHashFLBA(field, logical_type, converted_type, parquet_column_descriptor->type_length());
        default:
            return std::nullopt;
    }
}

std::optional<std::vector<uint64_t>> hash(const IColumn * data_column, const parquet::ColumnDescriptor * parquet_column_descriptor)
{
    std::vector<uint64_t> hashes;

    for (size_t i = 0u; i < data_column->size(); i++)
    {
        Field f;
        data_column->get(i, f);

        auto hashed_value = tryHash(f, parquet_column_descriptor);

        if (!hashed_value)
        {
            return std::nullopt;
        }

        hashes.emplace_back(*hashed_value);
    }

    return hashes;
}

bool maybeTrueOnBloomFilter(const std::vector<uint64_t> & hashes, const std::unique_ptr<parquet::BloomFilter> & bloom_filter)
{
    for (const auto hash : hashes)
    {
        if (bloom_filter->FindHash(hash))
        {
            return true;
        }
    }

    return false;
}

bool mayBeTrueOnParquetRowGroup(const ParquetFilterCondition::BloomFilterData & condition_bloom_filter_data,
                                const ParquetBloomFilterCondition::ColumnIndexToBF & column_index_to_column_bf)
{
    bool maybe_true = true;
    for (auto column_index = 0u; column_index < condition_bloom_filter_data.hashes_per_column.size(); column_index++)
    {
        // in case bloom filter is not present for this row group
        // https://github.com/ClickHouse/ClickHouse/pull/62966#discussion_r1722361237
        if (!column_index_to_column_bf.contains(condition_bloom_filter_data.key_columns[column_index]))
        {
            continue;
        }

        bool column_maybe_contains = maybeTrueOnBloomFilter(
            condition_bloom_filter_data.hashes_per_column[column_index],
            column_index_to_column_bf.at(condition_bloom_filter_data.key_columns[column_index]));

        if (!column_maybe_contains)
        {
            maybe_true = false;
            break;
        }
    }

    return maybe_true;
}

std::vector<ParquetFilterCondition::ConditionElement> abcdefgh(
    const std::vector<KeyCondition::RPNElement> & rpn,
    const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
    const std::unique_ptr<parquet::RowGroupMetaData> & parquet_rg_metadata)
{
    std::vector<ParquetFilterCondition::ConditionElement> condition_elements;

    using RPNElement = KeyCondition::RPNElement;

    for (const auto & rpn_element : rpn)
    {
        condition_elements.emplace_back(rpn_element);
        // this would be a problem for `where negate(x) = -58`.
        // It would perform a bf search on `-58`, and possibly miss row groups containing this data.
        if (!rpn_element.monotonic_functions_chain.empty())
        {
            continue;
        }

        ParquetBloomFilterCondition::ConditionElement::HashesForColumns hashes;

        if (rpn_element.function == RPNElement::FUNCTION_IN_RANGE
            || rpn_element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
            // Only FUNCTION_EQUALS is supported and for that extremes need to be the same
            if (rpn_element.range.left != rpn_element.range.right)
            {
                continue;
            }

            const auto * parquet_column_descriptor =
                getColumnDescriptorIfBloomFilterIsPresent(parquet_rg_metadata, clickhouse_column_index_to_parquet_index, rpn_element.key_column);

            if (!parquet_column_descriptor)
            {
                continue;
            }

            auto hashed_value = tryHash(rpn_element.range.left, parquet_column_descriptor);

            if (!hashed_value)
            {
                continue;
            }

            std::vector<uint64_t> hashes_for_column;
            hashes_for_column.emplace_back(*hashed_value);

            hashes.emplace_back(std::move(hashes_for_column));

            std::vector<std::size_t> key_columns;
            key_columns.emplace_back(rpn_element.key_column);

            condition_elements.back().bloom_filter_data = ParquetFilterCondition::BloomFilterData {std::move(hashes), std::move(key_columns)};
        }
        else if (rpn_element.function == RPNElement::FUNCTION_IN_SET
                 || rpn_element.function == RPNElement::FUNCTION_NOT_IN_SET)
        {
            const auto & set_index = rpn_element.set_index;
            const auto & ordered_set = set_index->getOrderedSet();
            const auto & indexes_mapping = set_index->getIndexesMapping();
            bool found_empty_column = false;

            std::vector<std::size_t> key_columns;

            for (auto i = 0u; i < ordered_set.size(); i++)
            {
                const auto & set_column = ordered_set[i];

                const auto * parquet_column_descriptor = getColumnDescriptorIfBloomFilterIsPresent(
                    parquet_rg_metadata,
                    clickhouse_column_index_to_parquet_index,
                    indexes_mapping[i].key_index);

                if (!parquet_column_descriptor)
                {
                    continue;
                }

                auto column = set_column;

                if (column->empty())
                {
                    found_empty_column = true;
                    break;
                }

                if (const auto & nullable_column = checkAndGetColumn<ColumnNullable>(set_column.get()))
                {
                    column = nullable_column->getNestedColumnPtr();
                }

                auto hashes_for_column_opt = hash(column.get(), parquet_column_descriptor);

                if (!hashes_for_column_opt)
                {
                    continue;
                }

                auto & hashes_for_column = *hashes_for_column_opt;

                if (hashes_for_column.empty())
                {
                    continue;
                }

                hashes.emplace_back(hashes_for_column);

                key_columns.push_back(indexes_mapping[i].key_index);
            }

            if (found_empty_column)
            {
                // todo arthur
                continue;
            }

            if (hashes.empty())
            {
                continue;
            }

            condition_elements.back().bloom_filter_data = {std::move(hashes), std::move(key_columns)};
        }
    }

    return condition_elements;
}

BoolMask ParquetFilterCondition::check(const std::vector<ConditionElement> & rpn,
                                       const Hyperrectangle & hyperrectangle,
                                       const KeyCondition::SpaceFillingCurveDescriptions & key_space_filling_curves,
                                       const DataTypes & data_types,
                                       const ParquetBloomFilterCondition::ColumnIndexToBF & column_index_to_column_bf,
                                       bool single_point)
{
    std::vector<BoolMask> rpn_stack;

    auto curve_type = [&](size_t key_column_pos)
    {
        for (const auto & curve : key_space_filling_curves)
            if (curve.key_column_pos == key_column_pos)
                return curve.type;
        return KeyCondition::SpaceFillingCurveType::Unknown;
    };

    for (const auto & element : rpn)
    {
        if (element.argument_num_of_space_filling_curve.has_value())
        {
            // todo arthur, not sure what to do here yet
            /// If a condition on argument of a space filling curve wasn't collapsed into FUNCTION_ARGS_IN_HYPERRECTANGLE,
            /// we cannot process it.
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == ConditionElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == ConditionElement::FUNCTION_IN_RANGE
                 || element.function == ConditionElement::FUNCTION_NOT_IN_RANGE)
        {
            if (element.key_column >= hyperrectangle.size())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Hyperrectangle size is {}, but requested element at posittion {} ({})",
                                hyperrectangle.size(), element.key_column, element.toString());
            }

            const Range * key_range = &hyperrectangle[element.key_column];

            /// The case when the column is wrapped in a chain of possibly monotonic functions.
            Range transformed_range = Range::createWholeUniverse();
            if (!element.monotonic_functions_chain.empty())
            {
                std::optional<Range> new_range = applyMonotonicFunctionsChainToRange(
                    *key_range,
                    element.monotonic_functions_chain,
                    data_types[element.key_column],
                    single_point
                );

                if (!new_range)
                {
                    rpn_stack.emplace_back(true, true);

                    if (element.bloom_filter_data)
                    {
                        rpn_stack.back().can_be_true = mayBeTrueOnParquetRowGroup(*element.bloom_filter_data, column_index_to_column_bf);
                    }

                    continue;
                }
                transformed_range = *new_range;
                key_range = &transformed_range;
            }

            bool intersects = element.range.intersectsRange(*key_range);
            bool contains = element.range.containsRange(*key_range);

            rpn_stack.emplace_back(intersects, !contains);

            if (rpn_stack.back().can_be_true && element.bloom_filter_data)
            {
                rpn_stack.back().can_be_true = mayBeTrueOnParquetRowGroup(*element.bloom_filter_data, column_index_to_column_bf);
            }

            if (element.function == ConditionElement::FUNCTION_NOT_IN_RANGE)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == ConditionElement::FUNCTION_ARGS_IN_HYPERRECTANGLE)
        {
            /** The case of space-filling curves.
              * We unpack the range of a space filling curve into hyperrectangles of their arguments,
              * and then check the intersection of them with the given hyperrectangle from the key condition.
              *
              * Note: you might find this code hard to understand,
              * because there are three different hyperrectangles involved:
              *
              * 1. A hyperrectangle derived from the range of the table's sparse index (marks granule): `hyperrectangle`
              *    We analyze its dimension `key_range`, corresponding to the `key_column`.
              *    For example, the table's key is a single column `mortonEncode(x, y)`,
              *    the current granule is [500, 600], and it means that
              *    mortonEncode(x, y) in [500, 600]
              *
              * 2. A hyperrectangle derived from the key condition, e.g.
              *    `x >= 10 AND x <= 20 AND y >= 20 AND y <= 30` defines: (x, y) in [10, 20] × [20, 30]
              *
              * 3. A set of hyperrectangles that we obtain by inverting the space-filling curve on the range:
              *    From mortonEncode(x, y) in [500, 600]
              *    We get (x, y) in [30, 31] × [12, 13]
              *        or (x, y) in [28, 31] × [14, 15];
              *        or (x, y) in [0, 7] × [16, 23];
              *        or (x, y) in [8, 11] × [16, 19];
              *        or (x, y) in [12, 15] × [16, 17];
              *        or (x, y) in [12, 12] × [18, 18];
              *
              *  And we analyze the intersection of (2) and (3).
              */

            Range key_range = hyperrectangle[element.key_column];

            /// The only possible result type of a space filling curve is UInt64.
            /// We also only check bounded ranges.
            if (key_range.left.getType() == Field::Types::UInt64
                && key_range.right.getType() == Field::Types::UInt64)
            {
                key_range.shrinkToIncludedIfPossible();

                size_t num_dimensions = element.space_filling_curve_args_hyperrectangle.size();

                /// Let's support only the case of 2d, because I'm not confident in other cases.
                if (num_dimensions == 2)
                {
                    UInt64 left = key_range.left.safeGet<UInt64>();
                    UInt64 right = key_range.right.safeGet<UInt64>();

                    BoolMask mask(false, true);
                    auto hyperrectangle_intersection_callback = [&](std::array<std::pair<UInt64, UInt64>, 2> curve_hyperrectangle)
                    {
                        BoolMask current_intersection(true, false);
                        for (size_t dim = 0; dim < num_dimensions; ++dim)
                        {
                            const Range & condition_arg_range = element.space_filling_curve_args_hyperrectangle[dim];

                            const Range curve_arg_range(
                                curve_hyperrectangle[dim].first, true,
                                curve_hyperrectangle[dim].second, true);

                            bool intersects = condition_arg_range.intersectsRange(curve_arg_range);
                            bool contains = condition_arg_range.containsRange(curve_arg_range);

                            current_intersection = current_intersection & BoolMask(intersects, !contains);
                        }

                        mask = mask | current_intersection;
                    };

                    switch (curve_type(element.key_column))
                    {
                        case KeyCondition::SpaceFillingCurveType::Hilbert:
                        {
                            hilbertIntervalToHyperrectangles2D(left, right, hyperrectangle_intersection_callback);
                            break;
                        }
                        case KeyCondition::SpaceFillingCurveType::Morton:
                        {
                            mortonIntervalToHyperrectangles<2>(left, right, hyperrectangle_intersection_callback);
                            break;
                        }
                        case KeyCondition::SpaceFillingCurveType::Unknown:
                        {
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "curve_type is `Unknown`. It is a bug.");
                        }
                    }

                    rpn_stack.emplace_back(mask);
                }
                else
                    rpn_stack.emplace_back(true, true);
            }
            else
                rpn_stack.emplace_back(true, true);

            /** Note: we can consider implementing a simpler solution, based on "hidden keys".
              * It means, when we have a table's key like (a, b, mortonCurve(x, y))
              * we extract the arguments from the curves, and append them to the key,
              * imagining that we have the key (a, b, mortonCurve(x, y), x, y)
              *
              * Then while we analyze the granule's range between (a, b, mortonCurve(x, y))
              * and decompose it to the series of hyperrectangles,
              * we can construct a series of hyperrectangles of the extended key (a, b, mortonCurve(x, y), x, y),
              * and then do everything as usual.
              *
              * This approach is generalizable to any functions, that have preimage of interval
              * represented by a set of hyperrectangles.
              */
        }
        else if (element.function == ConditionElement::FUNCTION_POINT_IN_POLYGON)
        {
            /** There are 2 kinds of polygons:
              *   1. Polygon by minmax index
              *   2. Polygons which is provided by user
              *
              * Polygon by minmax index:
              *   For hyperactangle [1, 2] × [3, 4] we can create a polygon with 4 points: (1, 3), (1, 4), (2, 4), (2, 3)
              *
              * Algorithm:
              *   Check whether there is any intersection of the 2 polygons. If true return {true, true}, else return {false, true}.
              */
            const auto & key_column_positions = element.point_in_polygon_column_description->key_column_positions;

            Float64 x_min = applyVisitor(FieldVisitorConvertToNumber<Float64>(), hyperrectangle[key_column_positions[0]].left);
            Float64 x_max = applyVisitor(FieldVisitorConvertToNumber<Float64>(), hyperrectangle[key_column_positions[0]].right);
            Float64 y_min = applyVisitor(FieldVisitorConvertToNumber<Float64>(), hyperrectangle[key_column_positions[1]].left);
            Float64 y_max = applyVisitor(FieldVisitorConvertToNumber<Float64>(), hyperrectangle[key_column_positions[1]].right);

            if (unlikely(isNaN(x_min) || isNaN(x_max) || isNaN(y_min) || isNaN(y_max)))
            {
                rpn_stack.emplace_back(true, true);
                continue;
            }

            using Point = boost::geometry::model::d2::point_xy<Float64>;
            using Polygon = boost::geometry::model::polygon<Point>;
            Polygon  polygon_by_minmax_index;
            polygon_by_minmax_index.outer().emplace_back(x_min, y_min);
            polygon_by_minmax_index.outer().emplace_back(x_min, y_max);
            polygon_by_minmax_index.outer().emplace_back(x_max, y_max);
            polygon_by_minmax_index.outer().emplace_back(x_max, y_min);

            /// Close ring
            boost::geometry::correct(polygon_by_minmax_index);

            /// Because the polygon may have a hole so the "can_be_false" should always be true.
            rpn_stack.emplace_back(
                boost::geometry::intersects(polygon_by_minmax_index, element.polygon), true);
        }
        else if (
            element.function == ConditionElement::FUNCTION_IS_NULL
            || element.function == ConditionElement::FUNCTION_IS_NOT_NULL)
        {
            const Range * key_range = &hyperrectangle[element.key_column];

            /// No need to apply monotonic functions as nulls are kept.
            bool intersects = element.range.intersectsRange(*key_range);
            bool contains = element.range.containsRange(*key_range);

            rpn_stack.emplace_back(intersects, !contains);
            if (element.function == ConditionElement::FUNCTION_IS_NULL)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (
            element.function == ConditionElement::FUNCTION_IN_SET
            || element.function == ConditionElement::FUNCTION_NOT_IN_SET)
        {
            if (!element.set_index)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Set for IN is not created yet");

            rpn_stack.emplace_back(element.set_index->checkInRange(hyperrectangle, data_types, single_point));

            if (rpn_stack.back().can_be_true && element.bloom_filter_data)
            {
                rpn_stack.back().can_be_true = mayBeTrueOnParquetRowGroup(*element.bloom_filter_data, column_index_to_column_bf);
            }

            if (element.function == ConditionElement::FUNCTION_NOT_IN_SET)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == ConditionElement::FUNCTION_NOT)
        {
            assert(!rpn_stack.empty());

            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == ConditionElement::FUNCTION_AND)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == ConditionElement::FUNCTION_OR)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == ConditionElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else if (element.function == ConditionElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::ConditionElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::checkInHyperrectangle");

    return rpn_stack[0];
}

}

#endif
