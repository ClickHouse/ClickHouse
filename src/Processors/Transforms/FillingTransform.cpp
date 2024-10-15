#include <Processors/Transforms/FillingTransform.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/IDataType.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>
#include <Common/FieldVisitorSum.h>
#include <Common/FieldVisitorToString.h>
#include <Common/logger_useful.h>


namespace DB
{

constexpr bool debug_logging_enabled = false;

template <typename T>
void logDebug(String key, const T & value, const char * separator = " : ")
{
    if constexpr (debug_logging_enabled)
    {
        WriteBufferFromOwnString ss;
        if constexpr (std::is_pointer_v<T>)
            ss << *value;
        else
            ss << value;

        LOG_DEBUG(getLogger("FillingTransform"), "{}{}{}", key, separator, ss.str());
    }
}

namespace ErrorCodes
{
    extern const int INVALID_WITH_FILL_EXPRESSION;
}

Block FillingTransform::transformHeader(Block header, const SortDescription & sort_description/*, const InterpolateDescription & interpolate_description*/)
{
    NameSet sort_keys;
    for (const auto & key : sort_description)
        sort_keys.insert(key.column_name);

    /// Columns which are not from sorting key may not be constant anymore.
    for (auto & column : header)
        if (column.column && isColumnConst(*column.column) && !sort_keys.contains(column.name))
            column.column = column.column->convertToFullColumnIfConst();

    return header;
}

template <typename T>
static FillColumnDescription::StepFunction getStepFunction(
    IntervalKind::Kind kind, Int64 step, const DateLUTImpl & date_lut, UInt16 scale = DataTypeDateTime64::default_scale)
{
    static const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");
    switch (kind) // NOLINT(bugprone-switch-missing-default-case)
    {
#define DECLARE_CASE(NAME) \
        case IntervalKind::Kind::NAME: \
            return [step, scale, &date_lut](Field & field) { \
                field = Add##NAME##sImpl::execute(static_cast<T>(\
                    field.safeGet<T>()), static_cast<Int32>(step), date_lut, utc_time_zone, scale); };

        FOR_EACH_INTERVAL_KIND(DECLARE_CASE)
#undef DECLARE_CASE
    }
}

static bool tryConvertFields(FillColumnDescription & descr, const DataTypePtr & type)
{
    auto max_type = Field::Types::Null;
    WhichDataType which(type);
    DataTypePtr to_type;

    /// For Date/DateTime types TO/FROM type should match column type
    if (descr.fill_from_type)
    {
        WhichDataType which_from(descr.fill_from_type);
        if ((which_from.isDateOrDate32() || which_from.isDateTime() || which_from.isDateTime64()) &&
            !descr.fill_from_type->equals(*removeNullable(type)))
                return false;
    }

    if (descr.fill_to_type)
    {
        WhichDataType which_to(descr.fill_to_type);
        if ((which_to.isDateOrDate32() || which_to.isDateTime() || which_to.isDateTime64()) &&
            !descr.fill_to_type->equals(*type))
                return false;
    }

    if (which.isInt128() || which.isUInt128())
    {
        max_type = Field::Types::Int128;
        to_type = type;
    }
    else if (which.isInt256() || which.isUInt256())
    {
        max_type = Field::Types::Int256;
        to_type = type;
    }
    else if (isInteger(type) || which.isDate() || which.isDate32() || which.isDateTime())
    {
        max_type = Field::Types::Int64;
        to_type = std::make_shared<DataTypeInt64>();
    }
    else if (which.isDateTime64())
    {
        max_type = Field::Types::Decimal64;
        const auto & date_type = static_cast<const DataTypeDateTime64 &>(*type);
        size_t precision = date_type.getPrecision();
        size_t scale = date_type.getScale();
        to_type = std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
    }
    else if (which.isFloat())
    {
        max_type = Field::Types::Float64;
        to_type = std::make_shared<DataTypeFloat64>();
    }
    else
        return false;

    if (descr.fill_from.getType() > max_type
        || descr.fill_to.getType() > max_type
        || descr.fill_step.getType() > max_type)
        return false;

    if (!descr.fill_from.isNull())
        descr.fill_from = convertFieldToTypeOrThrow(descr.fill_from, *to_type);
    if (!descr.fill_to.isNull())
        descr.fill_to = convertFieldToTypeOrThrow(descr.fill_to, *to_type);
    if (!descr.fill_step.isNull())
        descr.fill_step = convertFieldToTypeOrThrow(descr.fill_step, *to_type);

    if (descr.step_kind)
    {
        if (which.isDate() || which.isDate32())
        {
            Int64 avg_seconds = descr.fill_step.safeGet<Int64>() * descr.step_kind->toAvgSeconds();
            if (std::abs(avg_seconds) < 86400)
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                                "Value of step is to low ({} seconds). Must be >= 1 day", std::abs(avg_seconds));
        }

        if (which.isDate())
            descr.step_func = getStepFunction<UInt16>(*descr.step_kind, descr.fill_step.safeGet<Int64>(), DateLUT::instance());
        else if (which.isDate32())
            descr.step_func = getStepFunction<Int32>(*descr.step_kind, descr.fill_step.safeGet<Int64>(), DateLUT::instance());
        else if (const auto * date_time = checkAndGetDataType<DataTypeDateTime>(type.get()))
            descr.step_func = getStepFunction<UInt32>(*descr.step_kind, descr.fill_step.safeGet<Int64>(), date_time->getTimeZone());
        else if (const auto * date_time64 = checkAndGetDataType<DataTypeDateTime64>(type.get()))
        {
            const auto & step_dec = descr.fill_step.safeGet<const DecimalField<Decimal64> &>();
            Int64 step = DecimalUtils::convertTo<Int64>(step_dec.getValue(), step_dec.getScale());
            static const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");

            switch (*descr.step_kind) // NOLINT(bugprone-switch-missing-default-case)
            {
#define DECLARE_CASE(NAME) \
                case IntervalKind::Kind::NAME: \
                    descr.step_func = [step, &time_zone = date_time64->getTimeZone()](Field & field) \
                    { \
                        auto field_decimal = field.safeGet<DecimalField<DateTime64>>(); \
                        auto res = Add##NAME##sImpl::execute(field_decimal.getValue(), step, time_zone, utc_time_zone, field_decimal.getScale()); \
                        field = DecimalField(res, field_decimal.getScale()); \
                    }; \
                    break;

                FOR_EACH_INTERVAL_KIND(DECLARE_CASE)
#undef DECLARE_CASE
            }
        }
        else
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                            "STEP of Interval type can be used only with Date/DateTime types, but got {}", type->getName());
    }
    else
    {
        descr.step_func = [step = descr.fill_step](Field & field)
        {
            applyVisitor(FieldVisitorSum(step), field);
        };
    }

    return true;
}

FillingTransform::FillingTransform(
    const Block & header_,
    const SortDescription & sort_description_,
    const SortDescription & fill_description_,
    InterpolateDescriptionPtr interpolate_description_,
    const bool use_with_fill_by_sorting_prefix_)
    : ISimpleTransform(header_, transformHeader(header_, fill_description_), true)
    , sort_description(sort_description_)
    , fill_description(fill_description_)
    , interpolate_description(interpolate_description_)
    , filling_row(fill_description_)
    , next_row(fill_description_)
    , use_with_fill_by_sorting_prefix(use_with_fill_by_sorting_prefix_)
{
    if (interpolate_description)
        interpolate_actions = std::make_shared<ExpressionActions>(interpolate_description->actions.clone());

    std::vector<bool> is_fill_column(header_.columns());
    for (size_t i = 0, size = fill_description.size(); i < size; ++i)
    {
        if (interpolate_description && interpolate_description->result_columns_set.contains(fill_description[i].column_name))
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "Column '{}' is participating in ORDER BY ... WITH FILL expression and can't be INTERPOLATE output",
                fill_description[i].column_name);

        size_t block_position = header_.getPositionByName(fill_description[i].column_name);
        is_fill_column[block_position] = true;
        fill_column_positions.push_back(block_position);

        auto & descr = filling_row.getFillDescription(i);

        const Block & output_header = getOutputPort().getHeader();
        const DataTypePtr & type = removeNullable(output_header.getByPosition(block_position).type);

        if (!tryConvertFields(descr, type))
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "Incompatible types of WITH FILL expression values with column type {}", type->getName());

        if (isUInt(type) &&
            ((!descr.fill_from.isNull() && less(descr.fill_from, Field{0}, 1)) ||
             (!descr.fill_to.isNull() && less(descr.fill_to, Field{0}, 1))))
        {
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "WITH FILL bound values cannot be negative for unsigned type {}", type->getName());
        }
    }
    logDebug("fill description", dumpSortDescription(fill_description));

    std::unordered_set<size_t> ordinary_sort_positions;
    for (const auto & desc : sort_description)
    {
        if (!desc.with_fill)
            ordinary_sort_positions.insert(header_.getPositionByName(desc.column_name));
    }

    std::unordered_set<size_t> unique_positions;
    for (auto pos : fill_column_positions)
    {
        if (!unique_positions.insert(pos).second)
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION, "Multiple WITH FILL for identical expressions is not supported in ORDER BY");
        if (ordinary_sort_positions.contains(pos))
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION, "ORDER BY containing the same expression with and without WITH FILL modifier is not supported");
    }

    if (use_with_fill_by_sorting_prefix)
    {
        /// build sorting prefix for first fill column
        for (const auto & desc : sort_description)
        {
            if (desc.column_name == fill_description[0].column_name)
                break;

            size_t pos = header_.getPositionByName(desc.column_name);
            sort_prefix_positions.push_back(pos);

            sort_prefix.push_back(desc);
        }
        logDebug("sort prefix", dumpSortDescription(sort_prefix));
        last_range_sort_prefix.reserve(sort_prefix.size());
    }

    size_t idx = 0;
    for (const ColumnWithTypeAndName & column : header_.getColumnsWithTypeAndName())
    {
        if (interpolate_description)
            if (const auto & p = interpolate_description->required_columns_map.find(column.name);
                p != interpolate_description->required_columns_map.end())
                input_positions.emplace_back(idx, p->second);

        if (!is_fill_column[idx] && !(interpolate_description && interpolate_description->result_columns_set.contains(column.name))
            && sort_prefix_positions.end() == std::find(sort_prefix_positions.begin(), sort_prefix_positions.end(), idx))
            other_column_positions.push_back(idx);

        ++idx;
    }

    if (interpolate_description)
        for (const auto & name : interpolate_description->result_columns_order)
            interpolate_column_positions.push_back(header_.getPositionByName(name));

    /// check conflict in positions between interpolate and sorting prefix columns
    if (!sort_prefix_positions.empty() && !interpolate_column_positions.empty())
    {
        std::unordered_set<size_t> interpolate_positions(interpolate_column_positions.begin(), interpolate_column_positions.end());
        for (auto sort_prefix_pos : sort_prefix_positions)
        {
            if (interpolate_positions.contains(sort_prefix_pos))
                throw Exception(
                    ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "The same column in ORDER BY before WITH FILL (sorting prefix) and INTERPOLATE is not allowed. Column: {}",
                    (header_.begin() + sort_prefix_pos)->name);
        }
    }
}

/// prepare() is overrididen to call transform() after all chunks are processed
/// it can be necessary for suffix generation in case of WITH FILL .. TO is provided
IProcessor::Status FillingTransform::prepare()
{
    if (input.isFinished() && !output.isFinished() && !has_input && !all_chunks_processed)
    {
        logDebug("prepare()", "all chunks processed");
        all_chunks_processed = true;

        /// push output data to output port if we can
        if (has_output && output.canPush())
        {
            output.pushData(std::move(output_data));
            has_output = false;
        }

        /// return Ready to call transform() for generating filling rows after latest chunk was processed
        return Status::Ready;
    }

    return ISimpleTransform::prepare();
}

void FillingTransform::interpolate(const MutableColumns & result_columns, Block & interpolate_block)
{
    if (interpolate_description)
    {
        interpolate_block.clear();

        if (!input_positions.empty())
        {
            /// populate calculation block with required columns with values from previous row
            for (const auto & [col_pos, name_type] : input_positions)
            {
                MutableColumnPtr column = name_type.type->createColumn();
                const auto * res_column = result_columns[col_pos].get();
                size_t size = res_column->size();
                if (size == 0) /// this is the first row in current chunk
                {
                    /// take value from last row of previous chunk if exists, else use default
                    if (last_row.size() > col_pos && !last_row[col_pos]->empty())
                        column->insertFrom(*last_row[col_pos], 0);
                    else
                        column->insertDefault();
                }
                else /// take value from previous row of current chunk
                    column->insertFrom(*res_column, size - 1);

                interpolate_block.insert({std::move(column), name_type.type, name_type.name});
            }
            interpolate_actions->execute(interpolate_block);
        }
        else /// all INTERPOLATE expressions are constants
        {
            size_t n = 1;
            interpolate_actions->execute(interpolate_block, n);
        }
    }
}

using MutableColumnRawPtrs = std::vector<IColumn*>;

void FillingTransform::insertFromFillingRow(
    const MutableColumnRawPtrs & filling_columns,
    const MutableColumnRawPtrs & interpolate_columns,
    const MutableColumnRawPtrs & other_columns,
    const Block & interpolate_block)
{
    logDebug("insertFromFillingRow", filling_row);

    for (size_t i = 0, size = filling_columns.size(); i < size; ++i)
    {
        if (filling_row[i].isNull())
            filling_columns[i]->insertDefault();
        else
            filling_columns[i]->insert(filling_row[i]);
    }

    if (size_t size = interpolate_block.columns())
    {
        Columns columns = interpolate_block.getColumns();
        for (size_t i = 0; i < size; ++i)
            interpolate_columns[i]->insertFrom(*columns[i]->convertToFullColumnIfConst(), 0);
    }
    else
    {
        for (auto * interpolate_column : interpolate_columns)
            interpolate_column->insertDefault();
    }

    for (auto * other_column : other_columns)
        other_column->insertDefault();

    filling_row_inserted = true;
}

static void copyRowFromColumns(const MutableColumnRawPtrs & dest, const Columns & source, size_t row_num)
{
    chassert(dest.size() == source.size());

    for (size_t i = 0, size = source.size(); i < size; ++i)
        dest[i]->insertFrom(*source[i], row_num);
}

static void initColumnsByPositions(
    const Columns & input_columns,
    Columns & input_columns_by_positions,
    const MutableColumns & output_columns,
    MutableColumnRawPtrs & output_columns_by_position,
    const std::vector<size_t> & positions)
{
    for (const size_t pos : positions)
    {
        input_columns_by_positions.push_back(input_columns[pos]);
        output_columns_by_position.push_back(output_columns[pos].get());
    }
}

void FillingTransform::initColumns(
    const Columns & input_columns,
    Columns & input_fill_columns,
    Columns & input_interpolate_columns,
    Columns & input_sort_prefix_columns,
    Columns & input_other_columns,
    MutableColumns & output_columns,
    MutableColumnRawPtrs & output_fill_columns,
    MutableColumnRawPtrs & output_interpolate_columns,
    MutableColumnRawPtrs & output_sort_prefix_columns,
    MutableColumnRawPtrs & output_other_columns)
{
    Columns non_const_columns;
    non_const_columns.reserve(input_columns.size());

    for (const auto & column : input_columns)
        non_const_columns.push_back(column->convertToFullColumnIfConst());

    for (const auto & column : non_const_columns)
        output_columns.push_back(column->cloneEmpty()->assumeMutable());

    initColumnsByPositions(non_const_columns, input_fill_columns, output_columns, output_fill_columns, fill_column_positions);
    initColumnsByPositions(
        non_const_columns, input_interpolate_columns, output_columns, output_interpolate_columns, interpolate_column_positions);
    initColumnsByPositions(non_const_columns, input_sort_prefix_columns, output_columns, output_sort_prefix_columns, sort_prefix_positions);
    initColumnsByPositions(non_const_columns, input_other_columns, output_columns, output_other_columns, other_column_positions);
}

bool FillingTransform::generateSuffixIfNeeded(const Columns & input_columns, MutableColumns & result_columns)
{
    Columns input_fill_columns;
    Columns input_interpolate_columns;
    Columns input_sort_prefix_columns;
    Columns input_other_columns;
    MutableColumnRawPtrs res_fill_columns;
    MutableColumnRawPtrs res_interpolate_columns;
    MutableColumnRawPtrs res_sort_prefix_columns;
    MutableColumnRawPtrs res_other_columns;

    initColumns(
        input_columns,
        input_fill_columns,
        input_interpolate_columns,
        input_sort_prefix_columns,
        input_other_columns,
        result_columns,
        res_fill_columns,
        res_interpolate_columns,
        res_sort_prefix_columns,
        res_other_columns);

    return generateSuffixIfNeeded(result_columns, res_fill_columns, res_interpolate_columns, res_sort_prefix_columns, res_other_columns);
}

bool FillingTransform::generateSuffixIfNeeded(
    const MutableColumns & result_columns,
    MutableColumnRawPtrs res_fill_columns,
    MutableColumnRawPtrs res_interpolate_columns,
    MutableColumnRawPtrs res_sort_prefix_columns,
    MutableColumnRawPtrs res_other_columns)
{
    logDebug("generateSuffixIfNeeded() filling_row", filling_row);
    logDebug("generateSuffixIfNeeded() next_row", next_row);

    /// Determines if we should insert filling row before start generating next rows
    bool should_insert_first = (next_row < filling_row && !filling_row_inserted) || next_row.isNull();
    logDebug("should_insert_first", should_insert_first);

    for (size_t i = 0, size = filling_row.size(); i < size; ++i)
        next_row[i] = filling_row.getFillDescription(i).fill_to;

    logDebug("generateSuffixIfNeeded() next_row updated", next_row);

    if (filling_row >= next_row)
    {
        logDebug("generateSuffixIfNeeded()", "no need to generate suffix");
        return false;
    }

    Block interpolate_block;
    if (should_insert_first && filling_row < next_row)
    {
        interpolate(result_columns, interpolate_block);
        insertFromFillingRow(res_fill_columns, res_interpolate_columns, res_other_columns, interpolate_block);
        /// fulfill sort prefix columns with last row values or defaults
        if (!last_range_sort_prefix.empty())
            copyRowFromColumns(res_sort_prefix_columns, last_range_sort_prefix, 0);
        else
            for (auto * sort_prefix_column : res_sort_prefix_columns)
                sort_prefix_column->insertDefault();
    }

    bool filling_row_changed = false;
    while (true)
    {
        const auto [apply, changed] = filling_row.next(next_row);
        filling_row_changed = changed;
        if (!apply)
            break;

        interpolate(result_columns, interpolate_block);
        insertFromFillingRow(res_fill_columns, res_interpolate_columns, res_other_columns, interpolate_block);
        /// fulfill sort prefix columns with last row values or defaults
        if (!last_range_sort_prefix.empty())
            copyRowFromColumns(res_sort_prefix_columns, last_range_sort_prefix, 0);
        else
            for (auto * sort_prefix_column : res_sort_prefix_columns)
                sort_prefix_column->insertDefault();
    }
    /// new valid filling row was generated but not inserted
    if (filling_row_changed)
        filling_row_inserted = false;

    return true;
}

template <typename Predicate>
size_t getRangeEnd(size_t begin, size_t end, Predicate pred)
{
    chassert(begin < end);

    const size_t linear_probe_threadhold = 16;
    size_t linear_probe_end = begin + linear_probe_threadhold;
    linear_probe_end = std::min(linear_probe_end, end);

    for (size_t pos = begin; pos < linear_probe_end; ++pos)
    {
        if (!pred(begin, pos))
            return pos;
    }

    size_t low = linear_probe_end;
    size_t high = end - 1;
    while (low <= high)
    {
        size_t mid = low + (high - low) / 2;
        if (pred(begin, mid))
            low = mid + 1;
        else
        {
            high = mid - 1;
            end = mid;
        }
    }
    return end;
}

void FillingTransform::transformRange(
    const Columns & input_fill_columns,
    const Columns & input_interpolate_columns,
    const Columns & input_sort_prefix_columns,
    const Columns & input_other_columns,
    const MutableColumns & result_columns,
    const MutableColumnRawPtrs & res_fill_columns,
    const MutableColumnRawPtrs & res_interpolate_columns,
    const MutableColumnRawPtrs & res_sort_prefix_columns,
    const MutableColumnRawPtrs & res_other_columns,
    std::pair<size_t, size_t> range,
    const bool new_sorting_prefix)
{
    const size_t range_begin = range.first;
    const size_t range_end = range.second;

    Block interpolate_block;
    if (new_sorting_prefix)
    {
        logDebug("--- new range ---", range_end);
        for (size_t i = 0, size = filling_row.size(); i < size; ++i)
        {
            const auto current_value = (*input_fill_columns[i])[range_begin];
            const auto & fill_from = filling_row.getFillDescription(i).fill_from;

            if (!fill_from.isNull() && !equals(current_value, fill_from))
            {
                filling_row.initFromDefaults(i);
                filling_row_inserted = false;
                if (less(fill_from, current_value, filling_row.getDirection(i)))
                {
                    interpolate(result_columns, interpolate_block);
                    insertFromFillingRow(res_fill_columns, res_interpolate_columns, res_other_columns, interpolate_block);
                    copyRowFromColumns(res_sort_prefix_columns, input_sort_prefix_columns, range_begin);
                }
                break;
            }
            filling_row[i] = current_value;
        }
    }

    for (size_t row_ind = range_begin; row_ind < range_end; ++row_ind)
    {
        logDebug("row", row_ind);
        logDebug("filling_row", filling_row);
        logDebug("next_row", next_row);

        bool should_insert_first = next_row < filling_row;
        logDebug("should_insert_first", should_insert_first);

        for (size_t i = 0, size = filling_row.size(); i < size; ++i)
        {
            const auto current_value = (*input_fill_columns[i])[row_ind];
            const auto & fill_to = filling_row.getFillDescription(i).fill_to;

            if (fill_to.isNull() || less(current_value, fill_to, filling_row.getDirection(i)))
                next_row[i] = current_value;
            else
                next_row[i] = fill_to;
        }
        logDebug("next_row updated", next_row);

        /// The condition is true when filling row is initialized by value(s) in FILL FROM,
        /// and there are row(s) in current range with value(s) < then in the filling row.
        /// It can happen only once for a range.
        if (should_insert_first && filling_row < next_row)
        {
            interpolate(result_columns, interpolate_block);
            insertFromFillingRow(res_fill_columns, res_interpolate_columns, res_other_columns, interpolate_block);
            copyRowFromColumns(res_sort_prefix_columns, input_sort_prefix_columns, row_ind);
        }

        bool filling_row_changed = false;
        while (true)
        {
            const auto [apply, changed] = filling_row.next(next_row);
            filling_row_changed = changed;
            if (!apply)
                break;

            interpolate(result_columns, interpolate_block);
            insertFromFillingRow(res_fill_columns, res_interpolate_columns, res_other_columns, interpolate_block);
            copyRowFromColumns(res_sort_prefix_columns, input_sort_prefix_columns, row_ind);
        }
        /// new valid filling row was generated but not inserted, will use it during suffix generation
        if (filling_row_changed)
            filling_row_inserted = false;

        logDebug("filling_row after", filling_row);

        copyRowFromColumns(res_fill_columns, input_fill_columns, row_ind);
        copyRowFromColumns(res_interpolate_columns, input_interpolate_columns, row_ind);
        copyRowFromColumns(res_sort_prefix_columns, input_sort_prefix_columns, row_ind);
        copyRowFromColumns(res_other_columns, input_other_columns, row_ind);
    }

    /// save sort prefix of last row in the range, it's used to generate suffix
    last_range_sort_prefix.clear();
    for (const auto & sort_prefix_column : input_sort_prefix_columns)
    {
        auto column = sort_prefix_column->cloneEmpty();
        column->insertFrom(*sort_prefix_column, range_end - 1);
        last_range_sort_prefix.push_back(std::move(column));
    }
}

void FillingTransform::transform(Chunk & chunk)
{
    logDebug("new chunk rows", chunk.getNumRows());
    logDebug("all chunks processed", all_chunks_processed);

    /// if got chunk with no rows and it's not for suffix generation, then just skip it
    /// Note: ExpressionTransform can return chunk with no rows, see 02579_fill_empty_chunk.sql for example
    if (!chunk.hasRows() && !all_chunks_processed)
        return;

    Columns input_fill_columns;
    Columns input_interpolate_columns;
    Columns input_sort_prefix_columns;
    Columns input_other_columns;
    MutableColumnRawPtrs res_fill_columns;
    MutableColumnRawPtrs res_interpolate_columns;
    MutableColumnRawPtrs res_sort_prefix_columns;
    MutableColumnRawPtrs res_other_columns;
    MutableColumns result_columns;

    Block interpolate_block;

    if (all_chunks_processed)
    {
        chassert(!chunk.hasRows());

        /// if all chunks are processed, then we may need to generate suffix for the following cases:
        /// (1) when all data are processed and WITH FILL .. TO is provided
        /// (2) for empty result set when WITH FILL FROM .. TO is provided (see PR #30888)

        /// if no data was processed, then need to initialize filling_row
        if (last_row.empty())
        {
            filling_row.initFromDefaults();
            filling_row_inserted = false;
        }

        if (generateSuffixIfNeeded(input.getHeader().getColumns(), result_columns))
        {
            size_t num_output_rows = result_columns[0]->size();
            chunk.setColumns(std::move(result_columns), num_output_rows);
        }

        return;
    }

    chassert(chunk.hasRows());

    const size_t num_rows = chunk.getNumRows();
    auto input_columns = chunk.detachColumns();
    initColumns(
        input_columns,
        input_fill_columns,
        input_interpolate_columns,
        input_sort_prefix_columns,
        input_other_columns,
        result_columns,
        res_fill_columns,
        res_interpolate_columns,
        res_sort_prefix_columns,
        res_other_columns);

    if (sort_prefix.empty() || !use_with_fill_by_sorting_prefix)
    {
        transformRange(
            input_fill_columns,
            input_interpolate_columns,
            input_sort_prefix_columns,
            input_other_columns,
            result_columns,
            res_fill_columns,
            res_interpolate_columns,
            res_sort_prefix_columns,
            res_other_columns,
            {0, num_rows},
            last_row.empty());

        saveLastRow(result_columns);
        size_t num_output_rows = result_columns[0]->size();
        chunk.setColumns(std::move(result_columns), num_output_rows);
        return;
    }

    /// check if last row in prev chunk had the same sorting prefix as the first in new one
    /// if not, we need to reinitialize filling row
    bool new_sort_prefix = last_row.empty();
    if (!last_row.empty())
    {
        ColumnRawPtrs last_sort_prefix_columns;
        last_sort_prefix_columns.reserve(sort_prefix.size());
        for (size_t pos : sort_prefix_positions)
            last_sort_prefix_columns.push_back(last_row[pos].get());

        new_sort_prefix = false;
        for (size_t i = 0; i < input_sort_prefix_columns.size(); ++i)
        {
            const int res = input_sort_prefix_columns[i]->compareAt(0, 0, *last_sort_prefix_columns[i], sort_prefix[i].nulls_direction);
            if (res != 0)
            {
                new_sort_prefix = true;
                break;
            }
        }
    }

    for (size_t row_ind = 0; row_ind < num_rows;)
    {
        /// find next range
        auto current_sort_prefix_end_pos = getRangeEnd(
            row_ind,
            num_rows,
            [&](size_t pos_with_current_sort_prefix, size_t row_pos)
            {
                for (size_t i = 0; i < input_sort_prefix_columns.size(); ++i)
                {
                    const int res = input_sort_prefix_columns[i]->compareAt(
                        pos_with_current_sort_prefix, row_pos, *input_sort_prefix_columns[i], sort_prefix[i].nulls_direction);
                    if (res != 0)
                        return false;
                }
                return true;
            });

        /// generate suffix for the previous range
        if (!last_range_sort_prefix.empty() && new_sort_prefix)
            generateSuffixIfNeeded(result_columns, res_fill_columns, res_interpolate_columns, res_sort_prefix_columns, res_other_columns);

        transformRange(
            input_fill_columns,
            input_interpolate_columns,
            input_sort_prefix_columns,
            input_other_columns,
            result_columns,
            res_fill_columns,
            res_interpolate_columns,
            res_sort_prefix_columns,
            res_other_columns,
            {row_ind, current_sort_prefix_end_pos},
            new_sort_prefix);

        logDebug("range end", current_sort_prefix_end_pos);
        row_ind = current_sort_prefix_end_pos;
        new_sort_prefix = true;
    }

    saveLastRow(result_columns);
    size_t num_output_rows = result_columns[0]->size();
    chunk.setColumns(std::move(result_columns), num_output_rows);
}

void FillingTransform::saveLastRow(const MutableColumns & result_columns)
{
    last_row.clear();

    const size_t num_rows = result_columns[0]->size();
    if (num_rows == 0)
        return;

    for (const auto & result_column : result_columns)
    {
        auto column = result_column->cloneEmpty();
        column->insertFrom(*result_column, num_rows - 1);
        last_row.push_back(std::move(column));
    }
}
}
