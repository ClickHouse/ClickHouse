#include <Processors/Transforms/FillingTransform.h>
#include <Interpreters/convertFieldToType.h>
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

        LOG_DEBUG(&Poco::Logger::get("FillingTransform"), "{}{}{}", key, separator, ss.str());
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
    IntervalKind kind, Int64 step, const DateLUTImpl & date_lut, UInt16 scale = DataTypeDateTime64::default_scale)
{
    switch (kind)
    {
#define DECLARE_CASE(NAME) \
        case IntervalKind::NAME: \
            return [step, scale, &date_lut](Field & field) { \
                field = Add##NAME##sImpl::execute(static_cast<T>(\
                    field.get<T>()), static_cast<Int32>(step), date_lut, scale); };

        FOR_EACH_INTERVAL_KIND(DECLARE_CASE)
#undef DECLARE_CASE
    }
    UNREACHABLE();
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
            Int64 avg_seconds = descr.fill_step.get<Int64>() * descr.step_kind->toAvgSeconds();
            if (std::abs(avg_seconds) < 86400)
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                                "Value of step is to low ({} seconds). Must be >= 1 day", std::abs(avg_seconds));
        }

        if (which.isDate())
            descr.step_func = getStepFunction<UInt16>(*descr.step_kind, descr.fill_step.get<Int64>(), DateLUT::instance());
        else if (which.isDate32())
            descr.step_func = getStepFunction<Int32>(*descr.step_kind, descr.fill_step.get<Int64>(), DateLUT::instance());
        else if (const auto * date_time = checkAndGetDataType<DataTypeDateTime>(type.get()))
            descr.step_func = getStepFunction<UInt32>(*descr.step_kind, descr.fill_step.get<Int64>(), date_time->getTimeZone());
        else if (const auto * date_time64 = checkAndGetDataType<DataTypeDateTime64>(type.get()))
        {
            const auto & step_dec = descr.fill_step.get<const DecimalField<Decimal64> &>();
            Int64 step = DecimalUtils::convertTo<Int64>(step_dec.getValue(), step_dec.getScale());

            switch (*descr.step_kind)
            {
#define DECLARE_CASE(NAME) \
                case IntervalKind::NAME: \
                    descr.step_func = [step, &time_zone = date_time64->getTimeZone()](Field & field) \
                    { \
                        auto field_decimal = field.get<DecimalField<DateTime64>>(); \
                        auto res = Add##NAME##sImpl::execute(field_decimal.getValue(), step, time_zone, field_decimal.getScale()); \
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
        const Block & header_, const SortDescription & sort_description_, InterpolateDescriptionPtr interpolate_description_)
        : ISimpleTransform(header_, transformHeader(header_, sort_description_), true)
        , sort_description(sort_description_)
        , interpolate_description(interpolate_description_)
        , filling_row(sort_description_)
        , next_row(sort_description_)
{
    if (interpolate_description)
        interpolate_actions = std::make_shared<ExpressionActions>(interpolate_description->actions);

    std::vector<bool> is_fill_column(header_.columns());
    for (size_t i = 0, size = sort_description.size(); i < size; ++i)
    {
        if (interpolate_description && interpolate_description->result_columns_set.contains(sort_description[i].column_name))
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "Column '{}' is participating in ORDER BY ... WITH FILL expression and can't be INTERPOLATE output",
                sort_description[i].column_name);

        size_t block_position = header_.getPositionByName(sort_description[i].column_name);
        is_fill_column[block_position] = true;
        fill_column_positions.push_back(block_position);

        auto & descr = filling_row.getFillDescription(i);

        const Block & output_header = getOutputPort().getHeader();
        const DataTypePtr & type = removeNullable(output_header.getByPosition(block_position).type);

        if (!tryConvertFields(descr, type))
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "Incompatible types of WITH FILL expression values with column type {}", type->getName());

        if (isUnsignedInteger(type) &&
            ((!descr.fill_from.isNull() && less(descr.fill_from, Field{0}, 1)) ||
             (!descr.fill_to.isNull() && less(descr.fill_to, Field{0}, 1))))
        {
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "WITH FILL bound values cannot be negative for unsigned type {}", type->getName());
        }
    }

    std::set<size_t> unique_positions;
    for (auto pos : fill_column_positions)
        if (!unique_positions.insert(pos).second)
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION, "Multiple WITH FILL for identical expressions is not supported in ORDER BY");

    size_t idx = 0;
    for (const ColumnWithTypeAndName & column : header_.getColumnsWithTypeAndName())
    {
        if (interpolate_description)
            if (const auto & p = interpolate_description->required_columns_map.find(column.name);
                p != interpolate_description->required_columns_map.end())
                    input_positions.emplace_back(idx, p->second);

        if (!is_fill_column[idx] && !(interpolate_description && interpolate_description->result_columns_set.contains(column.name)))
            other_column_positions.push_back(idx);

        ++idx;
    }

    if (interpolate_description)
        for (const auto & name : interpolate_description->result_columns_order)
            interpolate_column_positions.push_back(header_.getPositionByName(name));
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

static void insertFromFillingRow(const MutableColumnRawPtrs & filling_columns, const MutableColumnRawPtrs & interpolate_columns, const MutableColumnRawPtrs & other_columns,
    const FillingRow & filling_row, const Block & interpolate_block)
{
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
}

static void copyRowFromColumns(const MutableColumnRawPtrs & dest, const Columns & source, size_t row_num)
{
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
    for (size_t pos : positions)
    {
        input_columns_by_positions.push_back(input_columns[pos]);
        output_columns_by_position.push_back(output_columns[pos].get());
    }
}

void FillingTransform::initColumns(
    const Columns & input_columns,
    Columns & input_fill_columns,
    Columns & input_interpolate_columns,
    Columns & input_other_columns,
    MutableColumns & output_columns,
    MutableColumnRawPtrs & output_fill_columns,
    MutableColumnRawPtrs & output_interpolate_columns,
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
    initColumnsByPositions(non_const_columns, input_other_columns, output_columns, output_other_columns, other_column_positions);
}

bool FillingTransform::generateSuffixIfNeeded(const Columns & input_columns, MutableColumns & result_columns)
{
    logDebug("generateSuffixIfNeeded() filling_row", filling_row);
    logDebug("generateSuffixIfNeeded() next_row", next_row);
    logDebug("generateSuffixIfNeeded() first", first);

    /// Determines should we insert filling row before start generating next rows.
    bool should_insert_first = next_row < filling_row || first;

    for (size_t i = 0, size = filling_row.size(); i < size; ++i)
        next_row[i] = filling_row.getFillDescription(i).fill_to;

    logDebug("generateSuffixIfNeeded() next_row updated", next_row);

    if (!first && filling_row >= next_row)
    {
        logDebug("generateSuffixIfNeeded()", "no need to generate suffix");
        return false;
    }

    Columns input_fill_columns;
    Columns input_interpolate_columns;
    Columns input_other_columns;
    MutableColumnRawPtrs res_fill_columns;
    MutableColumnRawPtrs res_interpolate_columns;
    MutableColumnRawPtrs res_other_columns;

    initColumns(
        input_columns,
        input_fill_columns,
        input_interpolate_columns,
        input_other_columns,
        result_columns,
        res_fill_columns,
        res_interpolate_columns,
        res_other_columns);

    if (first)
        filling_row.initFromDefaults();

    Block interpolate_block;
    if (should_insert_first && filling_row < next_row)
    {
        interpolate(result_columns, interpolate_block);
        insertFromFillingRow(res_fill_columns, res_interpolate_columns, res_other_columns, filling_row, interpolate_block);
    }

    while (filling_row.next(next_row))
    {
        interpolate(result_columns, interpolate_block);
        insertFromFillingRow(res_fill_columns, res_interpolate_columns, res_other_columns, filling_row, interpolate_block);
    }

    return true;
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
    Columns input_other_columns;
    MutableColumnRawPtrs res_fill_columns;
    MutableColumnRawPtrs res_interpolate_columns;
    MutableColumnRawPtrs res_other_columns;
    MutableColumns result_columns;

    Block interpolate_block;

    if (all_chunks_processed)
    {
        chassert(!chunk.hasRows());

        /// if all chunks are processed, then we may need to generate suffix for the following cases:
        /// (1) when all data are processed and WITH FILL .. TO is provided
        /// (2) for empty result set when WITH FILL FROM .. TO is provided (see PR #30888)
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
        input_other_columns,
        result_columns,
        res_fill_columns,
        res_interpolate_columns,
        res_other_columns);

    if (first)
    {
        for (size_t i = 0, size = filling_row.size(); i < size; ++i)
        {
            auto current_value = (*input_fill_columns[i])[0];
            const auto & fill_from = filling_row.getFillDescription(i).fill_from;

            if (!fill_from.isNull() && !equals(current_value, fill_from))
            {
                filling_row.initFromDefaults(i);
                if (less(fill_from, current_value, filling_row.getDirection(i)))
                {
                    interpolate(result_columns, interpolate_block);
                    insertFromFillingRow(res_fill_columns, res_interpolate_columns, res_other_columns, filling_row, interpolate_block);
                }
                break;
            }
            filling_row[i] = current_value;
        }
        first = false;
    }

    for (size_t row_ind = 0; row_ind < num_rows; ++row_ind)
    {
        logDebug("row", row_ind);
        logDebug("filling_row", filling_row);
        logDebug("next_row", next_row);

        bool should_insert_first = next_row < filling_row;
        logDebug("should_insert_first", should_insert_first);

        for (size_t i = 0, size = filling_row.size(); i < size; ++i)
        {
            auto current_value = (*input_fill_columns[i])[row_ind];
            const auto & fill_to = filling_row.getFillDescription(i).fill_to;

            if (fill_to.isNull() || less(current_value, fill_to, filling_row.getDirection(i)))
                next_row[i] = current_value;
            else
                next_row[i] = fill_to;
        }
        logDebug("next_row updated", next_row);

        /// A case, when at previous step row was initialized from defaults 'fill_from' values
        ///  and probably we need to insert it to block.
        if (should_insert_first && filling_row < next_row)
        {
            interpolate(result_columns, interpolate_block);
            insertFromFillingRow(res_fill_columns, res_interpolate_columns, res_other_columns, filling_row, interpolate_block);
        }

        while (filling_row.next(next_row))
        {
            interpolate(result_columns, interpolate_block);
            insertFromFillingRow(res_fill_columns, res_interpolate_columns, res_other_columns, filling_row, interpolate_block);
        }

        copyRowFromColumns(res_fill_columns, input_fill_columns, row_ind);
        copyRowFromColumns(res_interpolate_columns, input_interpolate_columns, row_ind);
        copyRowFromColumns(res_other_columns, input_other_columns, row_ind);
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
