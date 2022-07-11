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


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_WITH_FILL_EXPRESSION;
}

Block FillingTransform::transformHeader(Block header, const SortDescription & sort_description)
{
    NameSet sort_keys;
    for (const auto & key : sort_description)
        sort_keys.insert(key.column_name);

    /// Columns which are not from sorting key may not be constant anymore.
    for (auto & column : header)
        if (column.column && isColumnConst(*column.column) && !sort_keys.count(column.name))
            column.column = column.type->createColumn();

    return header;
}

template <typename T>
static FillColumnDescription::StepFunction getStepFunction(
    IntervalKind kind, Int64 step, const DateLUTImpl & date_lut)
{
    switch (kind)
    {
        #define DECLARE_CASE(NAME) \
        case IntervalKind::NAME: \
            return [step, &date_lut](Field & field) { field = Add##NAME##sImpl::execute(get<T>(field), step, date_lut); };

        FOR_EACH_INTERVAL_KIND(DECLARE_CASE)
        #undef DECLARE_CASE
    }
    __builtin_unreachable();
}

static bool tryConvertFields(FillColumnDescription & descr, const DataTypePtr & type)
{
    auto max_type = Field::Types::Null;
    WhichDataType which(type);
    DataTypePtr to_type;

    /// TODO Wrong results for big integers.
    if (isInteger(type) || which.isDate() || which.isDate32() || which.isDateTime())
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

    descr.fill_from = convertFieldToType(descr.fill_from, *to_type);
    descr.fill_to = convertFieldToType(descr.fill_to, *to_type);
    descr.fill_step = convertFieldToType(descr.fill_step, *to_type);

    if (descr.step_kind)
    {
        if (which.isDate() || which.isDate32())
        {
            Int64 avg_seconds = get<Int64>(descr.fill_step) * descr.step_kind->toAvgSeconds();
            if (avg_seconds < 86400)
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "Value of step is to low ({} seconds). Must be >= 1 day", avg_seconds);
        }

        if (which.isDate())
            descr.step_func = getStepFunction<UInt16>(*descr.step_kind, get<Int64>(descr.fill_step), DateLUT::instance());
        else if (which.isDate32())
            descr.step_func = getStepFunction<Int32>(*descr.step_kind, get<Int64>(descr.fill_step), DateLUT::instance());
        else if (const auto * date_time = checkAndGetDataType<DataTypeDateTime>(type.get()))
            descr.step_func = getStepFunction<UInt32>(*descr.step_kind, get<Int64>(descr.fill_step), date_time->getTimeZone());
        else if (const auto * date_time64 = checkAndGetDataType<DataTypeDateTime64>(type.get()))
        {
            const auto & step_dec = get<const DecimalField<Decimal64> &>(descr.fill_step);
            Int64 step = DecimalUtils::convertTo<Int64>(step_dec.getValue(), step_dec.getScale());

            switch (*descr.step_kind)
            {
                #define DECLARE_CASE(NAME) \
                case IntervalKind::NAME: \
                    descr.step_func = [step, &time_zone = date_time64->getTimeZone()](Field & field) \
                    { \
                        auto field_decimal = get<DecimalField<DateTime64>>(field); \
                        auto components = DecimalUtils::splitWithScaleMultiplier(field_decimal.getValue(), field_decimal.getScaleMultiplier()); \
                        auto res = Add##NAME##sImpl::execute(components, step, time_zone); \
                        auto res_decimal = decimalFromComponentsWithMultiplier<DateTime64>(res, field_decimal.getScaleMultiplier()); \
                        field = DecimalField(res_decimal, field_decimal.getScale()); \
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
        const Block & header_, const SortDescription & sort_description_, bool on_totals_)
        : ISimpleTransform(header_, transformHeader(header_, sort_description_), true)
        , sort_description(sort_description_)
        , on_totals(on_totals_)
        , filling_row(sort_description_)
        , next_row(sort_description_)
{
    if (on_totals)
        return;

    std::vector<bool> is_fill_column(header_.columns());
    for (size_t i = 0, size = sort_description.size(); i < size; ++i)
    {
        size_t block_position = header_.getPositionByName(sort_description[i].column_name);
        is_fill_column[block_position] = true;
        fill_column_positions.push_back(block_position);

        auto & descr = filling_row.getFillDescription(i);
        const auto & type = header_.getByPosition(block_position).type;

        if (!tryConvertFields(descr, type))
            throw Exception("Incompatible types of WITH FILL expression values with column type "
                + type->getName(), ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

        if (type->isValueRepresentedByUnsignedInteger() &&
            ((!descr.fill_from.isNull() && less(descr.fill_from, Field{0}, 1)) ||
                (!descr.fill_to.isNull() && less(descr.fill_to, Field{0}, 1))))
        {
            throw Exception("WITH FILL bound values cannot be negative for unsigned type "
                + type->getName(), ErrorCodes::INVALID_WITH_FILL_EXPRESSION);
        }
    }

    std::set<size_t> unique_positions;
    for (auto pos : fill_column_positions)
        if (!unique_positions.insert(pos).second)
            throw Exception("Multiple WITH FILL for identical expressions is not supported in ORDER BY", ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

    for (size_t i = 0; i < header_.columns(); ++i)
        if (!is_fill_column[i])
            other_column_positions.push_back(i);
}

IProcessor::Status FillingTransform::prepare()
{
    if (!on_totals && input.isFinished() && !output.isFinished() && !has_input && !generate_suffix)
    {
        should_insert_first = next_row < filling_row || first;

        for (size_t i = 0, size = filling_row.size(); i < size; ++i)
            next_row[i] = filling_row.getFillDescription(i).fill_to;

        if (first || filling_row < next_row)
        {
            generate_suffix = true;
            return Status::Ready;
        }
    }

    return ISimpleTransform::prepare();
}


void FillingTransform::transform(Chunk & chunk)
{
    if (on_totals)
        return;

    Columns old_fill_columns;
    Columns old_other_columns;
    MutableColumns res_fill_columns;
    MutableColumns res_other_columns;

    auto init_columns_by_positions = [](const Columns & old_columns, Columns & new_columns,
        MutableColumns & new_mutable_columns, const Positions & positions)
    {
        for (size_t pos : positions)
        {
            auto old_column = old_columns[pos]->convertToFullColumnIfConst();
            new_columns.push_back(old_column);
            new_mutable_columns.push_back(old_column->cloneEmpty()->assumeMutable());
        }
    };

    if (generate_suffix)
    {
        const auto & empty_columns = input.getHeader().getColumns();
        init_columns_by_positions(empty_columns, old_fill_columns, res_fill_columns, fill_column_positions);
        init_columns_by_positions(empty_columns, old_other_columns, res_other_columns, other_column_positions);

        if (first)
            filling_row.initFromDefaults();

        if (should_insert_first && filling_row < next_row)
            insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);

        while (filling_row.next(next_row))
            insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);

        setResultColumns(chunk, res_fill_columns, res_other_columns);
        return;
    }

    size_t num_rows = chunk.getNumRows();
    auto old_columns = chunk.detachColumns();

    init_columns_by_positions(old_columns, old_fill_columns, res_fill_columns, fill_column_positions);
    init_columns_by_positions(old_columns, old_other_columns, res_other_columns, other_column_positions);

    if (first)
    {
        for (size_t i = 0; i < filling_row.size(); ++i)
        {
            auto current_value = (*old_fill_columns[i])[0];
            const auto & fill_from = filling_row.getFillDescription(i).fill_from;

            if (!fill_from.isNull() && !equals(current_value, fill_from))
            {
                filling_row.initFromDefaults(i);
                if (less(fill_from, current_value, filling_row.getDirection(i)))
                    insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);
                break;
            }
            filling_row[i] = current_value;
        }
        first = false;
    }

    for (size_t row_ind = 0; row_ind < num_rows; ++row_ind)
    {
        should_insert_first = next_row < filling_row;

        for (size_t i = 0; i < filling_row.size(); ++i)
        {
            auto current_value = (*old_fill_columns[i])[row_ind];
            const auto & fill_to = filling_row.getFillDescription(i).fill_to;

            if (fill_to.isNull() || less(current_value, fill_to, filling_row.getDirection(i)))
                next_row[i] = current_value;
            else
                next_row[i] = fill_to;
        }

        /// A case, when at previous step row was initialized from defaults 'fill_from' values
        ///  and probably we need to insert it to block.
        if (should_insert_first && filling_row < next_row)
            insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);

        /// Insert generated filling row to block, while it is less than current row in block.
        while (filling_row.next(next_row))
            insertFromFillingRow(res_fill_columns, res_other_columns, filling_row);

        copyRowFromColumns(res_fill_columns, old_fill_columns, row_ind);
        copyRowFromColumns(res_other_columns, old_other_columns, row_ind);
    }

    setResultColumns(chunk, res_fill_columns, res_other_columns);
}

void FillingTransform::setResultColumns(Chunk & chunk, MutableColumns & fill_columns, MutableColumns & other_columns) const
{
    MutableColumns result_columns(fill_columns.size() + other_columns.size());
    /// fill_columns always non-empty.
    size_t num_rows = fill_columns[0]->size();

    for (size_t i = 0, size = fill_columns.size(); i < size; ++i)
        result_columns[fill_column_positions[i]] = std::move(fill_columns[i]);
    for (size_t i = 0, size = other_columns.size(); i < size; ++i)
        result_columns[other_column_positions[i]] = std::move(other_columns[i]);

    chunk.setColumns(std::move(result_columns), num_rows);
}

}
