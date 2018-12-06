#include <Common/typeid_cast.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnConst.h>
#include <Columns/FilterDescription.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
}


AddingDefaultsBlockInputStream::AddingDefaultsBlockInputStream(const BlockInputStreamPtr & input,
                                                               const ColumnDefaults & column_defaults_,
                                                               const Context & context_)
    : column_defaults(column_defaults_),
      context(context_)
{
    children.push_back(input);
    header = input->getHeader();
}


Block AddingDefaultsBlockInputStream::readImpl()
{
    Block res = children.back()->read();
    if (!res)
        return res;

    if (column_defaults.empty())
        return res;

    const BlockMissingValues & block_missing_values = children.back()->getMissingValues();
    if (block_missing_values.empty())
        return res;

    Block evaluate_block{res};
    /// remove columns for recalculation
    for (const auto & column : column_defaults)
        if (evaluate_block.has(column.first))
            evaluate_block.erase(column.first);

    evaluateMissingDefaults(evaluate_block, header.getNamesAndTypesList(), column_defaults, context, false);

    std::unordered_map<size_t, MutableColumnPtr> mixed_columns;

    for (const ColumnWithTypeAndName & column_def : evaluate_block)
    {
        const String & column_name = column_def.name;

        if (column_defaults.count(column_name) == 0)
            continue;

        size_t block_column_position = res.getPositionByName(column_name);
        ColumnWithTypeAndName & column_read = res.getByPosition(block_column_position);
        const auto & defaults_mask = block_missing_values.getDefaultsBitmask(block_column_position);

        checkCalculated(column_read, column_def, defaults_mask.size());

        if (!defaults_mask.empty())
        {
            /// TODO: FixedString
            if (isColumnedAsNumber(column_read.type) || isDecimal(column_read.type))
            {
                MutableColumnPtr column_mixed = (*std::move(column_read.column)).mutate();
                mixNumberColumns(column_read.type->getTypeId(), column_mixed, column_def.column, defaults_mask);
                column_read.column = std::move(column_mixed);
            }
            else
            {
                MutableColumnPtr column_mixed = mixColumns(column_read, column_def, defaults_mask);
                mixed_columns.emplace(block_column_position, std::move(column_mixed));
            }
        }
    }

    if (!mixed_columns.empty())
    {
        /// replace columns saving block structure
        MutableColumns mutation = res.mutateColumns();
        for (size_t position = 0; position < mutation.size(); ++position)
        {
            auto it = mixed_columns.find(position);
            if (it != mixed_columns.end())
                mutation[position] = std::move(it->second);
        }
        res.setColumns(std::move(mutation));
    }

    return res;
}

void AddingDefaultsBlockInputStream::checkCalculated(const ColumnWithTypeAndName & col_read,
                                                     const ColumnWithTypeAndName & col_defaults,
                                                     size_t defaults_needed) const
{
    size_t column_size = col_read.column->size();

    if (column_size != col_defaults.column->size())
        throw Exception("Mismatch column sizes while adding defaults", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (column_size < defaults_needed)
        throw Exception("Unexpected defaults count", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (!col_read.type->equals(*col_defaults.type))
        throw Exception("Mismach column types while adding defaults", ErrorCodes::TYPE_MISMATCH);
}

void AddingDefaultsBlockInputStream::mixNumberColumns(TypeIndex type_idx, MutableColumnPtr & column_mixed, const ColumnPtr & column_defs,
                                                      const BlockMissingValues::RowsBitMask & defaults_mask) const
{
    auto call = [&](const auto & types) -> bool
    {
        using Types = std::decay_t<decltype(types)>;
        using DataType = typename Types::LeftType;

        if constexpr (!std::is_same_v<DataType, DataTypeString> && !std::is_same_v<DataType, DataTypeFixedString>)
        {
            using FieldType = typename DataType::FieldType;
            using ColVecType = std::conditional_t<IsDecimalNumber<FieldType>, ColumnDecimal<FieldType>, ColumnVector<FieldType>>;

            auto col_read = typeid_cast<ColVecType *>(column_mixed.get());
            if (!col_read)
                return false;

            typename ColVecType::Container & dst = col_read->getData();

            if (auto const_col_defs = checkAndGetColumnConst<ColVecType>(column_defs.get()))
            {
                FieldType value = checkAndGetColumn<ColVecType>(const_col_defs->getDataColumnPtr().get())->getData()[0];

                for (size_t i = 0; i < defaults_mask.size(); ++i)
                    if (defaults_mask[i])
                        dst[i] = value;

                return true;
            }
            else if (auto col_defs = checkAndGetColumn<ColVecType>(column_defs.get()))
            {
                auto & src = col_defs->getData();
                for (size_t i = 0; i < defaults_mask.size(); ++i)
                    if (defaults_mask[i])
                        dst[i] = src[i];

                return true;
            }
        }

        return false;
    };

    if (!callOnIndexAndDataType<void>(type_idx, call))
        throw Exception("Unexpected type on mixNumberColumns", ErrorCodes::LOGICAL_ERROR);
}

MutableColumnPtr AddingDefaultsBlockInputStream::mixColumns(const ColumnWithTypeAndName & col_read,
                                                            const ColumnWithTypeAndName & col_defaults,
                                                            const BlockMissingValues::RowsBitMask & defaults_mask) const
{
    size_t column_size = col_read.column->size();
    size_t defaults_needed = defaults_mask.size();

    MutableColumnPtr column_mixed = col_read.column->cloneEmpty();

    for (size_t i = 0; i < defaults_needed; ++i)
    {
        if (defaults_mask[i])
        {
            if (col_defaults.column->isColumnConst())
                column_mixed->insert((*col_defaults.column)[i]);
            else
                column_mixed->insertFrom(*col_defaults.column, i);
        }
        else
            column_mixed->insertFrom(*col_read.column, i);
    }

    for (size_t i = defaults_needed; i < column_size; ++i)
        column_mixed->insertFrom(*col_read.column, i);

    return column_mixed;
}

}
