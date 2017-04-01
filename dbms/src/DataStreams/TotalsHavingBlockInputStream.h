#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

class ExpressionActions;


/** Принимает блоки после группировки, с нефиализированными агрегатными функциями.
  * Вычисляет тотальные значения в соответствии с totals_mode.
  * Если нужно, вычисляет выражение из HAVING и фильтрует им строки. Отдает финализированные и отфильтрованные блоки.
  */
class TotalsHavingBlockInputStream : public IProfilingBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
    TotalsHavingBlockInputStream(
        BlockInputStreamPtr input_,
        bool overflow_row_, ExpressionActionsPtr expression_,
        const std::string & filter_column_, TotalsMode totals_mode_, double auto_include_threshold_);

    String getName() const override { return "TotalsHaving"; }

    String getID() const override;

    const Block & getTotals() override;

protected:
    Block readImpl() override;

private:
    bool overflow_row;
    ExpressionActionsPtr expression;
    String filter_column_name;
    TotalsMode totals_mode;
    double auto_include_threshold;
    size_t passed_keys = 0;
    size_t total_keys = 0;

    /** Здесь находятся значения, не прошедшие max_rows_to_group_by.
      * Они прибавляются или не прибавляются к current_totals в зависимости от totals_mode.
      */
    Block overflow_aggregates;

    /// Здесь накапливаются тотальные значения. После окончания работы, они будут помещены в IProfilingBlockInputStream::totals.
    Block current_totals;

    /// Если filter == nullptr - прибавлять все строки. Иначе - только строки, проходящие фильтр (HAVING).
    void addToTotals(Block & totals, Block & block, const IColumn::Filter * filter);
};

}
