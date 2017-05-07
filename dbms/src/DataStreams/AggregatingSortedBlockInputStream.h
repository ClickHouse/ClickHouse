#pragma once

#include <common/logger_useful.h>

#include <Core/Row.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>


namespace DB
{

/** Соединяет несколько сортированных потоков в один.
  * При этом, для каждой группы идущих подряд одинаковых значений первичного ключа (столбцов, по которым сортируются данные),
  * сливает их в одну строку. При слиянии, производится доагрегация данных - слияние состояний агрегатных функций,
  * соответствующих одному значению первичного ключа. Для столбцов, не входящих в первичный ключ, и не имеющих тип AggregateFunction,
  * при слиянии, выбирается первое попавшееся значение.
  */
class AggregatingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    AggregatingSortedBlockInputStream(BlockInputStreams inputs_, const SortDescription & description_, size_t max_block_size_)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_)
    {
    }

    String getName() const override { return "AggregatingSorted"; }

    String getID() const override
    {
        std::stringstream res;
        res << "AggregatingSorted(inputs";

        for (size_t i = 0; i < children.size(); ++i)
            res << ", " << children[i]->getID();

        res << ", description";

        for (size_t i = 0; i < description.size(); ++i)
            res << ", " << description[i].getID();

        res << ")";
        return res.str();
    }

    bool isGroupedOutput() const override { return true; }
    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

protected:
    /// Может возвращаться на 1 больше записей, чем max_block_size.
    Block readImpl() override;

private:
    Logger * log = &Logger::get("AggregatingSortedBlockInputStream");

    /// Прочитали до конца.
    bool finished = false;

    /// Столбцы с какими номерами надо аггрегировать.
    ColumnNumbers column_numbers_to_aggregate;
    ColumnNumbers column_numbers_not_to_aggregate;
    std::vector<ColumnAggregateFunction *> columns_to_aggregate;

    RowRef current_key;        /// Текущий первичный ключ.
    RowRef next_key;        /// Первичный ключ следующей строки.

    /** Делаем поддержку двух разных курсоров - с Collation и без.
     *  Шаблоны используем вместо полиморфных SortCursor'ов и вызовов виртуальных функций.
     */
    template <class TSortCursor>
    void merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

    /** Извлечь все состояния аггрегатных функций и объединить с текущей группой.
      */
    template <class TSortCursor>
    void addRow(TSortCursor & cursor);
};

}
