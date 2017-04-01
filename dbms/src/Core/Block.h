#pragma once

#include <vector>
#include <map>
#include <initializer_list>

#include <Common/Exception.h>
#include <Core/BlockInfo.h>
#include <Core/NamesAndTypes.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/ColumnNumbers.h>
#include <Common/Exception.h>



namespace DB
{

/** Container for set of columns for bunch of rows in memory.
  * This is unit of data processing.
  * Also contains metadata - data types of columns and their names
  *  (either original names from a table, or generated names during temporary calculations).
  * Allows to insert, remove columns in arbitary position, to change order of columns.
  */

class Context;

class Block
{
private:
    using Container = ColumnsWithTypeAndName;
    using IndexByName = std::map<String, size_t>;

    Container data;
    IndexByName index_by_name;

public:
    BlockInfo info;

    Block() = default;
    Block(std::initializer_list<ColumnWithTypeAndName> il);
    Block(const ColumnsWithTypeAndName & data_);

    /// вставить столбец в заданную позицию
    void insert(size_t position, const ColumnWithTypeAndName & elem);
    void insert(size_t position, ColumnWithTypeAndName && elem);
    /// вставить столбец в конец
    void insert(const ColumnWithTypeAndName & elem);
    void insert(ColumnWithTypeAndName && elem);
    /// вставить столбец в конец, если столбца с таким именем ещё нет
    void insertUnique(const ColumnWithTypeAndName & elem);
    void insertUnique(ColumnWithTypeAndName && elem);
    /// удалить столбец в заданной позиции
    void erase(size_t position);
    /// удалить столбец с заданным именем
    void erase(const String & name);
    /// Добавляет в блок недостающие столбцы со значениями по-умолчанию
    void addDefaults(const NamesAndTypesList & required_columns);

    /// References are invalidated after calling functions above.

    ColumnWithTypeAndName & getByPosition(size_t position) { return data[position]; }
    const ColumnWithTypeAndName & getByPosition(size_t position) const { return data[position]; }

    ColumnWithTypeAndName & safeGetByPosition(size_t position);
    const ColumnWithTypeAndName & safeGetByPosition(size_t position) const;

    ColumnWithTypeAndName & getByName(const std::string & name);
    const ColumnWithTypeAndName & getByName(const std::string & name) const;

    bool has(const std::string & name) const;

    size_t getPositionByName(const std::string & name) const;

    ColumnsWithTypeAndName getColumns() const;
    NamesAndTypesList getColumnsList() const;

    /// Returns number of rows from first column in block, not equal to nullptr. If no columns, returns 0.
    size_t rows() const;

    size_t columns() const { return data.size(); }

    /// Checks that every column in block is not nullptr and has same number of elements.
    void checkNumberOfRows() const;

    /// Approximate number of bytes in memory - for profiling and limits.
    size_t bytes() const;

    operator bool() const { return !data.empty(); }
    bool operator!() const { return data.empty(); }

    /** Получить список имён столбцов через запятую. */
    std::string dumpNames() const;

    /** Список имен, типов и длин столбцов. Предназначен для отладки. */
    std::string dumpStructure() const;

    /** Получить такой же блок, но пустой. */
    Block cloneEmpty() const;

    /** Получить блок со столбцами, переставленными в порядке их имён. */
    Block sortColumns() const;

    /** Заменяет столбцы смещений внутри вложенных таблиц на один общий для таблицы.
     *  Кидает исключение, если эти смещения вдруг оказались неодинаковы.
     */
    void optimizeNestedArraysOffsets();
    /** Тоже самое, только без замены смещений. */
    void checkNestedArraysOffsets() const;

    void clear();
    void swap(Block & other) noexcept;

    /** Some column implementations (ColumnArray) may have shared parts between different columns
      * (common array sizes of elements of nested data structures).
      * Before doing mutating operations on such columns, you must unshare that parts.
      * Also unsharing columns, if whole columns are shared_ptrs pointing to same instances.
      */
    void unshareColumns();

private:
    void eraseImpl(size_t position);
    void initializeIndexByName();
};

using Blocks = std::vector<Block>;
using BlocksList = std::list<Block>;


/// Сравнить типы столбцов у блоков. Порядок столбцов имеет значение. Имена не имеют значения.
bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs);

/// Calculate difference in structure of blocks and write description into output strings.
void getBlocksDifference(const Block & lhs, const Block & rhs, std::string & out_lhs_diff, std::string & out_rhs_diff);


/** Дополнительные данные к блокам. Они пока нужны только для запроса
  * DESCRIBE TABLE с Distributed-таблицами.
  */
struct BlockExtraInfo
{
    BlockExtraInfo() {}
    operator bool() const { return is_valid; }
    bool operator!() const { return !is_valid; }

    std::string host;
    std::string resolved_address;
    std::string user;
    UInt16 port = 0;

    bool is_valid = false;
};

}
