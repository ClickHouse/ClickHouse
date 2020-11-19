#include <Common/Exception.h>
#include <Common/FieldVisitors.h>

#include <Core/Block.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Common/assert_cast.h>

#include <Columns/ColumnConst.h>

#include <iterator>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int POSITION_OUT_OF_BOUND;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


Block::Block(std::initializer_list<ColumnWithTypeAndName> il) : data{il}
{
    initializeIndexByName();
}


Block::Block(const ColumnsWithTypeAndName & data_) : data{data_}
{
    initializeIndexByName();
}


void Block::initializeIndexByName()
{
    for (size_t i = 0, size = data.size(); i < size; ++i)
        index_by_name.emplace(data[i].name, i);
}


void Block::insert(size_t position, const ColumnWithTypeAndName & elem)
{
    if (position > data.size())
        throw Exception("Position out of bound in Block::insert(), max position = "
            + toString(data.size()), ErrorCodes::POSITION_OUT_OF_BOUND);

    for (auto & name_pos : index_by_name)
        if (name_pos.second >= position)
            ++name_pos.second;

    index_by_name.emplace(elem.name, position);
    data.emplace(data.begin() + position, elem);
}

void Block::insert(size_t position, ColumnWithTypeAndName && elem)
{
    if (position > data.size())
        throw Exception("Position out of bound in Block::insert(), max position = "
        + toString(data.size()), ErrorCodes::POSITION_OUT_OF_BOUND);

    for (auto & name_pos : index_by_name)
        if (name_pos.second >= position)
            ++name_pos.second;

    index_by_name.emplace(elem.name, position);
    data.emplace(data.begin() + position, std::move(elem));
}


void Block::insert(const ColumnWithTypeAndName & elem)
{
    index_by_name.emplace(elem.name, data.size());
    data.emplace_back(elem);
}

void Block::insert(ColumnWithTypeAndName && elem)
{
    index_by_name.emplace(elem.name, data.size());
    data.emplace_back(std::move(elem));
}


void Block::insertUnique(const ColumnWithTypeAndName & elem)
{
    if (index_by_name.end() == index_by_name.find(elem.name))
        insert(elem);
}

void Block::insertUnique(ColumnWithTypeAndName && elem)
{
    if (index_by_name.end() == index_by_name.find(elem.name))
        insert(std::move(elem));
}


void Block::erase(const std::set<size_t> & positions)
{
    for (auto it = positions.rbegin(); it != positions.rend(); ++it)
        erase(*it);
}


void Block::erase(size_t position)
{
    if (data.empty())
        throw Exception("Block is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= data.size())
        throw Exception("Position out of bound in Block::erase(), max position = "
            + toString(data.size() - 1), ErrorCodes::POSITION_OUT_OF_BOUND);

    eraseImpl(position);
}


void Block::eraseImpl(size_t position)
{
    data.erase(data.begin() + position);

    for (auto it = index_by_name.begin(); it != index_by_name.end();)
    {
        if (it->second == position)
            index_by_name.erase(it++);
        else
        {
            if (it->second > position)
                --it->second;
            ++it;
        }
    }
}


void Block::erase(const String & name)
{
    auto index_it = index_by_name.find(name);
    if (index_it == index_by_name.end())
        throw Exception("No such name in Block::erase(): '"
            + name + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    eraseImpl(index_it->second);
}


ColumnWithTypeAndName & Block::safeGetByPosition(size_t position)
{
    if (data.empty())
        throw Exception("Block is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= data.size())
        throw Exception("Position " + toString(position)
            + " is out of bound in Block::safeGetByPosition(), max position = "
            + toString(data.size() - 1)
            + ", there are columns: " + dumpNames(), ErrorCodes::POSITION_OUT_OF_BOUND);

    return data[position];
}


const ColumnWithTypeAndName & Block::safeGetByPosition(size_t position) const
{
    if (data.empty())
        throw Exception("Block is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= data.size())
        throw Exception("Position " + toString(position)
            + " is out of bound in Block::safeGetByPosition(), max position = "
            + toString(data.size() - 1)
            + ", there are columns: " + dumpNames(), ErrorCodes::POSITION_OUT_OF_BOUND);

    return data[position];
}


const ColumnWithTypeAndName * Block::findByName(const std::string & name) const
{
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it)
    {
        return nullptr;
    }
    return &data[it->second];
}


const ColumnWithTypeAndName & Block::getByName(const std::string & name) const
{
    const auto * result = findByName(name);
    if (!result)
        throw Exception("Not found column " + name + " in block. There are only columns: " + dumpNames()
            , ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    return *result;
}


bool Block::has(const std::string & name) const
{
    return index_by_name.end() != index_by_name.find(name);
}


size_t Block::getPositionByName(const std::string & name) const
{
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it)
        throw Exception("Not found column " + name + " in block. There are only columns: " + dumpNames()
            , ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    return it->second;
}


void Block::checkNumberOfRows(bool allow_null_columns) const
{
    ssize_t rows = -1;
    for (const auto & elem : data)
    {
        if (!elem.column && allow_null_columns)
            continue;

        if (!elem.column)
            throw Exception("Column " + elem.name + " in block is nullptr, in method checkNumberOfRows."
                , ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        ssize_t size = elem.column->size();

        if (rows == -1)
            rows = size;
        else if (rows != size)
            throw Exception("Sizes of columns doesn't match: "
                + data.front().name + ": " + toString(rows)
                + ", " + elem.name + ": " + toString(size)
                , ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    }
}


size_t Block::rows() const
{
    for (const auto & elem : data)
        if (elem.column)
            return elem.column->size();

    return 0;
}


size_t Block::bytes() const
{
    size_t res = 0;
    for (const auto & elem : data)
        res += elem.column->byteSize();

    return res;
}

size_t Block::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & elem : data)
        res += elem.column->allocatedBytes();

    return res;
}

std::string Block::dumpNames() const
{
    WriteBufferFromOwnString out;
    for (auto it = data.begin(); it != data.end(); ++it)
    {
        if (it != data.begin())
            out << ", ";
        out << it->name;
    }
    return out.str();
}


std::string Block::dumpStructure() const
{
    WriteBufferFromOwnString out;
    for (auto it = data.begin(); it != data.end(); ++it)
    {
        if (it != data.begin())
            out << ", ";
        it->dumpStructure(out);
    }
    return out.str();
}

std::string Block::dumpIndex() const
{
    WriteBufferFromOwnString out;
    bool first = true;
    for (const auto & [name, pos] : index_by_name)
    {
        if (!first)
            out << ", ";
        first = false;

        out << name << ' ' << pos;
    }
    return out.str();
}

Block Block::cloneEmpty() const
{
    Block res;

    for (const auto & elem : data)
        res.insert(elem.cloneEmpty());

    return res;
}


MutableColumns Block::cloneEmptyColumns() const
{
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = data[i].column ? data[i].column->cloneEmpty() : data[i].type->createColumn();
    return columns;
}


Columns Block::getColumns() const
{
    size_t num_columns = data.size();
    Columns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = data[i].column;
    return columns;
}


MutableColumns Block::mutateColumns()
{
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = data[i].column ? IColumn::mutate(std::move(data[i].column)) : data[i].type->createColumn();
    return columns;
}


void Block::setColumns(MutableColumns && columns)
{
    /// TODO: assert if |columns| doesn't match |data|!
    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i)
        data[i].column = std::move(columns[i]);
}


void Block::setColumns(const Columns & columns)
{
    /// TODO: assert if |columns| doesn't match |data|!
    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i)
        data[i].column = columns[i];
}


Block Block::cloneWithColumns(MutableColumns && columns) const
{
    Block res;

    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i)
        res.insert({ std::move(columns[i]), data[i].type, data[i].name });

    return res;
}


Block Block::cloneWithColumns(const Columns & columns) const
{
    Block res;

    size_t num_columns = data.size();

    if (num_columns != columns.size())
        throw Exception("Cannot clone block with columns because block has " + toString(num_columns) + " columns, "
                        "but " + toString(columns.size()) + " columns given.", ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < num_columns; ++i)
        res.insert({ columns[i], data[i].type, data[i].name });

    return res;
}


Block Block::cloneWithoutColumns() const
{
    Block res;

    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i)
        res.insert({ nullptr, data[i].type, data[i].name });

    return res;
}


Block Block::sortColumns() const
{
    Block sorted_block;

    /// std::unordered_map (index_by_name) cannot be used to guarantee the sort order
    std::vector<decltype(index_by_name.begin())> sorted_index_by_name(index_by_name.size());
    {
        size_t i = 0;
        for (auto it = index_by_name.begin(); it != index_by_name.end(); ++it)
            sorted_index_by_name[i++] = it;
    }
    std::sort(sorted_index_by_name.begin(), sorted_index_by_name.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs->first < rhs->first;
    });

    for (const auto & it : sorted_index_by_name)
        sorted_block.insert(data[it->second]);

    return sorted_block;
}


const ColumnsWithTypeAndName & Block::getColumnsWithTypeAndName() const
{
    return data;
}


NamesAndTypesList Block::getNamesAndTypesList() const
{
    NamesAndTypesList res;

    for (const auto & elem : data)
        res.emplace_back(elem.name, elem.type);

    return res;
}


Names Block::getNames() const
{
    Names res;
    res.reserve(columns());

    for (const auto & elem : data)
        res.push_back(elem.name);

    return res;
}


DataTypes Block::getDataTypes() const
{
    DataTypes res;
    res.reserve(columns());

    for (const auto & elem : data)
        res.push_back(elem.type);

    return res;
}


template <typename ReturnType>
static ReturnType checkBlockStructure(const Block & lhs, const Block & rhs, const std::string & context_description)
{
    auto on_error = [](const std::string & message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (std::is_same_v<ReturnType, void>)
            throw Exception(message, code);
        else
            return false;
    };

    size_t columns = rhs.columns();
    if (lhs.columns() != columns)
        return on_error("Block structure mismatch in " + context_description + " stream: different number of columns:\n"
            + lhs.dumpStructure() + "\n" + rhs.dumpStructure(), ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & expected = rhs.getByPosition(i);
        const auto & actual = lhs.getByPosition(i);

        if (actual.name != expected.name)
            return on_error("Block structure mismatch in " + context_description + " stream: different names of columns:\n"
                + lhs.dumpStructure() + "\n" + rhs.dumpStructure(), ErrorCodes::LOGICAL_ERROR);

        if (!actual.type->equals(*expected.type))
            return on_error("Block structure mismatch in " + context_description + " stream: different types:\n"
                + lhs.dumpStructure() + "\n" + rhs.dumpStructure(), ErrorCodes::LOGICAL_ERROR);

        if (!actual.column || !expected.column)
            continue;

        if (actual.column->getName() != expected.column->getName())
            return on_error("Block structure mismatch in " + context_description + " stream: different columns:\n"
                + lhs.dumpStructure() + "\n" + rhs.dumpStructure(), ErrorCodes::LOGICAL_ERROR);

        if (isColumnConst(*actual.column) && isColumnConst(*expected.column))
        {
            Field actual_value = assert_cast<const ColumnConst &>(*actual.column).getField();
            Field expected_value = assert_cast<const ColumnConst &>(*expected.column).getField();

            if (actual_value != expected_value)
                return on_error("Block structure mismatch in " + context_description + " stream: different values of constants, actual: "
                    + applyVisitor(FieldVisitorToString(), actual_value) + ", expected: " + applyVisitor(FieldVisitorToString(), expected_value),
                    ErrorCodes::LOGICAL_ERROR);
        }
    }

    return ReturnType(true);
}


bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs)
{
    return checkBlockStructure<bool>(lhs, rhs, {});
}


void assertBlocksHaveEqualStructure(const Block & lhs, const Block & rhs, const std::string & context_description)
{
    checkBlockStructure<void>(lhs, rhs, context_description);
}


void getBlocksDifference(const Block & lhs, const Block & rhs, std::string & out_lhs_diff, std::string & out_rhs_diff)
{
    /// The traditional task: the largest common subsequence (LCS).
    /// Assume that order is important. If this becomes wrong once, let's simplify it: for example, make 2 sets.

    std::vector<std::vector<int>> lcs(lhs.columns() + 1);
    for (auto & v : lcs)
        v.resize(rhs.columns() + 1);

    for (size_t i = 1; i <= lhs.columns(); ++i)
    {
        for (size_t j = 1; j <= rhs.columns(); ++j)
        {
            if (lhs.safeGetByPosition(i - 1) == rhs.safeGetByPosition(j - 1))
                lcs[i][j] = lcs[i - 1][j - 1] + 1;
            else
                lcs[i][j] = std::max(lcs[i - 1][j], lcs[i][j - 1]);
        }
    }

    /// Now go back and collect the answer.
    ColumnsWithTypeAndName left_columns;
    ColumnsWithTypeAndName right_columns;
    size_t l = lhs.columns();
    size_t r = rhs.columns();
    while (l > 0 && r > 0)
    {
        if (lhs.safeGetByPosition(l - 1) == rhs.safeGetByPosition(r - 1))
        {
            /// This element is in both sequences, so it does not get into `diff`.
            --l;
            --r;
        }
        else
        {
            /// Small heuristics: most often used when getting a difference for (expected_block, actual_block).
            /// Therefore, the preference will be given to the field, which is in the left block (expected_block), therefore
            /// in `diff` the column from `actual_block` will get.
            if (lcs[l][r - 1] >= lcs[l - 1][r])
                right_columns.push_back(rhs.safeGetByPosition(--r));
            else
                left_columns.push_back(lhs.safeGetByPosition(--l));
        }
    }

    while (l > 0)
        left_columns.push_back(lhs.safeGetByPosition(--l));
    while (r > 0)
        right_columns.push_back(rhs.safeGetByPosition(--r));

    WriteBufferFromString lhs_diff_writer(out_lhs_diff);
    WriteBufferFromString rhs_diff_writer(out_rhs_diff);

    for (auto it = left_columns.rbegin(); it != left_columns.rend(); ++it)
    {
        lhs_diff_writer << it->dumpStructure();
        lhs_diff_writer << ", position: " << lhs.getPositionByName(it->name) << '\n';
    }
    for (auto it = right_columns.rbegin(); it != right_columns.rend(); ++it)
    {
        rhs_diff_writer << it->dumpStructure();
        rhs_diff_writer << ", position: " << rhs.getPositionByName(it->name) << '\n';
    }
}


void Block::clear()
{
    info = BlockInfo();
    data.clear();
    index_by_name.clear();
}

void Block::swap(Block & other) noexcept
{
    std::swap(info, other.info);
    data.swap(other.data);
    index_by_name.swap(other.index_by_name);
}


void Block::updateHash(SipHash & hash) const
{
    for (size_t row_no = 0, num_rows = rows(); row_no < num_rows; ++row_no)
        for (const auto & col : data)
            col.column->updateHashWithValue(row_no, hash);
}

}
