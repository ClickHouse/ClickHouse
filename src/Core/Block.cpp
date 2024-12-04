#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Core/Block.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <base/sort.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
#include <Common/assert_cast.h>

#include <iterator>

#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int POSITION_OUT_OF_BOUND;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int AMBIGUOUS_COLUMN_NAME;
}

template <typename ReturnType, typename... FmtArgs>
static ReturnType onError(int code [[maybe_unused]],
                          FormatStringHelper<FmtArgs...> fmt_string [[maybe_unused]],
                          FmtArgs && ...fmt_args [[maybe_unused]])
{
    if constexpr (std::is_same_v<ReturnType, void>)
        throw Exception(code, std::move(fmt_string), std::forward<FmtArgs>(fmt_args)...);
    else
        return false;
}


template <typename ReturnType>
static ReturnType checkColumnStructure(const ColumnWithTypeAndName & actual, const ColumnWithTypeAndName & expected,
    std::string_view context_description, bool allow_materialize, int code)
{
    if (actual.name != expected.name)
        return onError<ReturnType>(code, "Block structure mismatch in {} stream: different names of columns:\n{}\n{}",
                                   context_description, actual.dumpStructure(), expected.dumpStructure());

    if ((actual.type && !expected.type) || (!actual.type && expected.type)
        || (actual.type && expected.type && !actual.type->equals(*expected.type)))
        return onError<ReturnType>(code, "Block structure mismatch in {} stream: different types:\n{}\n{}",
                                   context_description, actual.dumpStructure(), expected.dumpStructure());

    if (!actual.column || !expected.column)
        return ReturnType(true);

    const IColumn * actual_column = actual.column.get();

    /// If we allow to materialize, and expected column is not const or sparse, then unwrap actual column.
    if (allow_materialize)
    {
        if (!isColumnConst(*expected.column))
            if (const auto * column_const = typeid_cast<const ColumnConst *>(actual_column))
                actual_column = &column_const->getDataColumn();

        if (!expected.column->isSparse())
            if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(actual_column))
                actual_column = &column_sparse->getValuesColumn();
    }

    const auto * actual_column_maybe_agg = typeid_cast<const ColumnAggregateFunction *>(actual_column);
    const auto * expected_column_maybe_agg = typeid_cast<const ColumnAggregateFunction *>(expected.column.get());
    if (actual_column_maybe_agg && expected_column_maybe_agg)
    {
        if (!actual_column_maybe_agg->getAggregateFunction()->haveSameStateRepresentation(*expected_column_maybe_agg->getAggregateFunction()))
            return onError<ReturnType>(code,
                    "Block structure mismatch in {} stream: different columns:\n{}\n{}",
                    context_description,
                    actual.dumpStructure(),
                    expected.dumpStructure());
    }
    else if (actual_column->getName() != expected.column->getName())
        return onError<ReturnType>(code,
                "Block structure mismatch in {} stream: different columns:\n{}\n{}",
                context_description,
                actual.dumpStructure(),
                expected.dumpStructure());

    if (isColumnConst(*actual.column) && isColumnConst(*expected.column)
        && !actual.column->empty() && !expected.column->empty()) /// don't check values in empty columns
    {
        Field actual_value = assert_cast<const ColumnConst &>(*actual.column).getField();
        Field expected_value = assert_cast<const ColumnConst &>(*expected.column).getField();

        if (actual_value != expected_value)
            return onError<ReturnType>(code,
                    "Block structure mismatch in {} stream: different values of constants in column '{}': actual: {}, expected: {}",
                    context_description,
                    actual.name,
                    applyVisitor(FieldVisitorToString(), actual_value),
                    applyVisitor(FieldVisitorToString(), expected_value));
    }

    return ReturnType(true);
}


template <typename ReturnType>
static ReturnType checkBlockStructure(const Block & lhs, const Block & rhs, std::string_view context_description, bool allow_materialize)
{
    size_t columns = rhs.columns();
    if (lhs.columns() != columns)
        return onError<ReturnType>(ErrorCodes::LOGICAL_ERROR, "Block structure mismatch in {} stream: different number of columns:\n{}\n{}",
                                   context_description, lhs.dumpStructure(), rhs.dumpStructure());

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & actual = lhs.getByPosition(i);
        const auto & expected = rhs.getByPosition(i);

        if constexpr (std::is_same_v<ReturnType, bool>)
        {
            if (!checkColumnStructure<ReturnType>(actual, expected, context_description, allow_materialize, ErrorCodes::LOGICAL_ERROR))
                return false;
        }
        else
            checkColumnStructure<ReturnType>(actual, expected, context_description, allow_materialize, ErrorCodes::LOGICAL_ERROR);
    }

    return ReturnType(true);
}


Block::Block(std::initializer_list<ColumnWithTypeAndName> il) : data{il}
{
    initializeIndexByName();
}


Block::Block(const ColumnsWithTypeAndName & data_) : data{data_}
{
    initializeIndexByName();
}

Block::Block(ColumnsWithTypeAndName && data_) : data{std::move(data_)}
{
    initializeIndexByName();
}


void Block::initializeIndexByName()
{
    for (size_t i = 0, size = data.size(); i < size; ++i)
        index_by_name.emplace(data[i].name, i);
}


void Block::reserve(size_t count)
{
    index_by_name.reserve(count);
    data.reserve(count);
}


void Block::insert(size_t position, ColumnWithTypeAndName elem)
{
    if (position > data.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Position out of bound in Block::insert(), max position = {}",
        data.size());

    if (elem.name.empty())
        throw Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME, "Column name in Block cannot be empty");

    auto [new_it, inserted] = index_by_name.emplace(elem.name, position);
    if (!inserted)
        checkColumnStructure<void>(data[new_it->second], elem,
            "(columns with identical name must have identical structure)", true, ErrorCodes::AMBIGUOUS_COLUMN_NAME);

    for (auto it = index_by_name.begin(); it != index_by_name.end(); ++it)
    {
        if (it->second >= position && (!inserted || it != new_it))
            ++it->second;
    }

    data.emplace(data.begin() + position, std::move(elem));
}


void Block::insert(ColumnWithTypeAndName elem)
{
    if (elem.name.empty())
        throw Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME, "Column name in Block cannot be empty");

    auto [it, inserted] = index_by_name.emplace(elem.name, data.size());
    if (!inserted)
        checkColumnStructure<void>(data[it->second], elem,
            "(columns with identical name must have identical structure)", true, ErrorCodes::AMBIGUOUS_COLUMN_NAME);

    data.emplace_back(std::move(elem));
}


void Block::insertUnique(ColumnWithTypeAndName elem)
{
    if (elem.name.empty())
        throw Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME, "Column name in Block cannot be empty");

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
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Block is empty");

    if (position >= data.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Position out of bound in Block::erase(), max position = {}",
            data.size() - 1);

    eraseImpl(position);
}


void Block::eraseImpl(size_t position)
{
    data.erase(data.begin() + position);

    for (auto it = index_by_name.begin(); it != index_by_name.end();)
    {
        if (it->second == position)
            it = index_by_name.erase(it);
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
        throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "No such name in Block::erase(): '{}'", name);

    eraseImpl(index_it->second);
}


ColumnWithTypeAndName & Block::safeGetByPosition(size_t position)
{
    if (data.empty())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Block is empty");

    if (position >= data.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Position {} is out of bound in Block::safeGetByPosition(), "
                        "max position = {}, there are columns: {}", toString(position), toString(data.size() - 1), dumpNames());

    return data[position];
}


const ColumnWithTypeAndName & Block::safeGetByPosition(size_t position) const
{
    if (data.empty())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Block is empty");

    if (position >= data.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Position {} is out of bound in Block::safeGetByPosition(), "
                        "max position = {}, there are columns: {}", toString(position), toString(data.size() - 1), dumpNames());

    return data[position];
}


const ColumnWithTypeAndName * Block::findByName(const std::string & name, bool case_insensitive) const
{
    if (case_insensitive)
    {
        auto found = std::find_if(data.begin(), data.end(), [&](const auto & column) { return boost::iequals(column.name, name); });
        if (found == data.end())
        {
            return nullptr;
        }
        return &*found;
    }

    auto it = index_by_name.find(name);
    if (index_by_name.end() == it)
    {
        return nullptr;
    }
    return &data[it->second];
}


const ColumnWithTypeAndName & Block::getByName(const std::string & name, bool case_insensitive) const
{
    const auto * result = findByName(name, case_insensitive);
    if (!result)
        throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Not found column {} in block. There are only columns: {}",
            name, dumpNames());

    return *result;
}


bool Block::has(const std::string & name, bool case_insensitive) const
{
    if (case_insensitive)
        return std::find_if(data.begin(), data.end(), [&](const auto & column) { return boost::iequals(column.name, name); })
            != data.end();

    return index_by_name.end() != index_by_name.find(name);
}


size_t Block::getPositionByName(const std::string & name) const
{
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it)
        throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Not found column {} in block. There are only columns: {}",
            name, dumpNames());

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
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Column {} in block is nullptr, in method checkNumberOfRows." , elem.name);

        ssize_t size = elem.column->size();

        if (rows == -1)
            rows = size;
        else if (rows != size)
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Sizes of columns doesn't match: {}: {}, {}: {}",
                data.front().name, rows, elem.name, toString(size));
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
    res.reserve(data.size());

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


void Block::setColumn(size_t position, ColumnWithTypeAndName column)
{
    if (position >= data.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Position {} out of bound in Block::setColumn(), max position {}",
                        position, data.size());

    if (data[position].name != column.name)
    {
        index_by_name.erase(data[position].name);
        index_by_name.emplace(column.name, position);
    }

    data[position] = std::move(column);
}


Block Block::cloneWithColumns(MutableColumns && columns) const
{
    Block res;

    size_t num_columns = data.size();

    if (num_columns != columns.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot clone block with columns because block has {} columns, but {} columns given",
            num_columns, columns.size());
    }

    res.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
        res.insert({ std::move(columns[i]), data[i].type, data[i].name });

    return res;
}


Block Block::cloneWithColumns(const Columns & columns) const
{
    Block res;

    size_t num_columns = data.size();

    if (num_columns != columns.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot clone block with columns because block has {} columns, but {} columns given",
            num_columns, columns.size());
    }

    res.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
        res.insert({ columns[i], data[i].type, data[i].name });

    return res;
}


Block Block::cloneWithoutColumns() const
{
    Block res;

    size_t num_columns = data.size();
    res.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
        res.insert({ nullptr, data[i].type, data[i].name });

    return res;
}

Block Block::cloneWithCutColumns(size_t start, size_t length) const
{
    Block copy = *this;

    for (auto & column_to_cut : copy.data)
        column_to_cut.column = column_to_cut.column->cut(start, length);

    return copy;
}

Block Block::sortColumns() const
{
    Block sorted_block;

    /// std::unordered_map (index_by_name) cannot be used to guarantee the sort order
    std::vector<IndexByName::const_iterator> sorted_index_by_name(index_by_name.size());
    {
        size_t i = 0;
        for (auto it = index_by_name.begin(); it != index_by_name.end(); ++it)
            sorted_index_by_name[i++] = it;
    }
    ::sort(sorted_index_by_name.begin(), sorted_index_by_name.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs->first < rhs->first;
    });

    for (const auto & it : sorted_index_by_name)
        sorted_block.insert(data[it->second]);

    return sorted_block;
}

Block Block::shrinkToFit() const
{
    Columns new_columns(data.size(), nullptr);
    for (size_t i = 0; i < data.size(); ++i)
        new_columns[i] = data[i].column->cloneResized(data[i].column->size());
    return cloneWithColumns(new_columns);
}

Block Block::compress() const
{
    size_t num_columns = data.size();
    Columns new_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        new_columns[i] = data[i].column->compress(/*force_compression=*/false);
    return cloneWithColumns(new_columns);
}

Block Block::decompress() const
{
    size_t num_columns = data.size();
    Columns new_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        new_columns[i] = data[i].column->decompress();
    return cloneWithColumns(new_columns);
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

NamesAndTypes Block::getNamesAndTypes() const
{
    NamesAndTypes res;
    res.reserve(columns());

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


Names Block::getDataTypeNames() const
{
    Names res;
    res.reserve(columns());

    for (const auto & elem : data)
        res.push_back(elem.type->getName());

    return res;
}


Block::NameMap Block::getNamesToIndexesMap() const
{
    NameMap res(index_by_name.size());
    res.set_empty_key(StringRef{});
    for (const auto & [name, index] : index_by_name)
        res[name] = index;
    return res;
}


bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs)
{
    return checkBlockStructure<bool>(lhs, rhs, "", false);
}


void assertBlocksHaveEqualStructure(const Block & lhs, const Block & rhs, std::string_view context_description)
{
    checkBlockStructure<void>(lhs, rhs, context_description, false);
}


bool isCompatibleHeader(const Block & actual, const Block & desired)
{
    return checkBlockStructure<bool>(actual, desired, "", true);
}


void assertCompatibleHeader(const Block & actual, const Block & desired, std::string_view context_description)
{
    checkBlockStructure<void>(actual, desired, context_description, true);
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

    {
        WriteBufferFromString lhs_diff_writer(out_lhs_diff);
        for (auto it = left_columns.rbegin(); it != left_columns.rend(); ++it)
        {
            lhs_diff_writer << it->dumpStructure();
            lhs_diff_writer << ", position: " << lhs.getPositionByName(it->name) << '\n';
        }
    }

    {
        WriteBufferFromString rhs_diff_writer(out_rhs_diff);
        for (auto it = right_columns.rbegin(); it != right_columns.rend(); ++it)
        {
            rhs_diff_writer << it->dumpStructure();
            rhs_diff_writer << ", position: " << rhs.getPositionByName(it->name) << '\n';
        }
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

Serializations Block::getSerializations() const
{
    Serializations res;
    res.reserve(data.size());

    for (const auto & column : data)
        res.push_back(column.type->getDefaultSerialization());

    return res;
}

Serializations Block::getSerializations(const SerializationInfoByName & hints) const
{
    Serializations res;
    res.reserve(data.size());

    for (const auto & column : data)
    {
        auto it = hints.find(column.name);
        if (it == hints.end())
            res.push_back(column.type->getDefaultSerialization());
        else
            res.push_back(column.type->getSerialization(*it->second));
    }

    return res;
}

void convertToFullIfSparse(Block & block)
{
    for (auto & column : block)
        column.column = recursiveRemoveSparse(column.column);
}

Block materializeBlock(const Block & block)
{
    if (!block)
        return block;

    Block res = block;
    size_t columns = res.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        auto & element = res.getByPosition(i);
        element.column = recursiveRemoveSparse(element.column->convertToFullColumnIfConst());
    }

    return res;
}

void materializeBlockInplace(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
        block.getByPosition(i).column = recursiveRemoveSparse(block.getByPosition(i).column->convertToFullColumnIfConst());
}

Block concatenateBlocks(const std::vector<Block> & blocks)
{
    if (blocks.empty())
        return {};

    size_t num_rows = 0;
    for (const auto & block : blocks)
        num_rows += block.rows();

    Block out = blocks[0].cloneEmpty();
    MutableColumns columns = out.mutateColumns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns[i]->reserve(num_rows);
        for (const auto & block : blocks)
        {
            const auto & tmp_column = *block.getByPosition(i).column;
            columns[i]->insertRangeFrom(tmp_column, 0, block.rows());
        }
    }

    out.setColumns(std::move(columns));
    return out;
}

}
