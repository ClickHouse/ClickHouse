#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>
#include <Common/Volnitsky.h>
#include <Common/hex.h>


/** Functions for working with JSON stored in String fields:
 *
 * jsonAny: retrieve the scalar value of the first match of property path as String
 * jsonAnyArray: retrieve the array value of the first match of property path as Array(String)
 * jsonAll: retrieve scalar values of all mathes of property path as Array(String)
 * jsonAllArrays: retrieve array values of all matches of property path as Array(Array(String))
 * jsonCount: count the number of occurences for all matches of property path and return as UInt64
 *
 */

namespace DB
{
using Pos = const unsigned char *;
static constexpr size_t bytes_on_stack = 64;
using ExpectChars = PODArray<char, bytes_on_stack, AllocatorWithStackMemory<Allocator<false>, bytes_on_stack>>;

template <typename ResultColumn>
class BaseResultContainer
{
protected:
    ColumnWithTypeAndName & result;
    ColumnString::Offset current_row_offset;

    BaseResultContainer(ColumnWithTypeAndName & result_) : result(result_), current_row_offset(0) {}

    virtual ~BaseResultContainer() {}

    ResultColumn * column() { return typeid_cast<ResultColumn *>((result.column->assumeMutable()).get()); }

    /// Only used in ResultContainers returning strings
    ColumnString & columnString() { return typeid_cast<ColumnString &>(typeid_cast<ColumnArray *>(column())->getData()); }

    /// Only used in ResultContainers returning Array(String)
    void insertArray(Pos begin, Pos last)
    {
        ExpectChars expects_end;
        UInt8 current_expect_end = 0;
        Pos pos;

        for (pos = begin; pos <= last; ++pos)
        {
            if (*pos == current_expect_end)
            {
                expects_end.pop_back();
                current_expect_end = expects_end.empty() ? 0 : expects_end.back();
            }
            else
            {
                switch (*pos)
                {
                    case '[':
                        current_expect_end = ']';
                        expects_end.push_back(current_expect_end);
                        break;
                    case '{':
                        current_expect_end = '}';
                        expects_end.push_back(current_expect_end);
                        break;
                    case '"':
                        current_expect_end = '"';
                        expects_end.push_back(current_expect_end);
                        break;
                    case '\\':
                        /// skip backslash
                        if (pos + 1 < last && pos[1] == '"')
                            ++pos;
                        break;
                    default:
                        if (!current_expect_end && *pos == ',')
                        {
                            insertElement(begin, pos - 1);
                            begin = pos + 1;
                        }
                }
            }
        }

        if (pos > begin)
        {
            insertElement(begin, pos - 1);
        }
    }

    /// We have to define insertElement here for the insertArray, even though not any ResultContainer is using it
    virtual void insertElement(Pos begin, Pos last) = 0;

    /* We do not define the public API of ResiltContainer classes here to avoid making the corresponding
     * functions virtual and therefore losing in performance. See documentation of all public methods in the child
     * classes.
     *
     */
};


/// Implementation of jsonAny: retrieve the scalar value of the first match of property path as String
class AnyResultContainer : BaseResultContainer<ColumnString>
{
protected:
    ColumnString::Chars & stringChars() { return column()->getChars(); }
    ColumnString::Offsets & stringOffsets() { return column()->getOffsets(); }

    void insertElement(Pos payload_begin, Pos payload_last) override
    {
        stringChars().insert(payload_begin, payload_last + 1);
        current_row_offset += stringChars().size();
    }

public:
    static constexpr auto name = "jsonAny";

    AnyResultContainer(ColumnWithTypeAndName & result_) : BaseResultContainer(result_) { result.column = ColumnString::create(); }

    /// If isAny returns true, the extract method will be called at most one time per input row. Otherwise, it will be
    /// called for each match of the json path.
    bool isAny() { return true; }

    /// finishRow is called exactly once per input row
    void finishRow()
    {
        stringChars().push_back(0);
        ++current_row_offset;
        stringOffsets().push_back(current_row_offset);
    }

    /// extract is called exactly once for each match, or exactly once only for the first match, depending on isAny().
    void extract(Pos payload_begin, Pos payload_last) { insertElement(payload_begin, payload_last); }

    static DataTypePtr getReturnType() { return std::make_shared<DataTypeString>(); }
};

/// Implementation of jsonCount: count the number of occurences for all matches of property path and return as UInt64
class CountResultContainer : BaseResultContainer<ColumnUInt64>
{
protected:
    UInt64 count;

    void insertElement(Pos /*payload_begin*/, Pos /*payload_last*/)
    {
        /// Unused here
    }

public:
    static constexpr auto name = "jsonCount";

    CountResultContainer(ColumnWithTypeAndName & result_) : BaseResultContainer(result_), count(0)
    {
        result.column = ColumnUInt64::create();
    }

    /// If isAny returns true, the extract method will be called at most one time per input row. Otherwise, it will be
    /// called for each match of the json path.
    bool isAny() { return false; }

    /// finishRow is called exactly once per input row
    void finishRow()
    {
        column()->insertValue(count);
        count = 0;
    }

    /// extract is called exactly once for each match, or exactly once only for the first match, depending on isAny().
    void extract(Pos /*payload_begin*/, Pos /*payload_last*/) { ++count; }

    static DataTypePtr getReturnType() { return std::make_shared<DataTypeUInt64>(); }
};


/// Implementation of jsonAll: retrieve scalar values of all mathes of property path as Array(String)
class AllResultContainer : BaseResultContainer<ColumnArray>
{
protected:
    ColumnArray::Offsets & arrayOffsets() { return column()->getOffsets(); }
    ColumnString::Chars & stringChars() { return columnString().getChars(); }
    ColumnString::Offsets & stringOffsets() { return columnString().getOffsets(); }

    void insertElement(Pos payload_begin, Pos payload_last) override
    {
        stringChars().insert(payload_begin, payload_last + 1);
        stringChars().push_back(0);

        stringOffsets().push_back(stringChars().size());
        ++current_row_offset;
    }

public:
    static constexpr auto name = "jsonAll";

    AllResultContainer(ColumnWithTypeAndName & result_) : BaseResultContainer(result_)
    {
        result.column = ColumnArray::create(ColumnString::create());
    }

    /// If isAny returns true, the extract method will be called at most one time per input row. Otherwise, it will be
    /// called for each match of the json path.
    bool isAny() { return false; }

    /// finishRow is called exactly once per input row
    void finishRow() { arrayOffsets().push_back(current_row_offset); }

    /// extract is called exactly once for each match, or exactly once only for the first match, depending on isAny().
    void extract(Pos payload_begin, Pos payload_last) { insertElement(payload_begin, payload_last); }

    static DataTypePtr getReturnType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()); }
};

/// Implementation of jsonAnyArray: retrieve the array value of the first match of property path as Array(String)
class AnyArrayResultContainer : BaseResultContainer<ColumnArray>
{
protected:
    ColumnArray::Offsets & arrayOffsets() { return column()->getOffsets(); }
    ColumnString::Chars & stringChars() { return columnString().getChars(); }
    ColumnString::Offsets & stringOffsets() { return columnString().getOffsets(); }

    void insertElement(Pos payload_begin, Pos payload_last) override
    {
        stringChars().insert(payload_begin, payload_last + 1);
        stringChars().push_back(0);

        stringOffsets().push_back(stringChars().size());
        ++current_row_offset;
    }

public:
    static constexpr auto name = "jsonAnyArray";

    AnyArrayResultContainer(ColumnWithTypeAndName & result_) : BaseResultContainer(result_)
    {
        result.column = ColumnArray::create(ColumnString::create());
    }

    /// If isAny returns true, the extract method will be called at most one time per input row. Otherwise, it will be
    /// called for each match of the json path.
    bool isAny() { return true; }

    /// finishRow is called exactly once per input row
    void finishRow() { arrayOffsets().push_back(current_row_offset); }

    /// extract is called exactly once for each match, or exactly once only for the first match, depending on isAny().
    void extract(Pos payload_begin, Pos payload_last)
    {
        while (' ' == *payload_begin && payload_begin <= payload_last)
            ++payload_begin;

        while (' ' == *payload_last && payload_begin < payload_last)
            --payload_last;

        if ('[' == *payload_begin && ']' == *payload_last)
        {
            insertArray(payload_begin + 1, payload_last - 1);
        }
        else
        {
            insertElement(payload_begin, payload_last);
        }
    }

    static DataTypePtr getReturnType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()); }
};

/// Implementation of jsonAllArrays: retrieve array values of all matches of property path as Array(Array(String))
class AllArraysResultContainer : BaseResultContainer<ColumnArray>
{
protected:
    ColumnArray::Offset current_array_offset;

    ColumnArray::Offsets & outerArrayOffsets() { return column()->getOffsets(); }

    ColumnArray & innerArrayColumn() { return typeid_cast<ColumnArray &>(column()->getData()); }
    ColumnArray::Offsets & innerArrayOffsets() { return innerArrayColumn().getOffsets(); }

    ColumnString::Chars & stringChars() { return typeid_cast<ColumnString &>(innerArrayColumn().getData()).getChars(); }
    ColumnString::Offsets & stringOffsets() { return typeid_cast<ColumnString &>(innerArrayColumn().getData()).getOffsets(); }

    void insertElement(Pos payload_begin, Pos payload_last)
    {
        stringChars().insert(payload_begin, payload_last + 1);
        stringChars().push_back(0);

        stringOffsets().push_back(stringChars().size());
        ++current_array_offset;
    }

public:
    static constexpr auto name = "jsonAllArrays";

    AllArraysResultContainer(ColumnWithTypeAndName & result_) : BaseResultContainer(result_), current_array_offset(0)
    {
        result.column = ColumnArray::create(ColumnArray::create(ColumnString::create()));
    }

    /// If isAny returns true, the extract method will be called at most one time per input row. Otherwise, it will be
    /// called for each match of the json path.
    bool isAny() { return false; }

    /// finishRow is called exactly once per input row
    void finishRow() { outerArrayOffsets().push_back(current_row_offset); }

    /// extract is called exactly once for each match, or exactly once only for the first match, depending on isAny().
    void extract(Pos payload_begin, Pos payload_last)
    {
        auto old_array_offset = current_array_offset;

        while (' ' == *payload_begin && payload_begin <= payload_last)
            ++payload_begin;

        while (' ' == *payload_last && payload_begin < payload_last)
            --payload_last;

        if ('[' == *payload_begin && ']' == *payload_last)
        {
            insertArray(payload_begin + 1, payload_last - 1);
        }
        else
        {
            insertElement(payload_begin, payload_last);
        }
        innerArrayOffsets().push_back(current_array_offset);
        if (current_array_offset > old_array_offset)
            ++current_row_offset;
    }

    static DataTypePtr getReturnType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    }
};


/// Common implementation parts of all json functions
template <typename ResultContainer>
class FunctionJson : public IFunction
{
public:
    static constexpr auto name = ResultContainer::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJson>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isStringOrFixedString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return ResultContainer::getReturnType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        size_t json_arg = arguments[0];
        size_t path_arg = arguments[1];

        if (!block.getByPosition(path_arg).column->isColumnConst())
            throw Exception(getName() + " currently supports only constant expressions as second argument", ErrorCodes::ILLEGAL_COLUMN);

        const ColumnConst * needle_str = checkAndGetColumnConstStringOrFixedString(block.getByPosition(path_arg).column.get());
        String needle = needle_str->getValue<String>();

        const auto maybe_const = block.getByPosition(json_arg).column.get()->convertToFullColumnIfConst();
        const ColumnString * json_str = checkAndGetColumn<ColumnString>(maybe_const.get());

        if (json_str)
        {
            const ColumnString::Chars & json_chars = json_str->getChars();
            const ColumnString::Offsets & json_offsets = json_str->getOffsets();

            ResultContainer resultContainer(block.getByPosition(result));

            std::size_t dot_position = needle.find(".");
            const Pos data_begin = json_chars.data();

            /// The original visitParam* functions search across the rows, which should lead to better performance, if not every row
            /// contains the needle. We have to search in each row, because we want to support json paths and want to avoid complexity
            /// arizing from tracking parts of the path across different rows.
            for (size_t row_index = 0; row_index < json_offsets.size(); ++row_index)
            {
                /// -2, because the offset points to the beginning of the next row, and the last byte of the previous row is always \0
                extractJsonRecursive(data_begin, data_begin + json_offsets[row_index] - 2, needle, dot_position, 0, resultContainer);
                resultContainer.finishRow();
            }
        }
        else
            throw Exception(
                "Illegal columns " + block.getByPosition(0).column->getName() + ", " + block.getByPosition(1).column->getName()
                    + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    void extractJsonRecursive(
        Pos begin, Pos last, const std::string & jsonPath, size_t dot_position, size_t previous_dot_position, ResultContainer & resultContainer)
    {
        std::string jsonKey;

        if (std::string::npos == dot_position)
            jsonKey = "\"" + jsonPath.substr(previous_dot_position, std::string::npos) + "\":";
        else
            jsonKey = "\"" + jsonPath.substr(previous_dot_position, dot_position - previous_dot_position) + "\":";

        Pos pos = begin;

        Volnitsky searcher(jsonKey.data(), jsonKey.size(), begin - last + 1);

        /// quickly find the beginning of the next part of the json key with Volnitsky searcher
        while (pos <= last && last > (pos = searcher.search(pos, last - pos + 1)))
        {
            Pos payload_begin = nullptr;
            Pos payload_last = nullptr;
            /// now pos + jsonKey.size() points to the beginning of the json value. Slowly scan the json to
            /// find where this value ends (i.e. we're looking for the next "," or "}"
            pos = findBeginLast(pos + jsonKey.size(), last + 1, payload_begin, payload_last);

            /// If valid value is present
            if (payload_begin && payload_last)
            {
                if (std::string::npos == dot_position) /// no other key parts to search
                {
                    resultContainer.extract(payload_begin, payload_last);

                    /// Ensure that extract is called only once per input row, if isAny is true
                    if (resultContainer.isAny())
                        return;
                }
                else
                {
                    extractJsonRecursive(
                        payload_begin, payload_last, jsonPath, jsonPath.find(".", dot_position + 1), dot_position + 1, resultContainer);
                }
            }
        }
    }

    /// Slowly scan the json to find where the json value ends (i.e. we're looking for the next "," or "}"
    Pos findBeginLast(Pos pos, Pos end, Pos & payload_begin, Pos & payload_last)
    {
        ExpectChars expects_end;
        UInt8 current_expect_end = 0;

        for (auto extract_begin = pos; pos <= end; ++pos)
        {
            if (*pos == current_expect_end)
            {
                expects_end.pop_back();
                current_expect_end = expects_end.empty() ? 0 : expects_end.back();
            }
            else
            {
                switch (*pos)
                {
                    case '[':
                        current_expect_end = ']';
                        expects_end.push_back(current_expect_end);
                        break;
                    case '{':
                        current_expect_end = '}';
                        expects_end.push_back(current_expect_end);
                        break;
                    case '"':
                        current_expect_end = '"';
                        expects_end.push_back(current_expect_end);
                        break;
                    case '\\':
                        /// skip backslash
                        if (pos + 1 < end && pos[1] == '"')
                            ++pos;
                        break;
                    default:
                        if (!current_expect_end && (*pos == ',' || *pos == '}'))
                        {
                            payload_begin = extract_begin;
                            payload_last = pos - 1;
                            while (payload_begin < payload_last && *payload_begin == ' ')
                                ++payload_begin;
                            while (payload_last > payload_begin && *payload_last == ' ')
                                --payload_last;
                            if (payload_begin < payload_last && *payload_begin == '"' && *payload_last == '"')
                            {
                                ++payload_begin;
                                --payload_last;
                            }

                            return pos;
                        }
                }
            }
        }
        return pos;
    }
};


}
