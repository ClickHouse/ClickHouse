#include <Functions/Conditional/StringArrayEvaluator.h>
#include <Functions/Conditional/CondSource.h>
#include <Functions/Conditional/common.h>
#include <Functions/Conditional/NullMapBuilder.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Common/typeid_cast.h>


/// NOTE: this code is quite complicated and ugly because it handles
/// the internals of arrays of strings.
/// Arrays of fixed strings are currently unsupported.

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace Conditional
{

namespace
{

enum class ChunkType : UInt8
{
    VARIABLE = 0,
    CONSTANT
};

using VarCallback = std::function<size_t(ColumnString::Chars_t & to_data,
    ColumnString::Offset_t &, ColumnString::Offsets_t &)>;

/// The name of this structure is a bit of a misnomer, but it makes
/// the code slightly simpler to read.
struct Chunk
{
    explicit Chunk(const VarCallback & var_callback_)
        : var_callback{var_callback_}
    {
    }

    VarCallback var_callback;
};

/// This class provides access to the values of a string array branch
/// (then, else) column.
class StringArraySource
{
public:
    virtual ~StringArraySource() {}
    virtual ChunkType getType() const = 0;
    virtual Chunk get() const = 0;
    virtual void next() = 0;
    virtual size_t getDataSize() const = 0;
    virtual size_t getStringOffsetsSize() const = 0;
    virtual size_t getIndex() const = 0;
};

using StringArraySourcePtr = std::unique_ptr<StringArraySource>;
using StringArraySources = std::vector<StringArraySourcePtr>;


/// Implementation of StringArraySource specific to arrays of variable strings.
class VarStringArraySource : public StringArraySource
{
public:
    VarStringArraySource(const ColumnString::Chars_t & data_,
        const ColumnString::Offsets_t & string_offsets_,
        const ColumnArray::Offsets_t & array_offsets_,
        size_t index_)
        : data(data_), string_offsets(string_offsets_), array_offsets(array_offsets_),
        index(index_)
    {
    }

    ChunkType getType() const override
    {
        return ChunkType::VARIABLE;
    }

    Chunk get() const override
    {
        return Chunk{var_callback};
    }

    void next() override
    {
        array_prev_offset = array_offsets[i];
        if (array_prev_offset)
            string_prev_offset = string_offsets[array_prev_offset - 1];
        ++i;
    }

    size_t getDataSize() const override
    {
        return data.size();
    }

    size_t getStringOffsetsSize() const override
    {
        return string_offsets.size();
    }

    size_t getIndex() const override
    {
        return index;
    }

private:
    const ColumnString::Chars_t & data;
    const ColumnString::Offsets_t & string_offsets;
    const ColumnArray::Offsets_t & array_offsets;

    ColumnArray::Offset_t array_prev_offset = 0;
    ColumnString::Offset_t string_prev_offset = 0;
    size_t index = 0;
    size_t i = 0;

    VarCallback var_callback = [&](ColumnString::Chars_t & to_data,
        ColumnString::Offset_t & to_string_prev_offset,
        ColumnString::Offsets_t & to_string_offsets) ALWAYS_INLINE
    {
        size_t array_size = array_offsets[i] - array_prev_offset;
        size_t bytes_to_copy = 0;
        size_t string_prev_offset_local = string_prev_offset;

        for (size_t j = 0; j < array_size; ++j)
        {
            size_t string_size = string_offsets[array_prev_offset + j] - string_prev_offset_local;

            to_string_prev_offset += string_size;
            to_string_offsets.push_back(to_string_prev_offset);

            string_prev_offset_local += string_size;
            bytes_to_copy += string_size;
        }

        size_t to_data_old_size = to_data.size();
        to_data.resize(to_data_old_size + bytes_to_copy);
        memcpySmallAllowReadWriteOverflow15(&to_data[to_data_old_size], &data[string_prev_offset],
            bytes_to_copy * sizeof(ColumnString::Chars_t::value_type));

        return array_size;
    };
};


/// Implementation of StringArraySource specific to arrays of constant strings.
class ConstStringArraySource : public StringArraySource
{
public:
    ConstStringArraySource(const Array & data_, size_t index_)
        : data(data_), index(index_)
    {
        data_size = 0;
        for (const auto & s : data)
            data_size += s.get<const String &>().size() + 1;
    }

    ChunkType getType() const override
    {
        return ChunkType::CONSTANT;
    }

    Chunk get() const override
    {
        return Chunk{var_callback};
    }

    void next() override
    {
    }

    size_t getDataSize() const override
    {
        return data_size;
    }

    size_t getStringOffsetsSize() const override
    {
        return data.size();
    }

    size_t getIndex() const override
    {
        return index;
    }

private:
    Array data;
    size_t data_size;
    size_t index;

    VarCallback var_callback = [&](ColumnString::Chars_t & to_data,
        ColumnString::Offset_t & to_string_prev_offset,
        ColumnString::Offsets_t & to_string_offsets) ALWAYS_INLINE
    {
        size_t array_size = data.size();

        for (size_t j = 0; j < array_size; ++j)
        {
            const std::string & str = data[j].get<const String &>();
            size_t string_size = str.size() + 1;    /// Including trailing zero byte.

            to_data.resize(to_string_prev_offset + string_size);
            memcpy(&to_data[to_string_prev_offset], str.data(),
                string_size * sizeof(std::string::value_type));    /// constant string have no padding bytes for memcpySmall... function.

            to_string_prev_offset += string_size;
            to_string_offsets.push_back(to_string_prev_offset);
        }

        return array_size;
    };
};


/// Access provider to the target array that receives the results of the
/// execution of the function multiIf.
class VarStringArraySink
{
public:
    VarStringArraySink(ColumnString::Chars_t & data_,
        ColumnString::Offsets_t & string_offsets_,
        ColumnArray::Offsets_t & array_offsets_,
        size_t data_size_,
        size_t offsets_size_,
        size_t row_count)
        : data(data_), string_offsets(string_offsets_), array_offsets(array_offsets_)
    {
        array_offsets.resize(row_count);
        string_offsets.reserve(offsets_size_);
        data.reserve(data_size_);
    }

    void store(const Chunk & chunk)
    {
        size_t array_size = chunk.var_callback(data, string_prev_offset, string_offsets);
        array_prev_offset += array_size;
        array_offsets[i] = array_prev_offset;
        ++i;
    }

private:
    ColumnString::Chars_t & data;
    ColumnString::Offsets_t & string_offsets;
    ColumnArray::Offsets_t & array_offsets;

    ColumnArray::Offset_t array_prev_offset = 0;
    ColumnString::Offset_t string_prev_offset = 0;
    size_t i = 0;
};


/// Create accessors for condition values.
CondSources createConds(const Block & block, const ColumnNumbers & args)
{
    CondSources conds;
    conds.reserve(getCondCount(args));

    for (size_t i = firstCond(); i < elseArg(args); i = nextCond(i))
        conds.emplace_back(block, args, i);
    return conds;
}


/// Create accessors for branch values.
bool createStringArraySources(StringArraySources & sources, const Block & block,
    const ColumnNumbers & args)
{
    auto append_source = [&](size_t i) -> bool
    {
        const IColumn * col = block.getByPosition(args[i]).column.get();
        const ColumnArray * col_arr = checkAndGetColumn<ColumnArray>(col);
        const ColumnString * var_col = col_arr ? checkAndGetColumn<ColumnString>(&col_arr->getData()) : nullptr;
        const ColumnConst * const_col = checkAndGetColumnConst<ColumnArray>(col);

        if ((col_arr && var_col) || const_col)
        {
            StringArraySourcePtr source;

            if (var_col)
                source = std::make_unique<VarStringArraySource>(var_col->getChars(),
                    var_col->getOffsets(), col_arr->getOffsets(), args[i]);
            else if (const_col)
                source = std::make_unique<ConstStringArraySource>(const_col->getValue<Array>(), args[i]);
            else
                throw Exception{"Unexpected type of column in then or else condition of multiIf function with Array(String) arguments",
                    ErrorCodes::LOGICAL_ERROR};

            sources.push_back(std::move(source));

            return true;
        }
        else
            return false;
    };

    sources.reserve(getBranchCount(args));

    for (size_t i = firstThen(); i < elseArg(args); i = nextThen(i))
    {
        if (!append_source(i))
            return false;
    }
    return append_source(elseArg(args));
}


auto computeResultSize(const StringArraySources & sources, size_t row_count)
{
    size_t max_var = 0;
    size_t max_var_string_offsets = 0;
    size_t max_const = 0;
    size_t max_const_string_offsets = 0;

    for (const auto & source : sources)
    {
        if (source->getType() == ChunkType::VARIABLE)
        {
            max_var = std::max(max_var, source->getDataSize());
            max_var_string_offsets = std::max(max_var_string_offsets,
                source->getStringOffsetsSize());
        }
        else if (source->getType() == ChunkType::CONSTANT)
        {
            max_const = std::max(max_const, source->getDataSize());
            max_const_string_offsets = std::max(max_const_string_offsets,
                source->getStringOffsetsSize());
        }
        else
            throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
    }

    return std::make_tuple(std::max(max_var, max_const * row_count), std::max(max_var_string_offsets, max_const_string_offsets * row_count));
}

/// Create the result column.
VarStringArraySink createSink(Block & block, const StringArraySources & sources,
    size_t result, size_t row_count)
{
    size_t offsets_size;
    size_t data_size;

    std::tie(data_size, offsets_size) = computeResultSize(sources, row_count);

    std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
    auto var_col_res = std::make_shared<ColumnArray>(col_res);
    block.getByPosition(result).column = var_col_res;

    return VarStringArraySink{col_res->getChars(), col_res->getOffsets(),
        var_col_res->getOffsets(), data_size, offsets_size, row_count};
}

}

/// Process a multiIf.
bool StringArrayEvaluator::perform(Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder)
{
    StringArraySources sources;
    if (!createStringArraySources(sources, block, args))
        return false;

    const CondSources conds = createConds(block, args);
    size_t row_count = conds[0].getSize();
    VarStringArraySink sink = createSink(block, sources, result, row_count);

    if (builder)
        builder.init(args);

    for (size_t cur_row = 0; cur_row < row_count; ++cur_row)
    {
        bool has_triggered_cond = false;

        size_t cur_source = 0;
        for (const auto & cond : conds)
        {
            if (cond.get(cur_row))
            {
                sink.store(sources[cur_source]->get());
                if (builder)
                    builder.update(sources[cur_source]->getIndex(), cur_row);
                has_triggered_cond = true;
                break;
            }
            ++cur_source;
        }

        if (!has_triggered_cond)
        {
            sink.store(sources.back()->get());
            if (builder)
                builder.update(sources.back()->getIndex(), cur_row);
        }

        for (auto & source : sources)
            source->next();
    }

    return true;
}

}

}
