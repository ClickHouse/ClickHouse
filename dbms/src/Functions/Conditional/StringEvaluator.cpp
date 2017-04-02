#include <Functions/Conditional/StringEvaluator.h>
#include <Functions/Conditional/common.h>
#include <Functions/Conditional/NullMapBuilder.h>
#include <Functions/Conditional/CondSource.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Types.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int ILLEGAL_COLUMN;

}

namespace Conditional
{

namespace
{

struct StringType
{
    static constexpr UInt64 VARIABLE = UINT64_C(1) << 0;
    static constexpr UInt64 FIXED = UINT64_C(1) << 1;
    static constexpr UInt64 CONSTANT = UINT64_C(1) << 2;
};

struct StringChunk
{
    using value_type = unsigned char;
    const value_type * pos_begin;
    const value_type * pos_end;
    UInt64 type;
};

/// This class provides access to the values of a string branch
/// (then, else) column.
class StringSource
{
public:
    virtual ~StringSource() {}
    virtual UInt64 getType() const = 0;
    virtual void next() = 0;
    virtual StringChunk get() const = 0;
    virtual size_t getSize() const { throw Exception{"Unsupported method", ErrorCodes::LOGICAL_ERROR}; }
    virtual size_t getDataSize() const = 0;
    virtual size_t getIndex() const = 0;
};

using StringSourcePtr = std::unique_ptr<StringSource>;
using StringSources = std::vector<StringSourcePtr>;


/// Implementation of StringSource specific to constant strings.
class ConstStringSource final : public StringSource
{
public:
    ConstStringSource(const std::string & str_, size_t size_, size_t index_)
        : str{str_}, size{size_}, index{index_},
        type{static_cast<UInt64>(StringType::CONSTANT | ((size > 0) ? StringType::FIXED : 0))}
    {
    }

    ConstStringSource(const ConstStringSource &) = delete;
    ConstStringSource & operator=(const ConstStringSource &) = delete;

    ConstStringSource(ConstStringSource &&) = default;
    ConstStringSource & operator=(ConstStringSource &&) = default;

    UInt64 getType() const override
    {
        return type;
    }

    void next() override
    {
    }

    StringChunk get() const override
    {
        StringChunk chunk;
        chunk.pos_begin = reinterpret_cast<const unsigned char *>(str.data());
        chunk.pos_end = chunk.pos_begin + str.size();
        chunk.type = type;
        return chunk;
    }

    inline size_t getSize() const override
    {
        return size;
    }

    inline size_t getDataSize() const override
    {
        return str.length();
    }

    inline size_t getIndex() const override
    {
        return index;
    }

private:
    const std::string & str;
    size_t size;
    size_t index;
    UInt64 type;
};


/// Implementation of StringSource specific to fixed strings.
class FixedStringSource final : public StringSource
{
public:
    FixedStringSource(const ColumnFixedString::Chars_t & data_, size_t size_, size_t index_)
        : data{data_}, size{size_}, index{index_}
    {
    }

    FixedStringSource(const FixedStringSource &) = delete;
    FixedStringSource & operator=(const FixedStringSource &) = delete;

    FixedStringSource(FixedStringSource &&) = default;
    FixedStringSource & operator=(FixedStringSource &&) = default;

    UInt64 getType() const override
    {
        return StringType::FIXED;
    }

    void next() override
    {
        ++i;
    }

    StringChunk get() const override
    {
        StringChunk chunk;
        chunk.pos_begin = &data[i * size];
        chunk.pos_end = chunk.pos_begin + size;
        chunk.type = StringType::FIXED;

        return chunk;
    }

    inline size_t getSize() const override
    {
        return size;
    }

    inline size_t getDataSize() const override
    {
        return data.size();
    }

    inline size_t getIndex() const override
    {
        return index;
    }

private:
    const ColumnFixedString::Chars_t & data;
    size_t size;
    size_t index;
    size_t i = 0;
};


/// Implementation of StringSource specific to variable strings.
class VarStringSource final : public StringSource
{
public:
    VarStringSource(const ColumnString::Chars_t & data_,
        const ColumnString::Offsets_t & offsets_, size_t index_)
        : data{data_}, offsets{offsets_}, index{index_}
    {
    }

    VarStringSource(const VarStringSource &) = delete;
    VarStringSource & operator=(const VarStringSource &) = delete;

    VarStringSource(VarStringSource &&) = default;
    VarStringSource & operator=(VarStringSource &&) = default;

    UInt64 getType() const override
    {
        return StringType::VARIABLE;
    }

    void next() override
    {
        prev_offset = offsets[i];
        ++i;
    }

    StringChunk get() const override
    {
        StringChunk chunk;
        chunk.pos_begin = &data[prev_offset];
        chunk.pos_end = chunk.pos_begin + (offsets[i] - prev_offset);
        chunk.type = StringType::VARIABLE;
        return chunk;
    }

    inline size_t getDataSize() const override
    {
        return data.size();
    }

    inline size_t getIndex() const override
    {
        return index;
    }

private:
    const ColumnString::Chars_t & data;
    const ColumnString::Offsets_t & offsets;
    ColumnString::Offset_t prev_offset = 0;
    size_t index;
    size_t i = 0;
};


/// Access provider to the target array that receives the results of the
/// execution of the function multiIf.
class StringSink
{
public:
    virtual ~StringSink() {}
    virtual void store(const StringChunk & chunk) = 0;
};


/// Implementation of StringSink for the case when the result column
/// has the fixed string type. It happens only if all the branches of
/// the function multiIf currently being executed have the fixed string
/// type.
class FixedStringSink : public StringSink
{
public:
    FixedStringSink(ColumnFixedString::Chars_t & data_, size_t size_, size_t data_size_)
        : data{data_}, size{size_}
    {
        data.reserve(data_size_);
    }

    FixedStringSink(const FixedStringSink &) = delete;
    FixedStringSink & operator=(const FixedStringSink &) = delete;

    FixedStringSink(FixedStringSink &&) = default;
    FixedStringSink & operator=(FixedStringSink &&) = default;

    void store(const StringChunk & chunk) override
    {
        if (!(chunk.type & StringType::FIXED))
            throw Exception{"Logical error in implementation of multiIf: one of arguments to build a FixedString is not fixed-size",
                ErrorCodes::LOGICAL_ERROR};

        size_t size_to_write = chunk.pos_end - chunk.pos_begin;
        data.resize(data.size() + size_to_write);
        memcpy(&data[i * size], chunk.pos_begin,
            size_to_write * sizeof(StringChunk::value_type));
        ++i;
    }

private:
    ColumnFixedString::Chars_t & data;
    size_t size;
    size_t i = 0;
};


/// Implementation of StringSink for the case when the result column
/// has the variable string type.
class VarStringSink : public StringSink
{
public:
    VarStringSink(ColumnString::Chars_t & data_, ColumnString::Offsets_t & offsets_,
        size_t data_size_, size_t offsets_size_)
        : data{data_}, offsets{offsets_}
    {
        offsets.resize(offsets_size_);
        data.reserve(data_size_);
    }

    VarStringSink(const VarStringSink &) = delete;
    VarStringSink & operator=(const VarStringSink &) = delete;

    VarStringSink(VarStringSink &&) = default;
    VarStringSink & operator=(VarStringSink &&) = delete;

    void store(const StringChunk & chunk) override
    {
        if (chunk.type & StringType::VARIABLE)
        {
            size_t size_to_write = chunk.pos_end - chunk.pos_begin;
            data.resize(data.size() + size_to_write);
            memcpySmallAllowReadWriteOverflow15(&data[prev_offset], chunk.pos_begin,
                size_to_write * sizeof(StringChunk::value_type));
            prev_offset += size_to_write;
        }
        else if (chunk.type & StringType::CONSTANT)
        {
            size_t size_to_write = chunk.pos_end - chunk.pos_begin + 1;
            data.resize(data.size() + size_to_write);
            memcpy(&data[prev_offset], chunk.pos_begin,        /// constant string have no padding bytes for memcpySmall... function.
                size_to_write * sizeof(StringChunk::value_type));
            prev_offset += size_to_write;
        }
        else if (chunk.type & StringType::FIXED)
        {
            size_t size_to_write = chunk.pos_end - chunk.pos_begin;
            data.resize(data.size() + size_to_write + 1);
            memcpySmallAllowReadWriteOverflow15(&data[prev_offset], chunk.pos_begin,
                size_to_write * sizeof(StringChunk::value_type));
            data.back() = 0;
            prev_offset += size_to_write + 1;
        }
        else
            throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

        offsets[i] = prev_offset;
        ++i;
    }

private:
    ColumnString::Chars_t & data;
    ColumnString::Offsets_t & offsets;
    ColumnString::Offset_t prev_offset = 0;
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

const std::string null_string;


/// Create accessors for branch values.
bool createStringSources(StringSources & sources, const Block & block,
    const ColumnNumbers & args)
{
    auto append_source = [&](size_t i)
    {
        const IColumn * col = block.safeGetByPosition(args[i]).column.get();
        const ColumnString * var_col = typeid_cast<const ColumnString *>(col);
        const ColumnFixedString * fixed_col = typeid_cast<const ColumnFixedString *>(col);
        const ColumnConstString * const_col = typeid_cast<const ColumnConstString *>(col);

        StringSourcePtr source;

        if (col->isNull())
            source = std::make_unique<ConstStringSource>(null_string, 1, args[i]);
        else if (var_col != nullptr)
            source = std::make_unique<VarStringSource>(var_col->getChars(),
                var_col->getOffsets(), args[i]);
        else if (fixed_col != nullptr)
            source = std::make_unique<FixedStringSource>(fixed_col->getChars(),
                fixed_col->getN(), args[i]);
        else if (const_col != nullptr)
        {
            /// If we actually have a fixed string, get its capacity.
            size_t size = 0;
            const IDataType * data_type = const_col->getDataType().get();
            if (data_type != nullptr)
            {
                const DataTypeFixedString * fixed = typeid_cast<const DataTypeFixedString *>(data_type);
                if (fixed != nullptr)
                    size = fixed->getN();
            }

            source = std::make_unique<ConstStringSource>(const_col->getData(), size, args[i]);
        }
        else
            return false;

        sources.push_back(std::move(source));
        return true;
    };

    sources.reserve(getBranchCount(args));

    for (size_t i = firstThen(); i < elseArg(args); i = nextThen(i))
    {
        if (!append_source(i))
            return false;
    }
    return append_source(elseArg(args));
}


size_t computeResultSize(const StringSources & sources, size_t row_count)
{
    size_t max_var = 0;
    size_t max_fixed = 0;
    size_t max_const = 0;

    for (const auto & source : sources)
    {
        if (source->getType() & StringType::VARIABLE)
            max_var = std::max(max_var, source->getDataSize());
        else if (source->getType() & StringType::FIXED)
            max_fixed = std::max(max_fixed, source->getDataSize());
        else if (source->getType() & StringType::CONSTANT)
            max_const = std::max(max_const, source->getDataSize());
        else
            throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
    }

    if (max_var > 0)
    {
        if (max_fixed > 0)
            return std::max(max_var, max_fixed + row_count);
        else
            return max_var;
    }
    else if (max_fixed > 0)
        return max_fixed;
    else if (max_const > 0)
        return (max_const + 1) * row_count;
    else
        throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
}

/// Updates a fixed or variable string sink.
template <typename SinkType>
class SinkUpdater
{
public:
    static void execute(Block & block, const StringSources & sources, const CondSources & conds,
        SinkType & sink, size_t row_count, const ColumnNumbers & args, size_t result,
        NullMapBuilder & builder)
    {
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
    }
};

/// Sink type-specific handler of multiIf.
template <bool hasFixedSources>
class Performer;

template <>
class Performer<true>
{
public:
    static void execute(const StringSources & sources, const CondSources & conds,
        size_t row_count, Block & block, const ColumnNumbers & args, size_t result,
        NullMapBuilder & builder)
    {
        FixedStringSink sink = createSink(block, sources, result, row_count);
        SinkUpdater<FixedStringSink>::execute(block, sources, conds, sink, row_count,
            args, result, builder);
    }

private:
    /// Create the result column.
    static FixedStringSink createSink(Block & block, const StringSources & sources,
        size_t result, size_t row_count)
    {
        size_t first_size = sources[0]->getSize();
        for (size_t i = 1; i < sources.size(); ++i)
        {
            if (sources[i]->getSize() != first_size)
                throw Exception{"All the columns have FixedString type but one or"
                    " more columns have various sizes.", ErrorCodes::ILLEGAL_COLUMN};
        }

        size_t first_data_size = sources[0]->getDataSize();

        auto col_res = std::make_shared<ColumnFixedString>(first_size);
        block.safeGetByPosition(result).column = col_res;
        return FixedStringSink{col_res->getChars(), first_size, first_data_size};
    }
};

template <>
class Performer<false>
{
public:
    static void execute(const StringSources & sources, const CondSources & conds,
        size_t row_count, Block & block, const ColumnNumbers & args, size_t result,
        NullMapBuilder & builder)
    {
        VarStringSink sink = createSink(block, sources, result, row_count);
        SinkUpdater<VarStringSink>::execute(block, sources, conds, sink, row_count,
            args, result, builder);
    }

private:
    /// Create the result column.
    static VarStringSink createSink(Block & block, const StringSources & sources,
        size_t result, size_t row_count)
    {
        size_t offsets_size = row_count;
        size_t data_size = computeResultSize(sources, row_count);

        std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
        block.safeGetByPosition(result).column = col_res;
        return VarStringSink{col_res->getChars(), col_res->getOffsets(),
            data_size, offsets_size};
    }
};

}

/// Process a multiIf.
bool StringEvaluator::perform(Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder)
{
    StringSources sources;
    if (!createStringSources(sources, block, args))
        return false;

    const CondSources conds = createConds(block, args);
    size_t row_count = conds[0].getSize();

    bool has_only_fixed_sources = true;
    for (const auto & source : sources)
    {
        if (!(source->getType() & StringType::FIXED))
        {
            has_only_fixed_sources = false;
            break;
        }
    }

    if (has_only_fixed_sources)
        Performer<true>::execute(sources, conds, row_count, block, args, result, builder);
    else
        Performer<false>::execute(sources, conds, row_count, block, args, result, builder);

    return true;
}

}

}
