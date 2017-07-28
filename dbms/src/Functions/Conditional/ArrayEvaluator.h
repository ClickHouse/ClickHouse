#pragma once

#include <Functions/Conditional/CondException.h>
#include <Functions/Conditional/common.h>
#include <Functions/Conditional/NullMapBuilder.h>
#include <Functions/Conditional/CondSource.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Common/memcpySmall.h>
#include <Common/typeid_cast.h>
#include <DataTypes/NumberTraits.h>


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

template <typename TType>
class Chunk
{
public:
    using value_type = TType;

public:
    Chunk(TType * pos_begin_, size_t size_)
        : pos_begin{pos_begin_}, size{size_}, is_const{true}
    {
    }

    Chunk(std::function<TType(size_t)> accessor_, size_t size_)
        : accessor{accessor_}, size{size_}, is_const{false}
    {
    }

    inline TType * data() const
    {
        return pos_begin;
    }

    inline TType operator[](size_t j) const
    {
        return accessor(j);
    }

    inline size_t getSize() const
    {
        return size;
    }

    inline bool isConst() const
    {
        return is_const;
    }

private:
    TType * pos_begin = nullptr;
    std::function<TType(size_t)> accessor;
    size_t size;
    bool is_const;
};

/// This class provides type-independent access to the values of a numeric array
/// branch (then, else) column. Returned values have the type TResult.
template <typename TResult>
class IArraySource
{
public:
    virtual void next() = 0;
    virtual Chunk<TResult> get() const = 0;
    virtual size_t getDataSize() const = 0;
    virtual bool isConst() const = 0;
    virtual ~IArraySource() {}
};

template <typename TResult>
using IArraySourcePtr = std::unique_ptr<IArraySource<TResult>>;

template <typename TResult>
using IArraySources = std::vector<IArraySourcePtr<TResult>>;

/// Column type-specific implementation of IArraySource for arrays.
template <typename TResult, typename TType>
class ArraySource final : public IArraySource<TResult>
{
public:
    ArraySource(const PaddedPODArray<TType> & data_, const ColumnArray::Offsets_t & offsets_)
        : data(data_), offsets(offsets_)
    {
    }

    void next() override
    {
        prev_offset = offsets[i];
        ++i;
    }

    Chunk<TResult> get() const override
    {
        const TType * pos_begin = &data[prev_offset];
        const TType * pos_end = pos_begin + (offsets[i] - prev_offset);

        std::function<TResult(size_t)> get_value = [pos_begin](size_t j)
        {
            return static_cast<TResult>(*(pos_begin + j));
        };

        size_t size = pos_end - pos_begin;
        return Chunk<TResult>{get_value, size};
    };

    size_t getDataSize() const override
    {
        return data.size();
    }

    bool isConst() const override
    {
        return false;
    }

private:
    const PaddedPODArray<TType> & data;
    const ColumnArray::Offsets_t & offsets;
    ColumnArray::Offset_t prev_offset = 0;
    size_t i = 0;
};

/// Column type-specific implementation of IArraySource for constant arrays.
template <typename TResult, typename TType>
class ConstArraySource final : public IArraySource<TResult>
{
public:
    ConstArraySource(const Array & array_)
        : array(array_)
    {
    }

    void next() override
    {
    }

    Chunk<TResult> get() const override
    {
        if (!converted)
        {
            converted = std::make_unique<PaddedPODArray<TResult>>(array.size());
            size_t size = array.size();
            for (size_t i = 0; i < size; ++i)
                (*converted)[i] = array[i].template get<typename NearestFieldType<TType>::Type>();
        }

        return {converted->data(), converted->size()};
    }

    size_t getDataSize() const override
    {
        return array.size();
    }

    bool isConst() const override
    {
        return true;
    }

private:
    Array array;
    mutable std::unique_ptr<PaddedPODArray<TResult>> converted;
};

/// Access provider to the target array that receives the results of the
/// execution of the function multiIf.
template <typename TResult>
class ArraySink
{
public:
    ArraySink(PaddedPODArray<TResult> & data_, ColumnArray::Offsets_t & offsets_,
        size_t data_size_, size_t offsets_size_)
        : data(data_), offsets(offsets_)
    {
        offsets.resize(offsets_size_);
        data.reserve(data_size_);
    }

    void store(const Chunk<TResult> & chunk)
    {
        data.resize(data.size() + chunk.getSize());

        if (chunk.isConst())
        {
            /// Copy from const.
            memcpySmallAllowReadWriteOverflow15(&data[prev_offset], chunk.data(),
                chunk.getSize() * sizeof(typename Chunk<TResult>::value_type));
        }
        else
        {
            /// Copy from vector.
            for (size_t j = 0; j < chunk.getSize(); ++j)
                data[prev_offset + j] = chunk[j];
        }

        prev_offset += chunk.getSize();
        offsets[i] = prev_offset;
        ++i;
    }

private:
    PaddedPODArray<TResult> & data;
    ColumnArray::Offsets_t & offsets;
    ColumnArray::Offset_t prev_offset = 0;
    size_t i = 0;
};

/// Create a numeric array column accessor if TType is the type registered
/// in the specified branch info.
template <typename TResult, typename TType>
class ArraySourceCreator final
{
public:
    static bool execute(IArraySources<TResult> & sources, const Block & block,
        const ColumnNumbers & args, const Branch & br)
    {
        auto type_name = br.type->getName();
        if (TypeName<TType>::get() == type_name)
        {
            const IColumn * col = block.getByPosition(args[br.index]).column.get();

            IArraySourcePtr<TResult> source;

            if (auto col_array = checkAndGetColumn<ColumnArray>(col))
            {
                const ColumnVector<TType> * content = typeid_cast<const ColumnVector<TType> *>(&col_array->getData());
                if (!content)
                    throw Exception{"Unexpected type of Array column in function multiIf with numeric Array arguments", ErrorCodes::LOGICAL_ERROR};
                source = std::make_unique<ArraySource<TResult, TType>>(content->getData(), col_array->getOffsets());
            }
            else if (auto col_const_array = checkAndGetColumnConst<ColumnArray>(col))
                source = std::make_unique<ConstArraySource<TResult, TType>>(col_const_array->getValue<Array>());

            sources.push_back(std::move(source));

            return true;
        }
        else
            return false;
    }
};

/// Case for null sources.
template <typename TResult>
class ArraySourceCreator<TResult, Null> final
{
public:
    static bool execute(IArraySources<TResult> & sources, const Block & block,
        const ColumnNumbers & args, const Branch & br)
    {
        auto type_name = br.type->getName();
        if (TypeName<Null>::get() == type_name)
        {
            IArraySourcePtr<TResult> source;
            source = std::make_unique<ConstArraySource<TResult, Null>>(Array());
            sources.push_back(std::move(source));

            return true;
        }
        else
            return false;
    }
};

}

/// Processing of multiIf in the case of numeric array types.
template <typename TResult>
class ArrayEvaluator final
{
public:
    static void perform(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder)
    {
        const CondSources conds = createConds(block, args);
        size_t row_count = conds[0].getSize();
        IArraySources<TResult> sources = createSources(block, args, branches);
        ArraySink<TResult> sink = createSink(block, sources, result, row_count);

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
                        builder.update(args[branches[cur_source].index], cur_row);
                    has_triggered_cond = true;
                    break;
                }
                ++cur_source;
            }

            if (!has_triggered_cond)
            {
                sink.store(sources.back()->get());
                if (builder)
                    builder.update(args[branches.back().index], cur_row);
            }

            for (auto & source : sources)
                source->next();
        }
    }

private:
    /// Create accessors for condition values.
    static CondSources createConds(const Block & block, const ColumnNumbers & args)
    {
        CondSources conds;
        conds.reserve(getCondCount(args));

        for (size_t i = firstCond(); i < elseArg(args); i = nextCond(i))
            conds.emplace_back(block, args, i);
        return conds;
    }

    /// Create accessors for branch values.
    static IArraySources<TResult> createSources(const Block & block,
        const ColumnNumbers & args, const Branches & branches)
    {
        IArraySources<TResult> sources;
        sources.reserve(branches.size());

        for (const auto & br : branches)
        {
            if (! (ArraySourceCreator<TResult, UInt8>::execute(sources, block, args, br)
                || ArraySourceCreator<TResult, UInt16>::execute(sources, block, args, br)
                || ArraySourceCreator<TResult, UInt32>::execute(sources, block, args, br)
                || ArraySourceCreator<TResult, UInt64>::execute(sources, block, args, br)
                || ArraySourceCreator<TResult, Int8>::execute(sources, block, args, br)
                || ArraySourceCreator<TResult, Int16>::execute(sources, block, args, br)
                || ArraySourceCreator<TResult, Int32>::execute(sources, block, args, br)
                || ArraySourceCreator<TResult, Int64>::execute(sources, block, args, br)
                || ArraySourceCreator<TResult, Float32>::execute(sources, block, args, br)
                || ArraySourceCreator<TResult, Float64>::execute(sources, block, args, br)
                || ArraySourceCreator<TResult, Null>::execute(sources, block, args, br)))
                throw Exception{"Unexpected type of Array column in function multiIf with numeric Array arguments", ErrorCodes::LOGICAL_ERROR};
        }

        return sources;
    }

    static size_t computeResultSize(const IArraySources<TResult> & sources, size_t row_count)
    {
        size_t max_size = 0;

        for (const auto & source : sources)
            max_size = std::max(max_size, source->isConst() ? source->getDataSize() * row_count : source->getDataSize());

        return max_size;
    }

    /// Create the result column.
    static ArraySink<TResult> createSink(Block & block, const IArraySources<TResult> & sources,
        size_t result, size_t row_count)
    {
        size_t offsets_size = row_count;
        size_t data_size = computeResultSize(sources, row_count);

        auto col_res_vec = std::make_shared<ColumnVector<TResult>>();
        auto col_res_array = std::make_shared<ColumnArray>(col_res_vec);
        block.getByPosition(result).column = col_res_array;

        return ArraySink<TResult>{col_res_vec->getData(), col_res_array->getOffsets(),
            data_size, offsets_size};
    }
};

/// Processing of multiIf in the case of an invalid return type.
template <>
class ArrayEvaluator<NumberTraits::Error>
{
public:
    /// For the meaning of the builder parameter, see the FunctionMultiIf::perform() declaration.
    static void perform(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder)
    {
        throw CondException{CondErrorCodes::ARRAY_EVALUATOR_INVALID_TYPES};
    }
};

}

}
