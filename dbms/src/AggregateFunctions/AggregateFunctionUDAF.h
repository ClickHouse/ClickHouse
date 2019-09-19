#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Interpreters/Compiler.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_CODE;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <const char * Header, const char * Body>
class AggregateFunctionUDAF final : public IAggregateFunctionHelper<
    AggregateFunctionUDAF<Header, Body>
>
{
private:
    const DataTypes arguments;

    // generate in the constructor

    SharedLibraryPtr lib;

    bool data_trivial;
    size_t data_size;
    size_t data_align;

    void (* funcInit)(
        AggregateDataPtr
    );
    void (* funcFree)(
        AggregateDataPtr
    );
    void (* funcType)(
        DataTypePtr &,
        const DataTypes &
    );
    void (* funcAdd)(
        AggregateDataPtr,
        const IColumn **,
        size_t
    );
    void (* funcSerialize)(
        ConstAggregateDataPtr,
        WriteBuffer &
    );
    void (* funcDeserialize)(
        AggregateDataPtr,
        ReadBuffer &
    );
    void (* funcMerge)(
        AggregateDataPtr,
        ConstAggregateDataPtr
    );
    void (* funcReduce)(
        ConstAggregateDataPtr,
        IColumn &
    );

public:
    AggregateFunctionUDAF(
        const String & code,
        const DataTypes & arguments_,
        const Array & params_
    ) :
        IAggregateFunctionHelper<
            AggregateFunctionUDAF<Header, Body>
        > {arguments_, params_},
        arguments {arguments_}
    {
        static Compiler compiler {
            "/dev/shm/ClickHouseUDF",
            1
        };

        lib = compiler.getOrCount(
            getName() + ':' + code,
            0,
            "",
            [code]() -> std::string
            {
                std::ostringstream out;

                out << Header << std::endl;
                out << std::endl;

                out << code << std::endl;
                out << std::endl;

                out << Body << std::endl;

                return out.str();
            },
            [](SharedLibraryPtr &)
            {
                // nothing
            }
        );

        if (!lib)
            throw Exception {
                "Code of user-defined aggregate function " + getName()
                    + " does not compile",
                ErrorCodes::CANNOT_COMPILE_CODE
            };

        data_trivial = lib->template get<
            decltype(data_trivial) (*)()
        >("dataTrivial")();
        data_size = lib->template get<
            decltype(data_size) (*)()
        >("dataSize")();
        data_align = lib->template get<
            decltype(data_align) (*)()
        >("dataAlign")();

        funcInit = lib->template get<
            decltype(funcInit)
        >("funcInit");
        funcFree = lib->template get<
            decltype(funcFree)
        >("funcFree");
        funcType = lib->template get<
            decltype(funcType)
        >("funcType");
        funcAdd = lib->template get<
            decltype(funcAdd)
        >("funcAdd");
        funcMerge = lib->template get<
            decltype(funcMerge)
        >("funcMerge");
        funcSerialize = lib->template get<
            decltype(funcSerialize)
        >("funcSerialize");
        funcDeserialize = lib->template get<
            decltype(funcDeserialize)
        >("funcDeserialize");
        funcReduce = lib->template get<
            decltype(funcReduce)
        >("funcReduce");
    }

    String getName() const override
    {
        return "udaf";
    }

    const char * getHeaderFilePath() const override
    {
        return __FILE__;
    }

    bool hasTrivialDestructor() const override
    {
        return data_trivial;
    }

    size_t sizeOfData() const override
    {
        return data_size;
    }

    size_t alignOfData() const override
    {
        return data_align;
    }

    void create(AggregateDataPtr place) const override
    {
        funcInit(place);
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        funcFree(place);
    }

    void add(
        AggregateDataPtr place,
        const IColumn ** columns,
        size_t row_num,
        Arena *
    ) const override
    {
        funcAdd(place, columns, row_num);
    }

    void merge(
        AggregateDataPtr place,
        ConstAggregateDataPtr rhs, Arena *
    ) const override
    {
        funcMerge(place, rhs);
    }

    void serialize(
        ConstAggregateDataPtr place,
        WriteBuffer & buf
    ) const override
    {
        funcSerialize(place, buf);
    }

    void deserialize(
        AggregateDataPtr place,
        ReadBuffer & buf, Arena *
    ) const override
    {
        funcDeserialize(place, buf);
    }

    DataTypePtr getReturnType() const override
    {
        DataTypePtr result;
        funcType(result, arguments);

        if (!result)
            throw Exception {
                "Argument types of user-defined aggregate function "
                    + getName() + " does not match",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
            };

        return result;
    }

    void insertResultInto(
        ConstAggregateDataPtr place,
        IColumn & to
    ) const override
    {
        funcReduce(place, to);
    }
};

}
