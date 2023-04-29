#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnLowCardinality.h>
#include <Interpreters/Set.h>
#include <Interpreters/BloomFilter.h>
#include <iostream>

// #include <Functions/IFunction.h>
// #include <Functions/FunctionFactory.h>
// #include <DataTypes/DataTypesNumber.h>
// #include <DataTypes/DataTypeArray.h>
// #include <DataTypes/DataTypeString.h>
// #include <Interpreters/Context.h>
// #include <Common/SipHash.h>
// #include <Common/HashTable/Hash.h>
//#include <Common/BloomFilter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

namespace
{

// using BloomFilter = SimpleBloomFilter<SipHash>;
// using BloomFilterPtr = std::shared_ptr<BloomFilter>;

// BloomFilterPtr createBloomFilter(const UInt64 & size)
// {
//     return std::make_shared<BloomFilter>(size);
// }

// struct FunctionProbableInName;

// template <> struct FunctionProbableInName { static constexpr auto name = "probalIn"; };


class FunctionProbableIn : public IFunction
{
public:

    static constexpr auto name = "probalIn";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionProbableIn>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, [[maybe_unused]] size_t input_rows_count) const override
    {

        // const ColumnWithTypeAndName & left_arg = arguments[0];
        // const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(left_arg.column.get());
        // //const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(left_arg.type.get());

        // const auto & tuple_columns = tuple->getColumns();
        // //const DataTypes & tuple_types = type_tuple->getElements();
        // size_t tuple_size = tuple_columns.size();
        // for (size_t i = 0; i < tuple_size; ++i) {
        //     ColumnPtr colPtr = tuple_columns[i];
        //     //auto col = *(colPtr.get());
        //     for (size_t j = 0; j < colPtr->size(); j++) {
        //         std::cout << (colPtr->operator[](j)).get<String>() << "\n";
        //     }

        // }

        
        // auto filter_size = arguments[1].column->getUInt(0);

        // auto filter = createBloomFilter(filter_size);

        // auto col = block.getByPosition(arguments[0]).column.get();
        // auto col_size = col->size();    

        // const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(arguments[0].column.get());

        // //ColumnPtr column_set_ptr = arguments[1].column;

        // for (size_t i = 0; i < col_size; i++)
        // {
        //     (*col)[i].get<UInt64>()
        // }

        // auto res = ColumnUInt8::create(col_size);
        // auto & out_data = res->getData();
        // for (size_t i = 0; i < col_size; i++)
        // {
        //     out_data[i] = filter->find((*col)[i].get<UInt64>());
        // }

        //block.getByPosition(result).column = std::move(res);
        std::cout << "AAA PROBAL IN\n";
        //return ColumnUInt8::create(input_rows_count, 1u);

        ColumnPtr column_set_ptr = arguments[1].column;
        const ColumnSet * column_set = checkAndGetColumnConstData<const ColumnSet>(column_set_ptr.get());
        if (!column_set)
            column_set = checkAndGetColumn<const ColumnSet>(column_set_ptr.get());
        if (!column_set)
            throw Exception("Second argument for function '" + getName() + "' must be Set; found " + column_set_ptr->getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        ColumnsWithTypeAndName columns_of_key_columns;

        /// First argument may be a tuple or a single column.
        const ColumnWithTypeAndName & left_arg = arguments[0];
        const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(left_arg.column.get());
        const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(left_arg.column.get());
        const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(left_arg.type.get());

        ColumnPtr materialized_tuple;
        if (const_tuple)
        {
            materialized_tuple = const_tuple->convertToFullColumn();
            tuple = typeid_cast<const ColumnTuple *>(materialized_tuple.get());
        }

        auto set = column_set->getData();
        auto set_types = set->getDataTypes();

        if (tuple && set_types.size() != 1 && set_types.size() == tuple->tupleSize())
        {
            const auto & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = type_tuple->getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
                columns_of_key_columns.emplace_back(tuple_columns[i], tuple_types[i], "_" + toString(i));
        }
        else
            columns_of_key_columns.emplace_back(left_arg);

        /// Replace single LowCardinality column to it's dictionary if possible.
        ColumnPtr lc_indexes = nullptr;
        bool is_const = false;
        if (columns_of_key_columns.size() == 1)
        {
            auto & arg = columns_of_key_columns.at(0);
            const auto * col = arg.column.get();
            if (const auto * const_col = typeid_cast<const ColumnConst *>(col))
            {
                col = &const_col->getDataColumn();
                is_const = true;
            }

            if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(col))
            {
                lc_indexes = lc->getIndexesPtr();
                arg.column = lc->getDictionary().getNestedColumn();
                arg.type = removeLowCardinality(arg.type);
            }
        }

        auto res = set->execute(columns_of_key_columns, false);

        if (lc_indexes)
            res = res->index(*lc_indexes, 0);

        if (is_const)
            res = ColumnUInt8::create(input_rows_count, res->getUInt(0));

        if (res->size() != input_rows_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Output size is different from input size, expect {}, get {}", input_rows_count, res->size());

        return res;

    }
};


void registerFunctionsProbableInImpl(FunctionFactory & factory)
{
    factory.registerFunction<FunctionProbableIn>();
}


}

REGISTER_FUNCTION(ProbableIn)
{
    registerFunctionsProbableInImpl(factory);

}

}
