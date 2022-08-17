#pragma once

#include <Common/config.h>

#if USE_DATASKETCHES

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <AggregateFunctions/AggregateFunctionUniq.h>

namespace DB
{


    namespace ErrorCodes
    {
        extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    }

    struct UniqThetaIntersectImpl
    {
        static void apply(AggregateFunctionUniqThetaData & sketch_data_1, const AggregateFunctionUniqThetaData & sketch_data_2)
        {
            sketch_data_1.set.intersect(sketch_data_2.set);
        }
    };

    struct UniqThetaUnionImpl
    {
        static void apply(AggregateFunctionUniqThetaData & sketch_data_1, const AggregateFunctionUniqThetaData & sketch_data_2)
        {
            sketch_data_1.set.merge(sketch_data_2.set);
        }
    };

    struct UniqThetaNotImpl
    {
        static void apply(AggregateFunctionUniqThetaData & sketch_data_1, const AggregateFunctionUniqThetaData & sketch_data_2)
        {
            sketch_data_1.set.aNotB(sketch_data_2.set);
        }
    };

    template <typename Impl, typename Name>
    class FunctionUniqTheta : public IFunction
    {
    public:
        static constexpr auto name = Name::name;

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUniqTheta>(); }

        String getName() const override { return name; }

        bool isVariadic() const override { return false; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        size_t getNumberOfArguments() const override { return 2; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            const auto * sketch_type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
            if (!(sketch_type0 && sketch_type0->getFunctionName() == "uniqTheta"))
                throw Exception(
                        "First argument for function " + getName() + " must be a uniqTheta but it has type " + arguments[0]->getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto * sketch_type1 = typeid_cast<const DataTypeAggregateFunction *>(arguments[1].get());
            if (!(sketch_type1 && sketch_type1->getFunctionName() == "uniqTheta"))
                throw Exception(
                        "Second argument for function " + getName() + " must be a uniqTheta but it has type " + arguments[1]->getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            //todo:check
            if (sketch_type0->getArgumentsDataTypes()[0]->getTypeId() != sketch_type1->getArgumentsDataTypes()[0]->getTypeId())
                throw Exception(
                        "The nested type in uniqThetas must be the same, but one is " + sketch_type0->getArgumentsDataTypes()[0]->getName()
                        + ", and the other is " + sketch_type1->getArgumentsDataTypes()[0]->getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            return arguments[0];
        }

        bool useDefaultImplementationForConstants() const override { return true; }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const ColumnAggregateFunction * column_ptrs[2];
            bool is_column_const[2];
            for (size_t i = 0; i < 2; ++i)
            {
                if (const auto * argument_column_const = typeid_cast<const ColumnConst *>(arguments[i].column.get()))
                {
                    column_ptrs[i] = typeid_cast<const ColumnAggregateFunction *>(argument_column_const->getDataColumnPtr().get());
                    is_column_const[i] = true;
                }
                else
                {
                    column_ptrs[i] = typeid_cast<const ColumnAggregateFunction *>(arguments[i].column.get());
                    is_column_const[i] = false;
                }
            }

            auto col_to = ColumnAggregateFunction::create(column_ptrs[0]->getAggregateFunction());

            col_to->reserve(input_rows_count);

            const PaddedPODArray<AggregateDataPtr> & container0 = column_ptrs[0]->getData();
            const PaddedPODArray<AggregateDataPtr> & container1 = column_ptrs[1]->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const AggregateDataPtr data_ptr_0 = is_column_const[0] ? container0[0] : container0[i];
                const AggregateDataPtr data_ptr_1 = is_column_const[1] ? container1[0] : container1[i];

                col_to->insertFrom(data_ptr_0);
                AggregateFunctionUniqThetaData & sketch_data_1 = *reinterpret_cast<AggregateFunctionUniqThetaData *>(col_to->getData()[i]);
                const AggregateFunctionUniqThetaData & sketch_data_2
                        = *reinterpret_cast<const AggregateFunctionUniqThetaData *>(data_ptr_1);
                Impl::apply(sketch_data_1, sketch_data_2);
            }
            return col_to;
        }
    };

    struct NameUniqThetaIntersect
    {
        static constexpr auto name = "uniqThetaIntersect";
    };

    struct NameUniqThetaUnion
    {
        static constexpr auto name = "uniqThetaUnion";
    };

    struct NameUniqThetaNot
    {
        static constexpr auto name = "uniqThetaNot";
    };

    using FunctionUniqThetaIntersect = FunctionUniqTheta<UniqThetaIntersectImpl, NameUniqThetaIntersect>;
    using FunctionUniqThetaUnion = FunctionUniqTheta<UniqThetaUnionImpl, NameUniqThetaUnion>;
    using FunctionUniqThetaNot = FunctionUniqTheta<UniqThetaNotImpl, NameUniqThetaNot>;

}


#endif
