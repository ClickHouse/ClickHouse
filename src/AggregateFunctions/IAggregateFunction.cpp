#include "config.h"

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

DataTypePtr IAggregateFunction::getStateType() const
{
    return std::make_shared<DataTypeAggregateFunction>(shared_from_this(), argument_types, parameters);
}

DataTypePtr IAggregateFunction::getNormalizedStateType() const
{
    DataTypes normalized_argument_types;
    normalized_argument_types.reserve(argument_types.size());
    for (const auto & arg : argument_types)
        normalized_argument_types.emplace_back(arg->getNormalizedType());
    return std::make_shared<DataTypeAggregateFunction>(shared_from_this(), normalized_argument_types, parameters);
}

String IAggregateFunction::getDescription() const
{
    String description;

    description += getName();

    description += '(';

    for (const auto & parameter : parameters)
    {
        description += parameter.dump();
        description += ", ";
    }

    if (!parameters.empty())
    {
        description.pop_back();
        description.pop_back();
    }

    description += ')';

    description += '(';

    for (const auto & argument_type : argument_types)
    {
        description += argument_type->getName();
        description += ", ";
    }

    if (!argument_types.empty())
    {
        description.pop_back();
        description.pop_back();
    }

    description += ')';

    return description;
}

bool IAggregateFunction::haveEqualArgumentTypes(const IAggregateFunction & rhs) const
{
    return std::equal(
        argument_types.begin(),
        argument_types.end(),
        rhs.argument_types.begin(),
        rhs.argument_types.end(),
        [](const auto & t1, const auto & t2) { return t1->equals(*t2); });
}

DataTypePtr IAggregateFunction::getReturnTypeToPredict() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prediction is not supported for {}", getName());
}

bool IAggregateFunction::haveSameStateRepresentation(const IAggregateFunction & rhs) const
{
    const auto & lhs_base = getBaseAggregateFunctionWithSameStateRepresentation();
    const auto & rhs_base = rhs.getBaseAggregateFunctionWithSameStateRepresentation();
    return lhs_base.haveSameStateRepresentationImpl(rhs_base);
}

bool IAggregateFunction::haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const
{
    return getStateType()->equals(*rhs.getStateType());
}

void IAggregateFunction::parallelizeMergePrepare(
    AggregateDataPtrs & /*places*/, ThreadPool & /*thread_pool*/, std::atomic<bool> & /*is_cancelled*/) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "parallelizeMergePrepare() with thread pool parameter isn't implemented for {} ", getName());
}

void IAggregateFunction::merge(
    AggregateDataPtr __restrict /*place*/,
    ConstAggregateDataPtr /*rhs*/,
    ThreadPool & /*thread_pool*/,
    std::atomic<bool> & /*is_cancelled*/,
    Arena * /*arena*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "merge() with thread pool parameter isn't implemented for {} ", getName());
}

void IAggregateFunction::insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
{
    if (isState())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Function {} is marked as State but method insertMergeResultInto is not implemented", getName());

    insertResultInto(place, to, arena);
}

void IAggregateFunction::predictValues(
    ConstAggregateDataPtr __restrict /* place */,
    IColumn & /*to*/,
    const ColumnsWithTypeAndName & /*arguments*/,
    size_t /*offset*/,
    size_t /*limit*/,
    ContextPtr /*context*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method predictValues is not supported for {}", getName());
}

void IAggregateFunction::compileCreate(llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_ptr*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
}

void IAggregateFunction::compileAdd(
    llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_ptr*/, const ValuesWithType & /*arguments*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
}

void IAggregateFunction::compileMerge(
    llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_dst_ptr*/, llvm::Value * /*aggregate_data_src_ptr*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
}

llvm::Value * IAggregateFunction::compileGetResult(llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_ptr*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
}

}
