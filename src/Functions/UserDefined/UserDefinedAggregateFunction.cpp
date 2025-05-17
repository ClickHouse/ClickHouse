#include "UserDefinedAggregateFunction.h"

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/Readelpers.h>
#include <IO/WriteHelpers.h>

void UserDefinedAggreagteFunction::add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override {
    auto & state = this->data(place);
    ColumnsWithTypeAndName arguments;
    arguments.emplace_back(state_type->createColumnConst(1, this->data(place)), state_type, "state");
    for (size_t i = 0; i < configuration.argument_types.size(); ++i) {
        arguments.emplace_back(columns[i]->getPtr(), argument_types[i], "arg_" + std::to_string(i));
    }
    auto result_column = configuration.process_func->executeImpl(arguments, state_type, 1);
    this->data(place) = (*result_column)[0];
}

void  UserDefinedAggreagteFunction::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override {
    ColumnsWithTypeAndName arguments;
    arguments.emplace_back(state_type->createColumnConst(1, this->data(place)), state_type, "state1");
    arguments.emplace_back(state_type->createColumnConst(1, this->data(rhs)), state_type, "state2");
    
    auto result_column = func->executeImpl(arguments, state_type, 1);
    this->data(place) = (*result_column)[0];
}

void UserDefinedAggreagteFunction::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override {
    ColumnsWithTypeAndName arguments;
    arguments.emplace_back(state_type->createColumnConst(1, this->data(place)), state_type, "state");
    to = *func->executeImpl(arguments, this->getResultType(), 1);
}

void UserDefinedAggreagteFunction::serialize(
    ConstAggregateDataPtr __restrict place,
    WriteBuffer & buf,
    std::optional<size_t> version = std::nullopt
) const override {
    auto str = this->data(place).dump();
    writeStringBinary(str);
}

void UserDefinedAggreagteFunction::deserialize(
    ConstAggregateDataPtr __restrict place,
    WriteBuffer & buf,
    std::optional<size_t> version = std::nullopt
) const override {
    std::string str;
    readStringBinary(str, buf);
    this->data(place) = Field::restoreFromDump(str);
}
