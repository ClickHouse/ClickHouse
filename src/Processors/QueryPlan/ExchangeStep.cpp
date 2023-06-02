//
// Created by zhangxiao871 on 2023/5/18.
//

#include <Processors/QueryPlan/ExchangeStep.h>

namespace DB
{

ExchangeStep::ExchangeStep(const DataStream & data_stream) : ISourceStep(data_stream) {}

}
