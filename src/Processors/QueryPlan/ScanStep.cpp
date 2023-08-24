//
// Created by zhangxiao871 on 2023/5/18.
//

#include <Processors/QueryPlan/ScanStep.h>

namespace DB
{

ScanStep::ScanStep(const DataStream & data_stream, StoragePtr main_table_) : ISourceStep(data_stream), main_table(main_table_) {}

}
