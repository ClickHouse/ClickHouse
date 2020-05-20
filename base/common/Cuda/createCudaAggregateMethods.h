#pragma once

#include <Interpreters/Cuda/CudaAggregateMethods.h>


namespace DB
{

void createCudaAggregateMethods(const Context &context)
{
    CudaAggregateMethods::instance().createMethods(context);
}

}