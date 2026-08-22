#include <Common/Exception.h>
#include <Storages/MergeTree/IExecutableTask.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


Priority ExecutableLambdaAdapter::getPriority() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "getPriority() method is not supported by LambdaAdapter");
}

}
